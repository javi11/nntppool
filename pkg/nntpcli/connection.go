//go:generate go tool mockgen -source=./connection.go -destination=./connection_mock.go -package=nntpcli Connection
package nntpcli

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/mnightingale/rapidyenc"
)

type Connection interface {
	io.Closer
	Authenticate(username, password string) (err error)
	JoinGroup(name string) error
	BodyDecoded(msgID string, w io.Writer, discard int64) (int64, error)
	BodyReader(msgID string) (ArticleBodyReader, error)
	Post(r io.Reader) (int64, error)
	Ping() error
	CurrentJoinedGroup() string
	MaxAgeTime() time.Time
	Stat(msgID string) (int, error)
	Capabilities() ([]string, error)
}

var _ Connection = (*connection)(nil)

type connection struct {
	maxAgeTime         time.Time
	netconn            net.Conn
	conn               *textproto.Conn
	currentJoinedGroup string
}

func newConnection(netconn net.Conn, maxAgeTime time.Time) (Connection, error) {
	conn := textproto.NewConn(netconn)

	_, _, err := conn.ReadCodeLine(StatusReady)
	if err != nil {
		// Download only server
		_, _, err = conn.ReadCodeLine(StatusReadyNoPosting)
		if err == nil {
			return &connection{
				conn:       conn,
				netconn:    netconn,
				maxAgeTime: maxAgeTime,
			}, nil
		}

		_ = conn.Close()

		return nil, err
	}

	return &connection{
		conn:       conn,
		netconn:    netconn,
		maxAgeTime: maxAgeTime,
	}, nil
}

// Close this client.
func (c *connection) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Close: %v", r)
		}
	}()

	_, _, err = c.sendCmd(StatusQuit, "QUIT")
	e := c.conn.Close()

	if err != nil {
		return err
	}

	return e
}

// Authenticate against an NNTP server using authinfo user/pass
func (c *connection) Authenticate(username, password string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Authenticate: %v", r)
		}
	}()

	code, _, err := c.sendCmd(StatusMoreAuthInfoRequired, "AUTHINFO USER %s", username)
	if err != nil {
		return fmt.Errorf("AUTHINFO USER %s: %w", username, err)
	}

	switch code {
	case 481, 482, StatusPermissionDenied:
		// failed, out of sequence or command not available
		return fmt.Errorf("AUTHINFO USER %s: authentication failed with code %d", username, code)
	case StatusAuthenticated:
		// accepted without password
		return nil
	case StatusMoreAuthInfoRequired:
		// need password
		break
	default:
		return fmt.Errorf("AUTHINFO USER %s: unexpected response code %d", username, code)
	}

	_, _, err = c.sendCmd(StatusAuthenticated, "AUTHINFO PASS %s", password)
	if err != nil {
		return fmt.Errorf("AUTHINFO PASS (user %s): %w", username, err)
	}

	return nil
}

func (c *connection) Ping() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Ping: %v", r)
		}
	}()

	_, _, err = c.sendCmd(StatusReady, "DATE")
	if err != nil {
		return fmt.Errorf("DATE: %w", err)
	}

	return nil
}

func (c *connection) JoinGroup(group string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in JoinGroup: %v", r)
		}
	}()

	if group == c.currentJoinedGroup {
		return nil
	}

	_, _, err = c.sendCmd(StatusGroupSelected, "GROUP %s", group)
	if err != nil {
		return fmt.Errorf("GROUP %s: %w", group, err)
	}

	c.currentJoinedGroup = group

	return nil
}

func (c *connection) CurrentJoinedGroup() (group string) {
	defer func() {
		if r := recover(); r != nil {
			// Cannot return error from this method, return empty string
			group = ""
		}
	}()

	return c.currentJoinedGroup
}

// BodyDecoded gets the decoded body of an article
// If discard is provided the body will be discarded until the discard line,
// this is useful if you don't want to start the writer from the beginning
// Body retrieves the body of a message with the given message ID from the NNTP server,
// writes it to the provided io.Writer, and optionally discards the first 'discard' lines.
//
// Parameters:
//   - msgID: The message ID of the article to retrieve.
//   - w: The io.Writer to which the message body will be written.
//   - discard: The number of lines to discard from the beginning of the message body.
//
// Returns:
//   - int64: The number of bytes written to the io.Writer.
//   - error: Any error encountered during the operation.
//
// The function sends the "BODY" command to the NNTP server, starts the response,
// and reads the response code. It uses a decoder to read the message body and
// optionally discards the specified number of lines before writing the remaining
// body to the provided io.Writer. If an error occurs during reading or writing,
// the function ensures that the decoder is fully read to avoid connection issues.
func (c *connection) BodyDecoded(msgID string, w io.Writer, discard int64) (n int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in BodyDecoded: %v", r)
			n = 0
		}
	}()

	id, err := c.conn.Cmd("BODY <%s>", msgID)
	if err != nil {
		return 0, fmt.Errorf("BODY <%s>: %w", msgID, formatError(err))
	}

	c.conn.StartResponse(id)
	defer c.conn.EndResponse(id)

	_, _, err = c.conn.ReadCodeLine(StatusBodyFollows)
	if err != nil {
		return 0, fmt.Errorf("BODY <%s>: %w", msgID, err)
	}

	dec := rapidyenc.AcquireDecoder(c.conn.R)
	defer rapidyenc.ReleaseDecoder(dec)

	// Discard the first n lines
	if discard > 0 {
		if _, err = io.CopyN(io.Discard, dec, discard); err != nil {
			// Attempt to drain the decoder to avoid connection issues
			if _, drainErr := io.Copy(io.Discard, dec); drainErr != nil {
				return 0, fmt.Errorf("BODY <%s>: discard %d bytes failed: %w (drain also failed: %v)", msgID, discard, err, drainErr)
			}

			return 0, fmt.Errorf("BODY <%s>: discard %d bytes failed: %w", msgID, discard, err)
		}
	}

	n, err = io.Copy(w, dec)
	if err != nil {
		// Attempt to drain the decoder to avoid connection issues
		if _, drainErr := io.Copy(io.Discard, dec); drainErr != nil {
			return n, fmt.Errorf("BODY <%s>: copy failed: %w (drain also failed: %v)", msgID, err, drainErr)
		}

		return n, fmt.Errorf("BODY <%s>: copy failed: %w", msgID, err)
	}

	return n, nil
}

func (c *connection) BodyReader(msgID string) (reader ArticleBodyReader, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in BodyReader: %v", r)
			reader = nil
		}
	}()

	id, err := c.conn.Cmd("BODY <%s>", msgID)
	if err != nil {
		return nil, fmt.Errorf("BODY <%s>: %w", msgID, formatError(err))
	}

	c.conn.StartResponse(id)

	_, _, err = c.conn.ReadCodeLine(StatusBodyFollows)
	if err != nil {
		c.conn.EndResponse(id)
		return nil, fmt.Errorf("BODY <%s>: %w", msgID, err)
	}

	dec := rapidyenc.AcquireDecoder(c.conn.R)

	return &articleBodyReader{
		decoder:    dec,
		conn:       c,
		responseID: id,
		closed:     false,
	}, nil
}

// Post a new article
//
// The reader should contain the entire article, headers and body in
// RFC822ish format.
//
// Returns the number of bytes written and any error encountered.
func (c *connection) Post(r io.Reader) (n int64, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("panic in Post: %v", rec)
			n = 0
		}
	}()

	_, _, err = c.sendCmd(StatusPasswordRequired, "POST")
	if err != nil {
		return 0, fmt.Errorf("POST: %w", err)
	}

	w := c.conn.DotWriter()

	n, err = io.Copy(w, r)
	if err != nil {
		return 0, fmt.Errorf("POST: copy article content failed: %w", err)
	}

	if err = w.Close(); err != nil {
		return 0, fmt.Errorf("POST: close writer failed: %w", err)
	}

	_, _, err = c.conn.ReadCodeLine(StatusArticlePosted)
	if err == nil {
		return n, nil
	}

	return 0, fmt.Errorf("POST: %w", err)
}

// Stat sends a STAT command to the NNTP server to check the status of a message
// with the given message ID. It returns the message number if the message exists.
//
// Parameters:
//
//	msgID - The message ID to check.
//
// Returns:
//
//	int - The message number if the message exists.
//	error - An error if the command fails or the response is invalid.
func (c *connection) Stat(msgID string) (number int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Stat: %v", r)
			number = 0
		}
	}()

	id, err := c.conn.Cmd("STAT <%s>", msgID)
	if err != nil {
		return 0, fmt.Errorf("STAT <%s>: %w", msgID, err)
	}

	c.conn.StartResponse(id)
	defer c.conn.EndResponse(id)

	_, line, err := c.conn.ReadCodeLine(StatusStatSuccess)
	if err != nil {
		return 0, fmt.Errorf("STAT <%s>: %w", msgID, err)
	}

	ss := strings.SplitN(line, " ", NumberOfStatResParams) // optional comment ignored
	if len(ss) < NumberOfStatResParams-1 {
		return 0, fmt.Errorf("STAT <%s>: bad response format: %s", msgID, line)
	}

	number, err = strconv.Atoi(ss[0])
	if err != nil {
		return 0, fmt.Errorf("STAT <%s>: invalid article number in response: %w", msgID, err)
	}

	return number, nil
}

func (c *connection) MaxAgeTime() (maxAge time.Time) {
	defer func() {
		if r := recover(); r != nil {
			// Cannot return error from this method, return zero time
			maxAge = time.Time{}
		}
	}()

	return c.maxAgeTime
}

// Capabilities returns a list of features this server performs.
// Not all servers support capabilities.
func (c *connection) Capabilities() (caps []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Capabilities: %v", r)
			caps = nil
		}
	}()

	const StatusCapabilities = 101 // Capability list follows
	_, _, err = c.sendCmd(StatusCapabilities, "CAPABILITIES")
	if err != nil {
		return nil, err
	}

	return c.readStrings()
}

// readStrings reads a list of strings from the NNTP connection,
// stopping at a line containing only a . (Convenience method for
// LIST, etc.)
func (c *connection) readStrings() ([]string, error) {
	var sv []string

	for {
		line, err := c.conn.ReadLine()
		if err != nil {
			return nil, err
		}

		// Trim trailing newlines more efficiently
		line = strings.TrimSuffix(line, "\r\n")
		line = strings.TrimSuffix(line, "\n")

		if line == "." {
			break
		}

		sv = append(sv, line)
	}

	return sv, nil
}

func (c *connection) sendCmd(expectCode int, cmd string, args ...any) (int, string, error) {
	id, err := c.conn.Cmd(cmd, args...)
	if err != nil {
		return 0, "", err
	}

	c.conn.StartResponse(id)

	defer c.conn.EndResponse(id)

	code, line, err := c.conn.ReadCodeLine(expectCode)
	return code, line, err
}

func formatError(err error) error {
	if IsArticleNotFoundError(err) {
		return errors.Join(err, ErrArticleNotFound)
	}

	return err
}
