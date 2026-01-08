package nntpcli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"testing"

	"github.com/mnightingale/rapidyenc"
	"github.com/stretchr/testify/require"
)

func BenchmarkDecoder(b *testing.B) {
	raw := make([]byte, 1024*1024)
	_, err := rand.Read(raw)
	require.NoError(b, err)

	r, err := nntpbody(raw)
	require.NoError(b, err)

	b.ResetTimer()
	for b.Loop() {
		resp := &NNTPResponse{}
		buf := make([]byte, 32*1024)

		for {
			n, rerr := r.Read(buf)
			if n > 0 {
				chunk := buf[:n]
				for len(chunk) > 0 {
					consumed, done, err := resp.Feed(chunk, io.Discard)
					require.NoError(b, err)
					chunk = chunk[consumed:]
					if done {
						break
					}
					require.NotZero(b, consumed)
				}
			}
			if rerr == io.EOF {
				break
			}
			require.NoError(b, rerr)
		}

		require.True(b, resp.eof || resp.StatusCode != 0)
		_, err = r.Seek(0, io.SeekStart)
		require.NoError(b, err)
	}
}

func BenchmarkDecoderConnection(b *testing.B) {
	raw := make([]byte, 1024*1024)
	_, err := rand.Read(raw)
	require.NoError(b, err)

	r, err := nntpbody(raw)
	require.NoError(b, err)

	wire, err := io.ReadAll(r)
	require.NoError(b, err)
	// NNTP multiline responses must be terminated by a dot line.
	if !bytes.HasSuffix(wire, []byte("\r\n")) {
		wire = append(wire, '\r', '\n')
	}
	wire = append(wire, '.', '\r', '\n')

	factory := func(_ context.Context) (net.Conn, error) {
		serverConn, clientConn := net.Pipe()
		go func() {
			defer serverConn.Close()

			// Greeting is sent immediately upon connect.
			_, _ = serverConn.Write([]byte("200 ready\r\n"))

			br := bufio.NewReader(serverConn)
			for {
				// Read a single command line (e.g. BODY/ARTICLE).
				if _, err := br.ReadString('\n'); err != nil {
					return
				}
				if _, err := serverConn.Write(wire); err != nil {
					return
				}
			}
		}()
		return clientConn, nil
	}

	client, err := NewClientWithConnFactory(context.Background(), 1, 1, Auth{}, factory)
	require.NoError(b, err)
	defer client.Close()

	payload := []byte("BODY <foo>\r\n")

	b.ResetTimer()
	for b.Loop() {
		respCh := client.Send(context.Background(), payload, io.Discard)
		resp, ok := <-respCh
		require.True(b, ok)
		require.NoError(b, resp.Err)
		require.Equal(b, 222, resp.StatusCode)
	}
}

func nntpbody(raw []byte) (io.ReadSeeker, error) {
	w := new(bytes.Buffer)
	w.WriteString("222 0 <foo@bar\r\n")

	enc, err := rapidyenc.NewEncoder(w, rapidyenc.Meta{
		FileName:   "filename",
		FileSize:   int64(len(raw)),
		PartSize:   int64(len(raw)),
		PartNumber: 1,
		TotalParts: 1,
	})
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(enc, bytes.NewReader(raw)); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}

	return bytes.NewReader(w.Bytes()), nil
}
