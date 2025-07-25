package nntppool

import "github.com/javi11/nntpcli"

// pooledBodyReader wraps an io.ReadCloser and manages the associated pooled connection
type pooledBodyReader struct {
	reader nntpcli.ArticleBodyReader
	conn   PooledConnection
}

func (r *pooledBodyReader) GetYencHeaders() (nntpcli.YencHeaders, error) {
	return r.reader.GetYencHeaders()
}

func (r *pooledBodyReader) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *pooledBodyReader) Close() error {
	// Close the reader first
	if r.reader != nil {
		_ = r.reader.Close()
	}

	// Then free the connection back to the pool
	if r.conn != nil {
		r.conn.Free()
	}

	return nil
}
