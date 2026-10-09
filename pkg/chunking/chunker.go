package chunking

import (
	"io"
)

const DefaultChunkSize = 4 * 1024 * 1024 // 4MB

// Reader yields fixed-size chunks from an underlying stream one at a time,
// so callers never need to hold more than one chunk in memory.
type Reader struct {
	r   io.Reader
	buf []byte
	eof bool
}

// NewReader creates a chunk Reader over r with the given chunk size.
func NewReader(r io.Reader, chunkSize int64) *Reader {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Reader{r: r, buf: make([]byte, chunkSize)}
}

// Next returns the next chunk, or io.EOF when the stream is exhausted.
// The returned slice is a fresh copy owned by the caller.
func (c *Reader) Next() ([]byte, error) {
	if c.eof {
		return nil, io.EOF
	}
	n, err := io.ReadFull(c.r, c.buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		c.eof = true
	} else if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, io.EOF
	}
	chunk := make([]byte, n)
	copy(chunk, c.buf[:n])
	return chunk, nil
}

// Split reads from reader and returns chunks of the specified size.
func Split(r io.Reader, chunkSize int64) ([][]byte, error) {
	cr := NewReader(r, chunkSize)
	var chunks [][]byte
	for {
		chunk, err := cr.Next()
		if err == io.EOF {
			return chunks, nil
		}
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
}

// Merge writes the provided slices of chunks into the destination writer.
func Merge(w io.Writer, chunks [][]byte) error {
	for _, chunk := range chunks {
		if _, err := w.Write(chunk); err != nil {
			return err
		}
	}
	return nil
}
