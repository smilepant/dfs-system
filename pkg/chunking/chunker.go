package chunking

import (
	"io"
)

const DefaultChunkSize = 4 * 1024 * 1024 // 4MB

// Split reads from reader and returns chunks of the specified size.
func Split(r io.Reader, chunkSize int64) ([][]byte, error) {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	var chunks [][]byte
	buf := make([]byte, chunkSize)
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			chunks = append(chunks, chunk)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return chunks, nil
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
