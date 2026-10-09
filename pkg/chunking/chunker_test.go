package chunking

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkerSplitAndMerge(t *testing.T) {
	data := []byte("Hello, this is a test of the chunking package. We will split this data into small chunks and then merge them back to verify correctness.")
	r := bytes.NewReader(data)

	// Split with a small chunk size of 10 bytes for testing
	chunks, err := Split(r, 10)
	assert.NoError(t, err)
	assert.NotEmpty(t, chunks)

	// Ensure each chunk is at most 10 bytes
	for _, chunk := range chunks {
		assert.LessOrEqual(t, len(chunk), 10)
	}

	// Merge back
	var buf bytes.Buffer
	err = Merge(&buf, chunks)
	assert.NoError(t, err)
	assert.Equal(t, data, buf.Bytes())
}

func TestReaderExactMultipleAndEmpty(t *testing.T) {
	// Exact multiple of chunk size must not yield a trailing empty chunk.
	cr := NewReader(bytes.NewReader(make([]byte, 20)), 10)
	for i := 0; i < 2; i++ {
		chunk, err := cr.Next()
		assert.NoError(t, err)
		assert.Len(t, chunk, 10)
	}
	_, err := cr.Next()
	assert.Equal(t, io.EOF, err)

	// Empty input yields no chunks.
	chunks, err := Split(bytes.NewReader(nil), 10)
	assert.NoError(t, err)
	assert.Empty(t, chunks)
}
