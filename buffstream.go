// The buffstream package wraps io.Reader and io.Writer and prefixes each written []byte with its length, each Read on the other end will then return the exact same byte slice
package buffstream

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

var (
	// ErrBufferTooSmall is returned when the buffer passed in for reading is too small to contain the whole message
	ErrBufferTooSmall = errors.New("Read buffer too small, please try again with a bigger buffer")
)

type byteReader interface {
	io.Reader
	io.ByteReader
}

// Reader wraps an io.Reader, each call to Read will return exactly one message, the length of which will exactly match the length of the buffer written on the other end
type Reader struct {
	reader         byteReader
	savedMsgLength int64
}

// Writer wraps an io.Writer, each call to Write will write exactly one message, and the Read on the other hand will return the exact same message with the same length
type Writer struct {
	writer io.Writer
	header [10]byte
}

// NewReader creates a new reader
func NewReader(r io.Reader) *Reader {
	var br byteReader
	if rr, ok := r.(byteReader); ok {
		br = rr
	} else {
		br = bufio.NewReader(r)
	}

	return &Reader{
		reader:         br,
		savedMsgLength: -1,
	}
}

// NewWriter creates a new writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: w,
	}
}

// Write allows you to send a stream of bytes as messages. Each slice of bytes
// you pass in will be prefixed by its size. If not all bytes can be written, Write will keep
// trying until the full message is delivered, or the connection is broken.
// By default Write is unbuffered, if buffered IO is desirable, the passed in reader can be wrapped with a bufio.Writer
func (w *Writer) Write(data []byte) (int, error) {
	headerBytes := binary.PutVarint(w.header[:], int64(len(data)))
	writtenHeader, err := w.writer.Write(w.header[:headerBytes])
	if err != nil {
		return writtenHeader, err
	}

	writtenBody, err := w.writer.Write(data)
	return writtenHeader + writtenBody, err
}

func (r *Reader) Read(data []byte) (int, error) {
	var msgLength int64
	if r.savedMsgLength != -1 {
		msgLength = r.savedMsgLength
		r.savedMsgLength = -1
	} else {
		var err error
		msgLength, err = binary.ReadVarint(r.reader)
		if err != nil {
			return 0, err
		}
	}

	if msgLength > int64(len(data)) {
		r.savedMsgLength = msgLength
		return 0, ErrBufferTooSmall
	}

	// Using the header, read the remaining body
	return io.ReadFull(r.reader, data[:msgLength])
}
