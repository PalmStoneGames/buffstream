// The buffstream package wraps io.Reader and io.Writer and prefixes each written []byte with its length, each Read on the other end will then return the exact same byte slice
// The byte slices sent are also typed, a integer data-type can be sent along as a way for the other end to know what to do with the data
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
	reader byteReader

	isSaved      bool
	savedMsgLen  int64
	savedMsgType int64
}

// Writer wraps an io.Writer, each call to Write will write exactly one message, and the Read on the other hand will return the exact same message with the same length
type Writer struct {
	writer  io.Writer
	msgLen  [10]byte
	msgType [10]byte
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
		reader:      br,
		savedMsgLen: -1,
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
func (w *Writer) Write(msgType int, data []byte) (int, error) {
	msgLenBytes := binary.PutVarint(w.msgLen[:], int64(len(data)))
	msgTypeBytes := binary.PutVarint(w.msgType[:], int64(msgType))

	_, err := w.writer.Write(w.msgLen[:msgLenBytes])
	if err != nil {
		return 0, err
	}

	_, err = w.writer.Write(w.msgType[:msgTypeBytes])
	if err != nil {
		return 0, err
	}

	return w.writer.Write(data)
}

// Read will read the next pending message and return its type and length, or an error if any occured
func (r *Reader) Read(data []byte) (int, int, error) {
	var msgLen int64
	var msgType int64
	if r.isSaved {
		msgLen = r.savedMsgLen
		msgType = r.savedMsgType
		r.isSaved = false
	} else {
		var err error
		msgLen, err = binary.ReadVarint(r.reader)
		if err != nil {
			return 0, 0, err
		}

		msgType, err = binary.ReadVarint(r.reader)
		if err != nil {
			return 0, 0, err
		}
	}

	if msgLen > int64(len(data)) {
		r.isSaved = true
		r.savedMsgLen = msgLen
		r.savedMsgType = msgType
		return 0, 0, ErrBufferTooSmall
	}

	// Using the header, read the remaining body
	readLen, err := io.ReadFull(r.reader, data[:msgLen])
	return int(msgType), readLen, err
}
