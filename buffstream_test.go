package buffstream

import (
	"bytes"
	"io"
	"math/rand"
	"net"
	"sync"
	"testing"
)

type msg struct {
	msgType int
	data    []byte
}

func testReaderWriter(t *testing.T, r io.ReadCloser, w io.WriteCloser) {
	msgs := make([]msg, 10)
	for i := 0; i < len(msgs); i++ {
		msgs[i].msgType = rand.Intn(50)
		msgs[i].data = make([]byte, rand.Intn(50))
		if _, err := rand.Read(msgs[i].data); err != nil {
			t.Fatalf("Error while reading random bytes: %v", err)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buffWriter := NewWriter(w)

		for i, msg := range msgs {
			t.Logf("Writing message %v: %+v", i, msg)
			if _, err := buffWriter.Write(msg.msgType, msg.data); err != nil {
				t.Fatalf("Error while writing: %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		buffReader := NewReader(r)
		buf := make([]byte, 50)

		for i, msg := range msgs {
			msgType, msgLen, err := buffReader.Read(buf)
			if err != nil {
				t.Fatalf("Error while reading: %v", err)
			}
			t.Logf("Read message %v: %v, %+v", i, msgType, buf[:msgLen])

			if msgType != msg.msgType {
				t.Fatalf("Error on message %v, Expected type %v, got %v", i, msg.msgType, msgType)
			}

			if msgLen != len(msg.data) {
				t.Fatalf("Error on message %v, expected length %v, got %v", i, len(msg.data), msgLen)
			}

			if !bytes.Equal(buf[:msgLen], msgs[i].data) {
				t.Fatalf("Error on message %v, message sent and recieved not identical", i)
			}
		}
	}()

	wg.Wait()
}

func TestPipe(t *testing.T) {
	return
	r, w := io.Pipe()
	testReaderWriter(t, r, w)
}

func TestTCPConn(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Error while resolving tcp address: %v", err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("Error while listening to tcp: %v", err)
	}

	connDial, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatalf("Error while dialing tcp: %v", err)
	}

	connListen, err := listener.Accept()
	if err != nil {
		t.Fatalf("Error while accepting tcp: %v", err)
	}

	testReaderWriter(t, connListen, connDial)
	testReaderWriter(t, connDial, connListen)
}

func TestRereadTooShort(t *testing.T) {
	r, w := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(2)

	msgType := rand.Intn(50)
	data := make([]byte, 50)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("Error while reading random bytes: %v", err)
	}

	go func() {
		defer wg.Done()
		buffWriter := NewWriter(w)
		if _, err := buffWriter.Write(msgType, data); err != nil {
			t.Fatalf("Error while writing: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		buffReader := NewReader(r)
		buf := make([]byte, 10)

		if _, _, err := buffReader.Read(buf); err != ErrBufferTooSmall {
			t.Fatalf("Expected ErrBufferTooSmall, got %v", err)
		}

		buf = make([]byte, 50)
		gotMsgType, gotMsgLen, err := buffReader.Read(buf)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if gotMsgType != msgType {
			t.Fatalf("Expected type %v, got %v", msgType, gotMsgType)
		}

		if len(data) != gotMsgLen || !bytes.Equal(buf[:gotMsgLen], data) {
			t.Fatalf("message sent and recieved not identical")
		}
	}()

	wg.Wait()
}
