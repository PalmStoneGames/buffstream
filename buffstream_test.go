package buffstream

import (
	"testing"
	"io"
	"sync"
	"math/rand"
	"bytes"
	"net"
)

func testReaderWriter(t *testing.T, r io.ReadCloser, w io.WriteCloser) {
	datas := make([][]byte, 10)
	for i := 0; i < len(datas); i++ {
		datas[i] = make([]byte, rand.Intn(50))
		if _, err := rand.Read(datas[i]); err != nil {
			t.Fatalf("Error while reading random bytes: %v", err)
		}
	}


	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buffWriter := NewWriter(w)

		for i, data := range datas {
			t.Logf("Writing message %v: %+v", i, data)
			if _, err := buffWriter.Write(data); err != nil {
				t.Fatalf("Error while writing: %v", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		buffReader := NewReader(r)
		buf := make([]byte, 50)

		for i, data := range datas {
			msgLen, err := buffReader.Read(buf)
			if err != nil {
				t.Fatalf("Error while reading: %v", err)
			}
			t.Logf("Read message %v: %+v", i, buf[:msgLen])

			if msgLen != len(data) {
				t.Fatalf("Error on message %v, expected length %v, got %v", i, len(data), msgLen)
			}

			if !bytes.Equal(buf[:msgLen], datas[i]) {
				t.Fatalf("Error on message %v, message sent and recieved not identical", i)
			}
		}
	}()

	wg.Wait()
}

func TestPipe(t *testing.T) {
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

	data := make([]byte, 50)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("Error while reading random bytes: %v", err)
	}

	go func() {
		defer wg.Done()
		buffWriter := NewWriter(w)
		if _, err := buffWriter.Write(data); err != nil {
			t.Fatalf("Error while writing: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		buffReader := NewReader(r)
		buf := make([]byte, 10)

		if _, err := buffReader.Read(buf); err != ErrBufferTooSmall {
			t.Fatalf("Expected ErrBufferTooSmall, got %v", err)
		}

		buf = make([]byte, 50)
		msgLen, err := buffReader.Read(buf)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(data) != msgLen || !bytes.Equal(buf[:msgLen], data) {
			t.Fatalf("message sent and recieved not identical")
		}
	}()

	wg.Wait()
}