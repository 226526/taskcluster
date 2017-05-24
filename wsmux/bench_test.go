package wsmux

import (
	"bytes"
	"crypto/sha256"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
)

// utils
var upgrader websocket.Upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
}

func genTransferHandler(b *testing.B) http.Handler {
	tr := func(w http.ResponseWriter, r *http.Request) {
		if !websocket.IsWebSocketUpgrade(r) {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			b.Fatal(err)
		}

		server := Server(conn, Config{Log: genLogger("transfer-bench-test"), StreamBufferSize: 64 * 1024})
		hash := sha256.New()

		str, err := server.Accept()
		if err != nil {
			b.Fatal(err)
		}
		// copy does not report EOF as error
		_, err = io.Copy(hash, str)
		if err != nil {
			b.Fatal(err)
		}
		_, err = str.Write(hash.Sum(nil))
		if err != nil {
			b.Fatal(err)
		}
		err = str.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
	return http.HandlerFunc(tr)
}

// test large transfer
func BenchmarkTransfer(b *testing.B) {
	server := &http.Server{
		Addr:    ":9999",
		Handler: genTransferHandler(b),
	}
	defer func() {
		_ = server.Close()
	}()
	go func() {
		_ = server.ListenAndServe()
	}()
	conn, _, err := (&websocket.Dialer{}).Dial("ws://127.0.0.1:9999", nil)
	if err != nil {
		b.Fatal(err)
	}
	client := Client(conn, Config{Log: genLogger("transfer-client-bench-test"), StreamBufferSize: 64 * 1024})
	str, err := client.Open()
	if err != nil {
		b.Fatal(err)
	}

	hash := sha256.New()
	// write 4MB of data
	// how: generate a fixed array of size 2048
	// write it to hash and stream 2048 times

	// generate array
	buf := make([]byte, 2048)
	for i := 0; i < 2048; i++ {
		buf[i] = byte(i % 127)
	}

	b.ResetTimer()
	// write this array 4096 times
	for i := 0; i < 4096; i++ {
		_, _ = hash.Write(buf)
		_, err := str.Write(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
	err = str.Close()

	// read hash generated by remote and compare
	buf, err = ioutil.ReadAll(str)
	if err != nil {
		b.Fatal(err)
	}

	if !bytes.Equal(hash.Sum(nil), buf) {
		b.Fatalf("bad message")
	}
}
