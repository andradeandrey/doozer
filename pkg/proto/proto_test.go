package proto

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"junta/assert"
	"os"
	"testing"
)

func encode(w io.Writer, a ... interface{}) (err os.Error) {
	_, err = fmt.Fprintf(w, "*%d\r\n", len(a))
	if err != nil {
		return
	}

	for _, v := range a {
		switch v.(type) {
		case string:
			s := v.(string)
			_, err = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
			if err != nil {
				return
			}
		case int:
			i := v.(int)
			_, err = fmt.Fprintf(w, ":%d\r\n", i)
			if err != nil {
				return
			}
		}
	}

	return
}

// An io.Writer that will return os.EOF on the `n`th byte written
type eWriter struct {
	n int
}

func (e *eWriter) Write(p []byte) (n int, err os.Error) {
	l := len(p)
	e.n -= l
	if e.n <= 0 {
		return 0, os.EOF
	}
	return l, nil
}

func TestProtoEncode(t *testing.T) {
	buf := new(bytes.Buffer)
	w   := bufio.NewWriter(buf)
	assert.Equal(t, nil, encode(w, "set", "foo", 1))
	w.Flush()
	assert.Equal(t, "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n:1\r\n", buf.String())
}

func TestProtoEncodeErrors(t *testing.T) {
	assert.Equal(t, os.EOF, encode(&eWriter{4}))
	assert.Equal(t, os.EOF, encode(&eWriter{5}, "ping"))
	assert.Equal(t, os.EOF, encode(&eWriter{5}, 1))
}
