package proto

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"junta/assert"
	"junta/test"
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

func TestProtoEncode(t *testing.T) {
	buf := new(bytes.Buffer)
	w   := bufio.NewWriter(buf)

	// One part
	assert.Equal(t, nil, encode(w, "set", "foo", 1))

	// We need to flush the output before reading
	w.Flush()

	assert.Equal(t, "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n:1\r\n", buf.String())
}

func TestProtoEncodeReturnsErrors(t *testing.T) {
	assert.Equal(t, os.EOF, encode(&test.ErrWriter{4}))
	assert.Equal(t, os.EOF, encode(&test.ErrWriter{5}, "ping"))
	assert.Equal(t, os.EOF, encode(&test.ErrWriter{5}, 1))
}
