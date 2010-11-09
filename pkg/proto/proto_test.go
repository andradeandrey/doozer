package proto

import (
	"bufio"
	"bytes"
	"fmt"
	"junta/assert"
	"os"
	"testing"
)

type WriteFlusher interface {
	Flush() os.Error
	Write([]byte) (int, os.Error)
}

func encode(w WriteFlusher, a ... interface{}) (err os.Error) {
	_, err = fmt.Fprintf(w, "*%d\r\n", len(a))
	if err != nil {
		return
	}

	for _, v := range a {
		s := v.(string)
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
	}

	return w.Flush() 
}


func TestProtoEncode(t *testing.T) {
	buf := new(bytes.Buffer)
	w   := bufio.NewWriter(buf)
	assert.Equal(t, nil, encode(w, "set", "foo", "bar"))
	assert.Equal(t, "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", buf.String())
}
