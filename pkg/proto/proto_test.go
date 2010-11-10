package proto

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"junta/assert"
	"junta/test"
	"os"
	"reflect"
	"testing"
)

func encode(w io.Writer, a ... interface{}) (err os.Error) {
	for _, v := range a {
		t := reflect.Typeof(v)
		switch {
		// Keith,  I'm trying ot reflect here to keep from using an epic case
		// statement.  This is where I left off.
		case reflect.Int <= t.Kind() && t.Kind() <= reflect.Int64:
			_, err = fmt.Fprintf(w, ":%d\r\n", int64(i))
			if err != nil {
				return
			}
		}
	}

	return
}

type encTest struct {
	data     interface{}
	expected string
}

var encTests = []encTest{
	{int(0), ":0\r\n"},
	{int8(0), ":0\r\n"},
	// {int16(0), ":0\r\n"},
	// {int32(0), ":0\r\n"},
	// {int64(0), ":0\r\n"},
	// {uint(0), ":0\r\n"},
	// {uint8(0), ":0\r\n"}, // aka byte
	// {uint16(0), ":0\r\n"},
	// {uint32(0), ":0\r\n"},
	// {uint64(0), ":0\r\n"},

	// {[]byte{'f', 'o', 'o'}, "$3\r\nfoo\r\n"},
	// {"foo", "$3\r\nfoo\r\n"},
	// {Line("hi"), "+hi\r\n"},
	// {os.NewError("hi"), "-hi\r\n"},
	// {nil, "$-1\r\n"},
	// {[]interface{}{[]byte{'a'}, []byte{'b'}}, "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
	// {[]string{"GET", "FOO"}, "*2\r\n$3\r\nGET\r\n$3\r\nFOO\r\n"},
	// {[]interface{}{1, []interface{}{1}}, "*2\r\n:1\r\n*1\r\n:1\r\n"},
}

func TestProtoEncode(t *testing.T) {
	for _, e := range encTests {
		b := new(bytes.Buffer)
		w := bufio.NewWriter(b)

		err := encode(w, e.data)
		if err != nil {
			t.Errorf("unexpected err: %s", err)
			continue
		}

		w.Flush()

		if e.expected != string(b.Bytes()) {
			t.Errorf("expected %q", e.expected)
			t.Errorf("     got %q", string(b.Bytes()))
		}
	}
}

var (
	FastErr = &test.ErrWriter{1}
)

func TestProtoEncodeReturnsErrors(t *testing.T) {
	assert.Equal(t, os.EOF, encode(FastErr, "set", "foo"))
	assert.Equal(t, os.EOF, encode(FastErr, "ping"))
	assert.Equal(t, os.EOF, encode(FastErr, 1))
}
