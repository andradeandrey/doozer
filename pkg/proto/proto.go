package proto

import (
	"bufio"
	"doozer/util"
	"os"
	"io"
	"fmt"
	"reflect"
	"strings"
	"strconv"
	"sync"
)

// Operations
const (
	SendReq = "send req"
	SendRes = "send res"
	ReadReq = "read req"
	ReadRes = "read res"
)

// Errors we can have
const (
	InvalidCommand = "invalid command"
)

// Response flags
const (
	Closed = 1 << iota
	Last
)

var crnl = []byte{'\r', '\n'}

var logger = util.NewLogger("proto")

type Line string

type Conn struct {
	c  io.ReadWriteCloser
	r  *bufio.Reader
	id uint
	cb map[uint]chan interface{}

	rl, wl, bl sync.Mutex

	RedirectAddr string
}

type request struct {
	Verb Line
	Id   uint
	Data interface{}
}

type response struct {
	Id   uint
	Flag uint
	Data interface{}
}

func (r *response) IsDone() bool {
	return r.Flag & (Closed|Last) != 0
}

func (r *response) IsValid() bool {
	return r.Flag & Closed == 0
}

type ProtoError struct {
	Id    uint
	Op    string
	Error os.Error
}

func (e *ProtoError) String() string {
	return fmt.Sprintf("%s %d: %s", e.Op, e.Id, e.Error)
}

type ResponseError string

func (e ResponseError) String() string {
	return string(e)
}

func NewConn(rw io.ReadWriteCloser) *Conn {
	return &Conn{
		c:rw,
		r:bufio.NewReader(rw),
		cb:make(map[uint]chan interface{}),
	}
}

// Server functions

func (c *Conn) SendResponse(id, flag uint, data interface{}) os.Error {
	c.wl.Lock()
	defer c.wl.Unlock()

	err := encode(c.c, response{id, flag, data})
	if err != nil {
		// TODO poison
		return &ProtoError{id, SendRes, err}
	}
	return nil
}

func (c *Conn) SendError(id uint, msg string) os.Error {
	return c.SendResponse(id, Last, os.NewError("ERR: "+msg))
}

func (c *Conn) SendRedirect(id uint, addr string) os.Error {
	return c.SendResponse(id, Last, os.NewError("REDIRECT: "+addr))
}

func (c *Conn) ReadRequest() (uint, string, interface{}, os.Error) {
	c.rl.Lock()
	defer c.rl.Unlock()

	data, err := decode(c.r)
	if err != nil {
		// TODO poison
		if err == os.EOF {
			return 0, "", nil, err
		} else {
			return 0, "", nil, &ProtoError{0, ReadReq, err}
		}
	}

	var req request
	err = Fit(data, &req)
	if err != nil {
		return 0, "", nil, &ProtoError{0, ReadReq, err}
	}

	logger.Println("got data", req.Data)
	return req.Id, string(req.Verb), req.Data, nil
}

// Client functions

func (c *Conn) SendRequest(verb string, data interface{}) (Response, os.Error) {
	ch := make(chan interface{})

	c.bl.Lock()
	c.id++
	id := c.id
	c.cb[id] = ch
	c.bl.Unlock()

	c.wl.Lock()
	err := encode(c.c, request{Line(verb), id, data})
	c.wl.Unlock()
	if err != nil {
		// TODO poison
		return nil, &ProtoError{id, SendReq, err}
	}

	return Response(ch), nil
}

func (c *Conn) ReadResponses() {
	c.rl.Lock()
	defer c.rl.Unlock()

	for {
		data, err := decode(c.r)
		if err != nil {
			break
		}

		var res response
		err = Fit(data, &res)
		if re, ok := err.(ResponseError); ok {
			if re[0:9] == "REDIRECT:" {
				c.RedirectAddr = strings.TrimSpace(string(re)[10:])
				logger.Println("redirect to", c.RedirectAddr)
				err = os.EAGAIN
			}
		}
		if err != nil {
			res.Data = err
		}

		if res.Id == 0 {
			continue
		}

		c.bl.Lock()
		ch, ok := c.cb[res.Id]
		if ok && res.IsDone() {
			c.cb[res.Id] = nil, false
		}
		c.bl.Unlock()
		if ok {
			if res.IsValid() {
				ch <- res.Data
			}
			if res.IsDone() {
				close(ch)
			}
		}
	}

	// TODO poison
	// TODO send errors to all waiting requests
}

type Response <-chan interface{}

func (r Response) Get(slot interface{}) os.Error {
	data := <-r
	if e, ok := data.(os.Error); ok {
		return e
	}

	logger.Println("got data", data)
	err := Fit(data, slot)
	if err != nil {
		return &ProtoError{0, ReadRes, err}
	}
	return nil
}

// Helpers

func decode(r *bufio.Reader) (data interface{}, err os.Error) {
	var line string

	for len(line) < 1 {
		line, err = readLine(r)
		if err != nil {
			return nil, err
		}
	}

	switch line[0] {
	case ':':
		i, e := strconv.Atoi64(line[1:])
		if e != nil {
			if ne, ok := e.(*strconv.NumError); ok && ne.Error == os.ERANGE {
				return strconv.Atoui64(line[1:])
			}
			return nil, e
		}
		return i, nil
	case '+':
		return []byte(line[1:]), nil
	case '-':
		return ResponseError(line[1:]), nil
	case '$':
		n, e := strconv.Atoi(line[1:])
		if e != nil {
			return nil, e
		}

		// in redis, "$-1" means nil
		if n < 0 {
			return nil, nil
		}

		b := make([]byte, n+2) // add space for trailing CR+LF
		_, e = io.ReadFull(r, b)
		if e != nil {
			return nil, e
		}
		if b[n] != '\r' || b[n+1] != '\n' {
			// TODO use ProtoError
			return nil, os.NewError("expected trailing CR+LF")
		}
		return b[0:n], nil
	case '*':
		n, e := strconv.Atoi(line[1:])
		if e != nil {
			return nil, e
		}

		// in redis, "*-1" means nil
		if n < 0 {
			return nil, nil
		}

		d := make([]interface{}, n)
		for i := range d {
			d[i], err = decode(r)
			if err != nil {
				return nil, err
			}
		}
		return d, nil
	}

	return nil, os.NewError("unknown line: " + line) // TODO use ProtoError
}

func encode(w io.Writer, data interface{}) (err os.Error) {
	switch t := data.(type) {
	case Line:
		if err = printfLine(w, "+%s", t); err != nil {
			return
		}
	case os.Error:
		if err = printfLine(w, "-%s", t.String()); err != nil {
			return
		}
	case nil:
		if err = printfLine(w, "$-1"); err != nil {
			return
		}
	case int:
		return encodeInt64(w, int64(t))
	case int8:
		return encodeInt64(w, int64(t))
	case int16:
		return encodeInt64(w, int64(t))
	case int32:
		return encodeInt64(w, int64(t))
	case int64:
		return encodeInt64(w, t)
	case uint:
		return encodeUint64(w, uint64(t))
	case uint8:
		return encodeUint64(w, uint64(t))
	case uint16:
		return encodeUint64(w, uint64(t))
	case uint32:
		return encodeUint64(w, uint64(t))
	case uint64:
		return encodeUint64(w, t)
	case string:
		return encodeBytes(w, []byte(t))
	case []byte:
		return encodeBytes(w, t)
	case []string:
		d := make([]interface{}, len(t))
		for i, x := range t {
			d[i] = x
		}
		return encodeSlice(w, d)
	case []interface{}:
		return encodeSlice(w, t)
	default:
		v := reflect.NewValue(data)
		switch tv := v.(type) {
		case *reflect.StructValue:
			return encodeStruct(w, tv)
		default:
			return os.NewError(fmt.Sprintf("unexpected type %T", t))
		}
	}
	return nil
}

func encodeInt64(w io.Writer, data int64) os.Error {
	return printfLine(w, ":%d", data)
}

func encodeUint64(w io.Writer, data uint64) os.Error {
	return printfLine(w, ":%d", data)
}

func encodeBytes(w io.Writer, data []byte) (err os.Error) {
	if err = printfLine(w, "$%d", len(data)); err != nil {
		return
	}
	if err = printfLine(w, "%s", data); err != nil {
		return
	}
	return nil
}

func encodeSlice(w io.Writer, data []interface{}) (err os.Error) {
	if err = printfLine(w, "*%d", len(data)); err != nil {
		return
	}
	for _, part := range data {
		if err = encode(w, part); err != nil {
			return
		}
	}
	return nil
}

func encodeStruct(w io.Writer, v *reflect.StructValue) (err os.Error) {
	n := v.NumField()
	if err = printfLine(w, "*%d", n); err != nil {
		return
	}
	for i := 0; i < n; i++ {
		if err = encode(w, v.Field(i).Interface()); err != nil {
			return
		}
	}
	return nil
}

func printfLine(w io.Writer, format string, args ...interface{}) os.Error {
	_, err := fmt.Fprintf(w, format, args...)
	if err != nil {
		return err
	}
	_, err = w.Write(crnl)
	return err
}

func readLine(r *bufio.Reader) (string, os.Error) {
	line, err := readLineBytes(r)
	return string(line), err
}

func readLineBytes(r *bufio.Reader) ([]byte, os.Error) {
	line, err := r.ReadBytes('\n')
	n := len(line)
	if n > 0 && line[n-1] == '\n' {
		n--
		if n > 0 && line[n-1] == '\r' {
			n--
		}
	}
	return line[0:n], err
}
