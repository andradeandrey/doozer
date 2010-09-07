package store

import (
	"container/vector"
	"junta/assert"
	"log"
	"os"
	"testing"
)

const (
	Wait = uint(1 << iota)
	Set
	Once
)

type Reply struct {
	seqn uint64
	path string
	body string
}

type req struct {
	seqn uint64
	path string
	body string
	mask uint
	ch   chan<- Reply
}

func (r req) Less(y interface{}) bool {
	return r.seqn < y.(req).seqn
}

type Store struct {
	seqn  uint64
	reqCh chan req
	waits *vector.Vector
}

func New() *Store {
	s := &Store{
		seqn:  0,
		reqCh: make(chan req),
		waits: new(vector.Vector),
	}

	go s.process()

	return s
}

func (s *Store) Close() {
	close(s.reqCh)
}

func (s *Store) process() {
	// NOTE:  There should never be an error in here
	for r := range s.reqCh {
		log.Stdoutf("r:%v\n", r)
		if r.mask&Wait != 0 {
			log.Stdoutf("pw:%v\n", r)
			s.waits.Push(r)
		}

		if r.mask&Set != 0 {
			s.seqn++
			// TODO: Create the path and set the body
			for n, x := range *s.waits {
				log.Stdoutf("n:%d x:%v\n", n, x)
				w := x.(req)
				if w.seqn <= s.seqn {
					w.ch <- Reply{s.seqn, r.path, r.body}
					if w.mask&Once != 0 {
						s.waits.Delete(n)
					}
				}
			}
		}
	}
}

func (s *Store) req(seqn uint64, path, body string, mask uint, ch chan<- Reply) {
	s.reqCh <- req{seqn, path, body, mask, ch}
}

func (s *Store) Wait(seqn uint64, path string, ch chan<- Reply) os.Error {
	s.req(seqn, path, "", Wait, ch)
	return nil
}

func (s *Store) Set(seqn uint64, path, body string) (Reply, os.Error) {
	ch := make(chan Reply)
	s.req(seqn, path, body, Wait|Once|Set, ch)
	return <-ch, nil
}

// Testing

func TestStoreSetSimple(t *testing.T) {
	s := New()
	defer s.Close()

	var got Reply

	got, _ = s.Set(1, "/foo", "bar")
	assert.Equal(t, uint64(1), got.seqn)
	assert.Equal(t, "/foo", got.path)
	assert.Equal(t, "bar", got.body)

	got, _ = s.Set(2, "/foo", "rab")
	assert.Equal(t, uint64(2), got.seqn)
	assert.Equal(t, "/foo", got.path)
	assert.Equal(t, "rab", got.body)
}

func TestStoreWaitSimple(t *testing.T) {
	s := New()
	defer s.Close()

	ch := make(chan Reply, 2)

	s.Wait(0, "/foo", ch)

	setCh := make(chan Reply)
	go func() {
		set, _ := s.Set(1, "/foo", "bar")
		setCh <- set
	}()

	exp := <-setCh
	assert.Equal(t, exp, <-ch)
}

func TestStoreWaitAfterSet(t *testing.T) {
	s := New()
	defer s.Close()

	ch := make(chan Reply, 2)

	setCh := make(chan Reply)
	go func() {
		set, _ := s.Set(1, "/foo", "bar")
		setCh <- set
	}()

	s.Wait(0, "/foo", ch)

	exp := <-setCh
	assert.Equal(t, exp, <-ch)
}

func TestStoreWaitFuture(t *testing.T) {
	s := New()
	defer s.Close()

	ch := make(chan Reply, 2)

	s.Wait(2, "/foo", ch)
	go s.Set(1, "/foo", "bar")
	go s.Set(2, "/foo", "rab")

	got := <-ch
	assert.Equal(t, uint64(2), got.seqn)
	assert.Equal(t, "rab", got.body)
}

func TestStoreSetOutOfOrder(t *testing.T) {
	s := New()
	defer s.Close()

	st1 := make(chan int)
	st2 := make(chan int)
	ch := make(chan Reply)

	go func() {
		<-st1
		r, _ := s.Set(1, "/foo", "bar")
		st1 <- 1
		ch <- r
	}()

	go func() {
		<-st2
		r, _ := s.Set(2, "/foo", "rab")
		st2 <- 1
		ch <- r
	}()

	st2 <- 1
	<-st2
	st1 <- 1
	<-st1

	got := <-ch
	assert.Equal(t, uint64(1), got.seqn)

	got = <-ch
	assert.Equal(t, uint64(2), got.seqn)
}
