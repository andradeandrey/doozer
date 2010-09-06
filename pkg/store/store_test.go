package store

import (
	"container/vector"
	"junta/assert"
	"os"
	"testing"
)

const (
	Wait = uint(1 << iota)
	Set
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
	seqn    uint64
	reqCh   chan req
	waits *vector.Vector
}

func New() *Store {
	s := &Store{
		seqn:    0,
		reqCh:   make(chan req),
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
		if r.mask & Wait != 0 {
			s.waits.Push(r)
		}

		if r.mask & Set != 0 {
			s.seqn++
			// TODO: Create the path and set the body
			waits := []interface(s.waits)
			for n, x := range waits {
				// Lazily GC old waits
				if w.seqn <= s.seqn {
					w := x.(req)
					w.ch <- Reply{s.seqn, r.path, r.body}
					if w.mask | Once != 0 {
						s.waits.Delete(n)
					}
				}
			}
		}
	}
}

func (s *Store) Req(seqn uint64, path, body string, mask uint, ch chan<- Reply) {
	s.reqCh <- req{seqn, path, body, mask, ch}
}

func (s *Store) Wait(seqn uint64, path string) (Reply, os.Error) {
	ch := make(chan Reply)
	s.Req(seqn, path, "", Wait, ch)
	return <-ch, nil
}

func (s *Store) Set(seqn uint64, path, body string) (Reply, os.Error) {
	ch := make(chan Reply)
	s.Req(seqn, path, body, Wait | Once | Set, ch)
	return <-ch, nil
}


// Testing

func TestStoreSetSimple(t *testing.T) {
	s := New()

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
