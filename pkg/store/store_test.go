package store

import (
	"container/heap"
	"container/vector"
	"junta/assert"
	"testing"
)

const (
	Watch = uint(1 << iota)
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
	watches *vector.Vector
}

func New() *Store {
	s := &Store{
		seqn:    0,
		reqCh:   make(chan req),
		watches: new(vector.Vector),
	}

	heap.Init(s.watches)

	go s.process()

	return s
}

func (s *Store) Close() {
	close(s.reqCh)
}

func (s *Store) process() {
	for r := range s.reqCh {
		if r.mask & Watch != 0 {
			heap.Push(s.watches, r)
		}

		if r.mask & Set != 0 {
			s.seqn++
			// TODO: Create the path and set the body
			s.watches.Do(func(x interface{}) {
				w := x.(req)
				if w.seqn <= s.seqn {
					w.ch <- Reply{s.seqn, r.path, r.body}
				}
			})
		}
	}
}

func (s *Store) Watch(seqn uint64, path string, ch chan<- Reply) {
	s.reqCh <- req{seqn, path, "", Watch, ch}
}

func (s *Store) Set(seqn uint64, path, body string, ch chan<- Reply) {
	s.reqCh <- req{seqn, path, body, Watch | Set, ch}
}

// Testing

func TestStoreSet(t *testing.T) {
	s := New()
	ch := make(chan Reply)
	s.Set(1, "/foo", "bar", ch)

	got := <-ch
	assert.Equal(t, uint64(1), got.seqn)
}
