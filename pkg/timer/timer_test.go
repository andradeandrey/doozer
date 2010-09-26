package timer

import (
	"junta/store"
	"log"
	"testing"
	"time"
)

const (
	oneSecond = 1000000000
)

type Timer struct {
	C chan string

	st *store.Store
	tk *time.Ticker
}

func New(st *store.Store) *Timer {
	t := &Timer{
		make(chan string),
		st,
		time.NewTicker(1),
	}
	go t.process()
	return t
}

func (t *Timer) process() {
	// Start the timer
	tick := time.NewTicker(oneSecond)

	// Begin watching as timers come and go
	events := make(chan store.Event)
	t.st.Watch("/j/timer/**", events)

	for {
		select {
		case e := <-events:
			log.Stderrf("%v\n", e)
		case <-tick.C:
			log.Stderrf("tick!\n")
		}
	}
}

func (t *Timer) Close() {

}

// Testing

func TestOneshotTimer(t *testing.T) {
	// Start the timer process
	st := store.New()
	tx := New(st)
	defer tx.Close()

	t.Errorf("%d\n", time.Seconds())

	path := "/j/timer/foo/bar"
	muta := store.MustEncodeSet(path, store.Clobber, "1")

	st.Apply(1, muta)

	t.Errorf("%q", <-tx.C)
	t.Errorf("%d\n", time.Seconds())
}
