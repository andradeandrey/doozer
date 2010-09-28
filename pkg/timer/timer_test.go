package timer

import (
	"container/heap"
	"container/vector"
	"junta/store"
	"junta/util"
	"math"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	oneSecond = 1e9
)

type Tick struct {
	Path string
	At int64
}

type Timer struct {
	// A name for the timer
	Name string

	// Watches ticked are sent here
	C chan Tick

	st *store.Store
	events chan store.Event
}

func New(name string, st *store.Store) *Timer {
	t := &Timer{
		name,
		make(chan Tick),
		st,
		make(chan store.Event),
	}

	go t.process()

	return t
}

func (t *Timer) process() {
	logger := util.NewLogger("timer (%s)", t.Name)

	ticks := new(vector.Vector)
	heap.Init(ticks)

	peek := func() Tick {
		if ticks.Len() == 0 {
			return Tick{At:math.MaxInt64}
		}
		return ticks.At(0).(Tick)
	}

	// Start the timer
	ticker := time.NewTicker(oneSecond)

	// Begin watching as timers come and go
	t.st.Watch("/j/timer/**", t.events)

	for {
		select {
		case e := <-t.events:
			if closed(t.events) {
				goto done
			}

			logger.Logf("recvd: %v", e)
			// TODO: Handle/Log the next error
			// I'm not sure if we should notify the client
			// on Set.  That seems like it would be difficult
			// with the currect way the code functions.  Dunno.
			at, _ := strconv.Atoi64(e.Body)

			x := Tick{e.Path, at}
			heap.Push(ticks, x)
		case <-ticker.C:
			ns := time.Nanoseconds()
			logger.Logf("ns (%d)", ns)
			logger.Logf("peek (%v)", peek())
			for next := peek() ; next.At <= ns; next = peek() {
				logger.Logf("ticked %#v", next)
				heap.Pop(ticks)
				t.C <- next
			}
		}
	}

done:
	ticker.Stop()
}

func (t *Timer) Close() {
	close(t.events)
}

// Testing


func TestOneshotTimer(t *testing.T) {
	util.LogWriter = os.Stderr

	// Start the timer process
	st := store.New()
	timer := New("test", st)
	defer timer.Close()

	t.Errorf("%d\n", time.Seconds())

	path := "/j/timer/foo/bar"
	muta := store.MustEncodeSet(
		path,
		strconv.Itoa64(time.Nanoseconds() + (oneSecond * 5)),
		store.Clobber,
	)

	st.Apply(1, muta)

	t.Errorf("%q", <-timer.C)
	t.Errorf("%d\n", time.Seconds())
}
