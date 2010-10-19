package lock

import (
	"junta/assert"
	"junta/store"
	"junta/util"
	"math"
	"os"
	"strconv"
	"testing"
)

const (
	SessionDir = "/session"
	LockDir    = "/lock"

	sessionWatch = SessionDir + "/*"
	lockWatch    = LockDir + "/**"
)

type Lock struct {
	ch     chan store.Event
}

func New(st *store.Store) *Lock {
	ch := make(chan store.Event)
	st.Watch("/session/*", ch)

	lk := &Lock{ch}
	go lk.process()

	return lk
}

func (lk *Lock) process() {
	logger := util.NewLogger("lock")
	for ev := range lk.ch {
		path := lk.Path[len(LockDir):]
		mut, _ := store.EncodeDel(path, store.Clobber)
		// How do I get the seqn to use here????
		lk.st.Apply(
	}
}

func (lk *Lock) Close() {
	close(lk.ch)
}

// Testing

const (
	me       = "me"
	locks    = "/lock"
	sessions = "/session"
)

func start(st *store.Store, seqn int, owner string) {
	mut := store.MustEncodeSet(
		SessionDir+"/"+owner,
		strconv.Uitoa64(math.MaxUint64),
		store.Clobber,
	)
	st.Apply(uint64(seqn), mut)
}

func stop(st *store.Store, seqn int, owner string) {
	logger := util.NewLogger("test:stop")
	logger.Log("stopping")
	mut := store.MustEncodeDel(
		SessionDir+"/"+owner,
		store.Clobber,
	)
	logger.Logf("MUT: %s", mut)
	st.Apply(uint64(seqn), mut)
}

func lock(st *store.Store, seqn int, owner string, what string) {
	mut := store.MustEncodeSet(
		LockDir+"/"+what,
		owner,
		store.Clobber,
	)
	st.Apply(uint64(seqn), mut)
}

func TestLockSingle(t *testing.T) {
	util.LogWriter = os.Stderr
	t.Error("running!")
	st := store.New()
	lk := New(st)
	defer lk.Close()

	t.Error("start")
	// Start our session so we can aquire locks
	start(st, 1, me)

	lock(st, 2, me, "/a")
	lock(st, 3, me, "/b")

	ch := make(chan store.Event)
	st.Watch(lockWatch, ch)

	t.Error("stopping")
	stop(st, 4, me)

	ev := <-ch
	assert.Equal(t, 1, ev)

	ev = <-ch
	assert.Equal(t, 1, ev)
}
