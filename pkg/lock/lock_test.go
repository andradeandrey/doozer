package lock

import (
	"doozer/assert"
	"doozer/store"
	"doozer/test"
	"testing"
)

func TestLockSimple(t *testing.T) {
	st := store.New()
	defer close(st.Ops)
	fp := &test.FakeProposer{Store: st}
	go Clean(fp.Store, fp)

	// start our session
	fp.Propose(store.MustEncodeSet("/session/a", "1.2.3.4:55", store.Clobber))

	// lock something for a
	fp.Propose(store.MustEncodeSet("/lock/x", "a", store.Missing))
	fp.Propose(store.MustEncodeSet("/lock/y", "b", store.Missing))
	fp.Propose(store.MustEncodeSet("/lock/z", "a", store.Missing))

	// watch the locks to be deleted
	ch := make(chan store.Event)
	fp.Watch("/lock/*", ch)

	// end the session
	fp.Propose(store.MustEncodeDel("/session/a", store.Clobber))

	// now that the session has ended, check all locks it owned are released
	assert.Equal(t, "/lock/x", (<-ch).Path)
	assert.Equal(t, "/lock/z", (<-ch).Path)
}
