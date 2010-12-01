package doozer

import (
	"doozer/client"
	"net"
	"testing"
)

// TODO make sure all these goroutines are cleaned up nicely

func mustListen() net.Listener {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	return l
}

func TestFoo(t *testing.T) {
	l := mustListen()
	a0 := l.Addr().String()

	done := make(chan int)
	defer close(done)

	go Main("a", "", l, nil, done)
	go Main("a", a0, mustListen(), nil, done)
	go Main("a", a0, mustListen(), nil, done)

	cl, err := client.Dial(a0)
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Noop()
	if err != nil {
		t.Fatal(err)
	}

	// cl.Get("/doozer/members")
	//for m in members {
	//	cl.Get(/session/m)
	//	cl.Get(/doozer/info/m/applied)
	//}
}
