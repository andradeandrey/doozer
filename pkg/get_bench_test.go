package junta

import (
	"fmt"
	"junta/proto"
	"junta/store"
	"net"
	"testing"
	"log"
)

func BenchmarkGet(b *testing.B) {
	conn, err := net.Dial("tcp", "", "127.0.0.1:8046")
	if err != nil {
		panic(err)
	}
	junta := proto.NewConn(conn)

	b.StartTimer()
	for i := 0; i < 10000; i++ {
		id, err := junta.SendRequest("sget", "/j/local/ping")
		if err != nil {
			panic(err)
		}

		parts, err := junta.ReadResponse(id)
		if err != nil {
			panic(err)
		}

		if len(parts) != 1 && parts[0] != "pong" {
			panic(fmt.Sprintf("Invalid parts %v", parts))
		}
	}
	b.StopTimer()
}

func BenchmarkSet(b *testing.B) {
	conn, err := net.Dial("tcp", "", "127.0.0.1:8046")
	if err != nil {
		panic(err)
	}
	junta := proto.NewConn(conn)

	b.StartTimer()
	for i := 0; i < 10000; i++ {
		id, err := junta.SendRequest("set", "/j/local/bench/set", "abc", store.Clobber)
		if err != nil {
			panic(err)
		}

		parts, err := junta.ReadResponse(id)
		if err != nil {
			panic(err)
		}

		log.Stdoutf("got response %v\n", parts)
		if len(parts) != 1 && parts[0] != "pong" {
			panic(fmt.Sprintf("Invalid parts %v", parts))
		}
	}
	b.StopTimer()
}
