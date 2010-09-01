package main

import (
	"flag"
	"os"
	"strconv"

	"junta/paxos"
	"junta/store"
	"junta/util"
	"junta/server"
)

const alpha = 50

// Flags
var (
	listenAddr *string = flag.String("l", ":8040", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

func main() {
	flag.Parse()

	util.LogWriter = os.Stderr
	self := util.RandString(160)

	if *attachAddr {
		c, err := net.Dial("tcp", *attachAddr, nil)
		if err != nil {
			panic(err)
		}

		pc := proto.NewConn(c)

		rid, err := pc.SendRequest("join", self)
		if err != nil {
			panic(err)
		}

		parts, err := pc.ReadResponse(rid)
		if err != nil {
			panic(err)
		}

		if len(parts) < 3 {
			panic("not enough information to join!")
		}

		seqn, err := strconv.Btoui64(parts[0], 10)
		if err != nil {
			panic(err)
		}

		hist := parts[1]
		snap := parts[2]

		st := store.New()
		rg := paxos.NewRegistrar(self, st, alpha)

		st.Apply(1, snap)
		rg.SetHistory(seqn, history) //TODO

		mg := paxos.NewManager(2, rg, paxos.ChanPutCloser(outs))
		go func() {
			panic(server.ListenAndServe(*listenAddr, st, mg))
		}()

		go func() {
			panic(server.ListenAndServeUdp(*listenAddr, mg, outs))
		}()

		for {
			st.Apply(mg.Recv())
		}
	} else {
		outs := make(chan paxos.Msg)

		st := store.New()
		rg := paxos.NewRegistrar(self, st, alpha)

		addMember(st, self, *listenAddr)

		mg := paxos.NewManager(2, rg, paxos.ChanPutCloser(outs))
		go func() {
			panic(server.ListenAndServe(*listenAddr, st, mg))
		}()

		go func() {
			panic(server.ListenAndServeUdp(*listenAddr, mg, outs))
		}()

		for {
			st.Apply(mg.Recv())
		}
	}
}

func addMember(st *store.Store, self, addr string) {
	// TODO pull out path as a const
	mx, err := store.EncodeSet("/j/junta/members/"+self, addr)
	if err != nil {
		panic(err)
	}
	st.Apply(1, mx)
}
