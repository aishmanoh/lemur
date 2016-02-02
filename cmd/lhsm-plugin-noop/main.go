package main

import (
	"flag"
	"log"

	"github.intel.com/hpdd/policy/pdm/dmplugin"
)

var (
	archive uint
)

func init() {
	flag.UintVar(&archive, "archive", 1, "archive id")
}

type Mover struct {
	fsName    string
	archiveID uint32
}

func (m *Mover) FsName() string {
	return m.fsName
}

func (m *Mover) ArchiveID() uint32 {
	return m.archiveID
}

func noop(agentAddress string) {
	done := make(chan struct{})

	plugin, err := dmplugin.New(agentAddress)
	if err != nil {
		log.Fatal(err)
	}
	mover := Mover{fsName: "noop", archiveID: uint32(archive)}
	plugin.AddMover(&mover)

	<-done
	plugin.Stop()
}

func main() {
	flag.Parse()

	noop("localhost:4242")
}
