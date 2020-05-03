package hashring

import "testing"

func Test_seed_start(t *testing.T) {
	hostnames := []string{
		"node01",
		"node02",
		"node03",
		"node04",
		"node05",
	}
	_ = NewRing(hostnames, 10)
}

func NewRing(hostnames []string, vnodes int) *Ring {
	return &Ring{}
}

type Ring struct {
}
