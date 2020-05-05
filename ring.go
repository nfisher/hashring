package hashring

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"sort"
)

// New initialise a consistent hash ring with the specified vnodes per host.
func New(hostnames []string, vnodes int) *Ring {
	seeds := make(map[string]uint64)
	var positions []uint32
	ring := &Ring{
		seeds:     seeds,
		positions: positions,
		vnodes:    vnodes,
	}

	hasher := fnv.New64a()
	for _, h := range hostnames {
		ring.add(h, hasher)
	}

	return ring
}

// Ring is a consistent hash ring with the specified vnodes per host.
type Ring struct {
	vnodes    int
	seeds     map[string]uint64
	positions []uint32
}

func (r *Ring) add(h string, hasher hash.Hash64) {
	hasher.Write([]byte(h))
	r.seeds[h] = hasher.Sum64()
	hasher.Reset()
	src := rand.NewSource(int64(r.seeds[h]))
	rint := rand.New(src)
	for i := 0; i < r.vnodes; i++ {
		r.positions = append(r.positions, rint.Uint32())
	}
	sort.Slice(r.positions, func(i, j int) bool { return r.positions[i] < r.positions[j] })
}

// Add the supplied hostname to the ring.
func (r *Ring) Add(h string) {
	hasher := fnv.New64a()
	r.add(h, hasher)
	sort.Slice(r.positions, func(i, j int) bool { return r.positions[i] < r.positions[j] })
}

// Remove the supplied hostname from the ring.
func (r *Ring) Remove(h string) {
	src := rand.NewSource(int64(r.seeds[h]))
	rint := rand.New(src)
	for i := 0; i < r.vnodes; i++ {
		val := rint.Uint32()
		for i, v := range r.positions {
			if v == val {
				r.positions = append(r.positions[:i], r.positions[i+1:]...)
				break
			}
		}
	}
	delete(r.seeds, h)
}
