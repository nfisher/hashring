package hashring

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"sort"
)

// New initialise a consistent hash ring with the specified vnodes per host.
func New(hostnames []string, vnodes int) *Ring {
	ring := &Ring{
		index:     make(map[uint32]string),
		positions: []uint32{},
		seeds:     make(map[string]uint64),
		vnodes:    vnodes,
	}

	hasher := fnv.New64a()
	for _, h := range hostnames {
		ring.add(h, hasher)
	}
	sort.Slice(ring.positions, func(i, j int) bool { return ring.positions[i] < ring.positions[j] })

	return ring
}

// Ring is a consistent hash ring with the specified vnodes per host.
type Ring struct {
	// TODO: benchmark string array vs map
	index     map[uint32]string
	positions []uint32
	seeds     map[string]uint64
	vnodes    int
}

func (r *Ring) add(h string, hasher hash.Hash64) {
	hasher.Write([]byte(h))
	r.seeds[h] = hasher.Sum64()
	hasher.Reset()
	src := rand.NewSource(int64(r.seeds[h]))
	rint := rand.New(src)
	for i := 0; i < r.vnodes; i++ {
		pos := rint.Uint32()
		r.index[pos] = h
		r.positions = append(r.positions, pos)
	}
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
				delete(r.index, v)
				r.positions = append(r.positions[:i], r.positions[i+1:]...)
				break
			}
		}
	}
	delete(r.seeds, h)
}

// Bucket indicates which hostname the input is routed to.
func (r *Ring) Bucket(input string) string {
	h := fnv.New32a()
	h.Write([]byte(input))
	n := r.nearest(h.Sum32())
	return r.index[n]
}

func (r *Ring) nearest(sum uint32) uint32 {
	var n = r.positions[0]
	for _, v := range r.positions {
		if v >= sum {
			n = v
			break
		}
	}

	return n
}
