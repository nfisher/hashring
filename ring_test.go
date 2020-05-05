package hashring

import (
	"reflect"
	"testing"
)

func Test_init_all_hosts(t *testing.T) {
	hostnames := []string{"node01", "node02", "node03", "node04", "node05"}
	ring := NewRing(hostnames, 3)
	if len(ring.seeds) != 5 {
		t.Errorf("got len(ring.seeds)=%v, want 5", len(ring.seeds))
	}

	expect := map[string]uint64{
		"node01": 16643466673093620226,
		"node02": 16643465573581992015,
		"node03": 16643464474070363804,
		"node04": 16643463374558735593,
		"node05": 16643462275047107382,
	}
	if !reflect.DeepEqual(ring.seeds, expect) {
		t.Errorf("got %v, want %v", ring.seeds, expect)
	}

	if len(ring.positions) != 15 {
		t.Errorf("got len(ring.positions)=%v, want 15", len(ring.positions))
	}

	positions := []uint32{
		106181428,
		144089888,
		180650148,
		593550213,
		685391244,
		1306359544,
		2986058550,
		3099279991,
		3168331354,
		3547494898,
		3643005514,
		3763303373,
		3822899824,
		3993943019,
		4064395128,
	}
	if !reflect.DeepEqual(positions, ring.positions) {
		t.Errorf("got ring.positions=%v, want %v", ring.positions, positions)
	}
}

func Test_add_host(t *testing.T) {
	hostnames := []string{"node01", "node02", "node03", "node04"}
	ring := NewRing(hostnames, 3)
	if len(ring.seeds) != 4 {
		t.Errorf("got len(ring.seeds)=%v, want 4", len(ring.seeds))
	}

	ring.Add("node05")
	expect := map[string]uint64{
		"node01": 16643466673093620226,
		"node02": 16643465573581992015,
		"node03": 16643464474070363804,
		"node04": 16643463374558735593,
		"node05": 16643462275047107382,
	}
	if !reflect.DeepEqual(ring.seeds, expect) {
		t.Errorf("got %v, want %v", ring.seeds, expect)
	}

	if len(ring.positions) != 15 {
		t.Errorf("got len(ring.positions)=%v, want 15", len(ring.positions))
	}

	positions := []uint32{
		106181428,
		144089888,
		180650148,
		593550213,
		685391244,
		1306359544,
		2986058550,
		3099279991,
		3168331354,
		3547494898,
		3643005514,
		3763303373,
		3822899824,
		3993943019,
		4064395128,
	}
	if !reflect.DeepEqual(positions, ring.positions) {
		t.Errorf("got ring.positions=%v, want %v", ring.positions, positions)
	}
}

func Test_remove_host(t *testing.T) {
	hostnames := []string{"node01", "node02", "node03", "node04", "node05"}
	ring := NewRing(hostnames, 3)
	if len(ring.seeds) != 5 {
		t.Errorf("got len(ring.seeds)=%v, want 5", len(ring.seeds))
	}

	ring.Remove("node02")
	expect := map[string]uint64{
		"node01": 16643466673093620226,
		"node03": 16643464474070363804,
		"node04": 16643463374558735593,
		"node05": 16643462275047107382,
	}
	if !reflect.DeepEqual(ring.seeds, expect) {
		t.Errorf("got %v, want %v", ring.seeds, expect)
	}

	if len(ring.positions) != 12 {
		t.Errorf("got len(ring.positions)=%v, want 12", len(ring.positions))
	}

	positions := []uint32{
		106181428,
		144089888,
		180650148,
		593550213,
		1306359544,
		2986058550,
		3099279991,
		3168331354,
		3643005514,
		3763303373,
		3822899824,
		3993943019,
	}
	if !reflect.DeepEqual(positions, ring.positions) {
		t.Errorf("got ring.positions=%v, want %v", ring.positions, positions)
	}
}
