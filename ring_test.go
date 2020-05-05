package hashring

import (
	"reflect"
	"testing"
)

func Test_bucket_for(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		bucket string
	}{
		{"default/pinger", "node01"},
		{"instana/instana-agent", "node02"},
		{"kube-system/metrics-server", "node01"},
		{"instana-agent/daemon", "node05"},
	}

	hostnames := []string{"node01", "node02", "node03", "node04", "node05"}
	ring := New(hostnames, 3)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bucket := ring.Bucket(tc.name)
			if bucket != tc.bucket {
				t.Errorf("bucket=%v, want %v", bucket, tc.bucket)
			}
		})
	}
}

func Test_nearest_for(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		input   uint32
		nearest uint32
	}{
		{"default/pinger", 2996425964, 3099279991},
		{"instana/instana-agent", 3313770152, 3547494898},
		{"kube-system/metrics-server", 1085184088, 1306359544},
		{"wrap-to-index-zero", 4064395129, 106181428},
	}
	hostnames := []string{"node01", "node02", "node03", "node04", "node05"}
	ring := New(hostnames, 3)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nearest := ring.nearest(tc.input)
			if nearest != tc.nearest {
				t.Errorf("nearest=%v, want %v", nearest, tc.nearest)
			}
		})
	}
}

func Test_init_all_hosts(t *testing.T) {
	t.Parallel()
	hostnames := []string{"node01", "node02", "node03", "node04", "node05"}
	ring := New(hostnames, 3)
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

	if len(ring.index) != 15 {
		t.Errorf("got len(ring.index)=%v, want 15", len(ring.index))
	}
}

func Test_add_host(t *testing.T) {
	t.Parallel()
	hostnames := []string{"node01", "node02", "node03", "node04"}
	ring := New(hostnames, 3)
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

	if len(ring.index) != 15 {
		t.Errorf("got len(ring.index)=%v, want 15", len(ring.index))
	}
}

func Test_remove_host(t *testing.T) {
	t.Parallel()
	hostnames := []string{"node01", "node02", "node03", "node04", "node05"}
	ring := New(hostnames, 3)
	if len(ring.seeds) != 5 {
		t.Errorf("got len(ring.seeds)=%v, want 5", len(ring.seeds))
	}

	ring.Remove("node02")
	seeds := map[string]uint64{
		"node01": 16643466673093620226,
		"node03": 16643464474070363804,
		"node04": 16643463374558735593,
		"node05": 16643462275047107382,
	}
	if !reflect.DeepEqual(ring.seeds, seeds) {
		t.Errorf("got %v, want %v", ring.seeds, seeds)
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

	if len(ring.index) != 12 {
		t.Errorf("got len(ring.index)=%v, want 12", len(ring.index))
	}
}
