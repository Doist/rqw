package meminfo

import "testing"

func TestMemInfo(t *testing.T) {
	memfile = `./meminfo.txt`
	mi := new(MemInfo)
	if err := mi.Update(); err != nil {
		t.Fatal(err)
	}
	var exp int64 = 1592553472
	if v := mi.FreeTotal(); v != exp {
		t.Fatalf("Free(): expected %d, got %d", exp, v)
	}
}
