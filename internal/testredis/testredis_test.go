package testredis

import "testing"

// A real server's database is flushed by every test, so the target must name one, and
// not the default database 0.
func TestParseTargetRefusesUnsafeDatabases(t *testing.T) {
	for v, ok := range map[string]bool{
		"localhost:6379/15": true,
		"10.0.0.5:6380/1":   true,
		"localhost:6379":    false, // no database: would have been db 0
		"localhost:6379/0":  false,
		"localhost:6379/":   false,
		"localhost:6379/x":  false,
		"localhost:6379/-1": false,
	} {
		addr, db, err := parseTarget(v)
		if (err == nil) != ok {
			t.Errorf("%q: err=%v, want ok=%v", v, err, ok)
		}
		if ok && (addr == "" || db < 1) {
			t.Errorf("%q: addr %q db %d", v, addr, db)
		}
	}
}
