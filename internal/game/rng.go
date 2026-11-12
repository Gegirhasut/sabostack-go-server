package game

// RNG is a tiny deterministic generator (SplitMix64). Its whole state is one uint64, so it
// serialises trivially into match snapshots and a restored match continues the exact
// same piece sequence.
type RNG struct {
	State uint64 `json:"state"`
}

// Next returns the next pseudo-random 64-bit value.
func (r *RNG) Next() uint64 {
	r.State += 0x9e3779b97f4a7c15
	z := r.State
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Intn returns a value in [0, n).
func (r *RNG) Intn(n int) int {
	return int(r.Next() % uint64(n))
}
