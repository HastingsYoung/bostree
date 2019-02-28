package ex_math

func Uint64Min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func Uint64Max(a, b uint64) uint64 {
	if a < b {
		return b
	}
	return a
}
