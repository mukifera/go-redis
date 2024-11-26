package rdb

func createSet[T comparable](items ...T) map[T]struct{} {
	ret := make(map[T]struct{})
	for _, item := range items {
		ret[item] = struct{}{}
	}
	return ret
}

func equalSets[T comparable](a map[T]struct{}, b map[T]struct{}) bool {
	for key := range a {
		if _, ok := b[key]; !ok {
			return false
		}
	}
	for key := range b {
		if _, ok := a[key]; !ok {
			return false
		}
	}
	return true
}

func equalSlices(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
