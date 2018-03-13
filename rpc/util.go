package rpc

import (
	"fmt"
	"math"
)

// InGroups calls f for sub slices of a slice where every slice
// is at most `size` big
func InGroups(set []string, size int, f func([]string) error) error {
	count := math.Ceil(float64(len(set)) / float64(size))

	for i := 0; i < int(count); i++ {
		start := i * int(size)
		end := start + int(size)

		if end > len(set) {
			end = len(set)
		}

		err := f(set[start:end])
		if err != nil {
			return fmt.Errorf("publishing failed on batch %d:%d %s", start, end, err)
		}
	}

	return nil
}
