package utils

func Split[T any](items []T, size int) [][]T {
	if size <= 0 {
		return nil
	}
	// Pre-sizing keeps the helper cheap when it is used for large tracking batches.
	result := make([][]T, 0, (len(items)+size-1)/size)
	for start := 0; start < len(items); start += size {
		end := start + size
		if end > len(items) {
			end = len(items)
		}
		result = append(result, items[start:end])
	}
	return result
}

func ChunkInto[T any](items []T, parts int) [][]T {
	if parts <= 0 {
		return nil
	}
	// The remainder is spread across the first chunks so the sizes stay as even as possible.
	out := make([][]T, 0, parts)
	k, m := len(items)/parts, len(items)%parts
	start := 0
	for i := 0; i < parts; i++ {
		size := k
		if i < m {
			size++
		}
		end := start + size
		if end > len(items) {
			end = len(items)
		}
		out = append(out, items[start:end])
		start = end
	}
	return out
}
