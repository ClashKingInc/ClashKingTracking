package utils

import "testing"

func TestRankedAttackTrophyBoundaries(t *testing.T) {
	tests := []struct{ stars, destruction, want int }{
		{0, 0, 0}, {0, 9, 0}, {0, 10, 1}, {0, 49, 4},
		{1, 1, 5}, {1, 9, 5}, {1, 10, 6}, {1, 90, 14}, {1, 91, 15}, {1, 99, 15},
		{2, 50, 16}, {2, 52, 16}, {2, 53, 17}, {2, 97, 31}, {2, 98, 32}, {2, 99, 32},
		{3, 0, 40}, {3, 100, 40},
	}
	for _, test := range tests {
		if got := RankedAttackTrophies(test.stars, test.destruction); got != test.want {
			t.Fatalf("RankedAttackTrophies(%d, %d) = %d, want %d", test.stars, test.destruction, got, test.want)
		}
	}
}
