package utils

// RankedAttackTrophies reproduces the integer trophy steps used by Ranked and
// Legend attacks. Callers use the same value for the attack gain and Legend
// real-defense loss.
func RankedAttackTrophies(stars, destruction int) int {
	destruction = min(max(destruction, 0), 100)
	switch {
	case stars <= 0:
		return min(4, destruction/10)
	case stars == 1:
		return 5 + min(10, max(0, (destruction-1)/9))
	case stars == 2:
		return 16 + min(16, max(0, (destruction-50)/3))
	default:
		return 40
	}
}
