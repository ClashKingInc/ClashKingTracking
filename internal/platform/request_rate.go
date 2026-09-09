package platform

// RequestConcurrency gives each tracker enough in-flight capacity to sustain
// its configured request-start rate when normal proxy latency approaches three
// seconds. The limiter still controls request starts; this value only prevents
// latency from becoming an artificial rate ceiling.
func RequestConcurrency(requestsPerSecond int) int {
	if requestsPerSecond <= 0 {
		return 0
	}
	return requestsPerSecond * 3
}
