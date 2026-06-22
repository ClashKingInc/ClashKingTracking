package platform

import (
	"context"
	"errors"
	"net"

	clashy "github.com/clashkinginc/clashy.go"
)

func IsNonFatalClashError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var invalidArgument *clashy.InvalidArgument
	if errors.As(err, &invalidArgument) {
		return true
	}
	var forbidden *clashy.Forbidden
	if errors.As(err, &forbidden) {
		return true
	}
	var privateWarLog *clashy.PrivateWarLog
	if errors.As(err, &privateWarLog) {
		return true
	}
	var notFound *clashy.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var maintenance *clashy.Maintenance
	if errors.As(err, &maintenance) {
		return true
	}
	var gateway *clashy.GatewayError
	if errors.As(err, &gateway) {
		return gateway.Status == 0 || gateway.Status >= 500
	}
	return false
}
