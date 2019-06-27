package mock

import (
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

type IsErrorRetryableFunc func(err error) bool
type IsRetryRemainingFunc func(err error, retryCount int32) bool
type IsErrorFailFastFunc func(err error) bool
type WaitOnErrorFunc func(clock clock.Clock, lastUpdatedTime time.Time) (time.Duration, bool)
type GetRetryDelayFunc func(retryCount int32) time.Duration
type BackOffFunc func(retryCount int32)

type RetryHandler struct {
	IsErrorRetryableFunc IsErrorRetryableFunc
	IsRetryRemainingFunc IsRetryRemainingFunc
	IsErrorFailFastFunc  IsErrorFailFastFunc
	WaitOnErrorFunc      WaitOnErrorFunc
	GetRetryDelayFunc    GetRetryDelayFunc
	BackOffFunc          BackOffFunc
}

func (e RetryHandler) IsErrorRetryable(err error) bool {
	if e.IsErrorRetryableFunc != nil {
		return e.IsErrorRetryableFunc(err)
	}

	return false
}

func (e RetryHandler) IsErrorFailFast(err error) bool {
	if e.IsErrorFailFastFunc != nil {
		return e.IsErrorFailFastFunc(err)
	}

	return false
}

func (e RetryHandler) IsRetryRemaining(err error, retryCount int32) bool {
	if e.IsRetryRemainingFunc != nil {
		return e.IsRetryRemainingFunc(err, retryCount)
	}

	return false
}

func (e RetryHandler) WaitOnError(clock clock.Clock, lastUpdatedTime time.Time) (time.Duration, bool) {
	if e.WaitOnErrorFunc != nil {
		return e.WaitOnErrorFunc(clock, lastUpdatedTime)
	}

	return time.Duration(time.Now().UnixNano()), false
}

func (e RetryHandler) GetRetryDelay(retryCount int32) time.Duration {
	if e.GetRetryDelayFunc != nil {
		return e.GetRetryDelayFunc(retryCount)
	}

	return time.Duration(time.Now().UnixNano())
}

func (e RetryHandler) BackOff(retryCount int32) {
	if e.BackOffFunc != nil {
		e.BackOffFunc(retryCount)
	}
}
