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
type IsTimeToRetryFunc func(clock clock.Clock, lastUpdatedTime time.Time, retryCount int32) bool

type RetryHandler struct {
	IsErrorRetryableFunc IsErrorRetryableFunc
	IsRetryRemainingFunc IsRetryRemainingFunc
	IsErrorFailFastFunc  IsErrorFailFastFunc
	WaitOnErrorFunc      WaitOnErrorFunc
	GetRetryDelayFunc    GetRetryDelayFunc
	IsTimeToRetryFunc    IsTimeToRetryFunc
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

	return time.Duration(time.Now().UnixNano()), true
}

func (e RetryHandler) GetRetryDelay(retryCount int32) time.Duration {
	if e.GetRetryDelayFunc != nil {
		return e.GetRetryDelayFunc(retryCount)
	}

	return time.Duration(time.Now().UnixNano())
}

func (e RetryHandler) IsTimeToRetry(clock clock.Clock, lastUpdatedTime time.Time, retryCount int32) bool {
	if e.IsTimeToRetryFunc != nil {
		return e.IsTimeToRetryFunc(clock, lastUpdatedTime, retryCount)
	}
	return false
}
