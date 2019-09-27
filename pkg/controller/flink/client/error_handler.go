package client

import (
	"fmt"
	"math/rand"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/clock"
)

// appError codes
const (
	GlobalFailure      = "FAILED"
	JSONUnmarshalError = "JSONUNMARSHALERROR"
	DefaultRetries     = 20
	NoRetries          = 0
)

func GetRetryableError(err error, method v1beta1.FlinkMethod, errorCode string, maxRetries int32) error {
	return GetRetryableErrorWithMessage(err, method, errorCode, maxRetries, "")
}

func GetRetryableErrorWithMessage(err error, method v1beta1.FlinkMethod, errorCode string, maxRetries int32, message string) error {
	appError := getErrorValue(err, method, errorCode, message)
	return NewFlinkApplicationError(appError.Error(), method, errorCode, true, false, maxRetries)
}

func GetNonRetryableError(err error, method v1beta1.FlinkMethod, errorCode string) error {
	return GetNonRetryableErrorWithMessage(err, method, errorCode, "")
}

func GetNonRetryableErrorWithMessage(err error, method v1beta1.FlinkMethod, errorCode string, message string) error {
	appError := getErrorValue(err, method, errorCode, message)
	return NewFlinkApplicationError(appError.Error(), method, errorCode, false, true, NoRetries)
}

func getErrorValue(err error, method v1beta1.FlinkMethod, errorCode string, message string) error {
	if err == nil {
		return errors.New(fmt.Sprintf("%v call failed with status %v and message '%s'", method, errorCode, message))
	}
	return errors.Wrapf(err, "%v call failed with status %v and message '%s'", method, errorCode, message)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type RetryHandlerInterface interface {
	IsErrorRetryable(err error) bool
	IsRetryRemaining(err error, retryCount int32) bool
	WaitOnError(clock clock.Clock, lastUpdatedTime time.Time) (time.Duration, bool)
	GetRetryDelay(retryCount int32) time.Duration
	IsTimeToRetry(clock clock.Clock, lastUpdatedTime time.Time, retryCount int32) bool
}

// A Retryer that has methods to determine if an error is retryable and also does exponential backoff
type RetryHandler struct {
	baseBackOffDuration      time.Duration
	maxErrDuration           time.Duration
	maxBackOffMillisDuration time.Duration
}

func NewRetryHandler(baseBackoff time.Duration, timeToWait time.Duration, maxBackOff time.Duration) RetryHandler {
	rand.Seed(time.Now().UnixNano())
	return RetryHandler{baseBackoff, timeToWait, maxBackOff}
}
func (r RetryHandler) IsErrorRetryable(err error) bool {
	if err == nil {
		return false
	}
	flinkAppError, ok := err.(*v1beta1.FlinkApplicationError)
	if ok && flinkAppError != nil {
		return flinkAppError.IsRetryable
	}

	return false
}

func (r RetryHandler) IsRetryRemaining(err error, retryCount int32) bool {
	flinkAppError, ok := err.(*v1beta1.FlinkApplicationError)
	if ok && flinkAppError != nil {
		return retryCount <= flinkAppError.MaxRetries
	}

	return false
}

func (r RetryHandler) WaitOnError(clock clock.Clock, lastUpdatedTime time.Time) (time.Duration, bool) {
	elapsedTime := clock.Since(lastUpdatedTime)
	return elapsedTime, elapsedTime <= r.maxErrDuration

}
func (r RetryHandler) GetRetryDelay(retryCount int32) time.Duration {
	timeInMillis := int(r.baseBackOffDuration.Nanoseconds() / int64(time.Millisecond))
	if timeInMillis <= 0 {
		timeInMillis = 1
	}
	maxBackoffMillis := int(r.maxBackOffMillisDuration.Nanoseconds() / int64(time.Millisecond))
	delay := 1 << uint(retryCount) * (rand.Intn(timeInMillis) + timeInMillis)
	return time.Duration(min(delay, maxBackoffMillis)) * time.Millisecond
}
func (r RetryHandler) IsTimeToRetry(clock clock.Clock, lastUpdatedTime time.Time, retryCount int32) bool {
	elapsedTime := clock.Since(lastUpdatedTime)
	return elapsedTime >= r.GetRetryDelay(retryCount)
}

func NewFlinkApplicationError(appError string, method v1beta1.FlinkMethod, errorCode string, isRetryable bool, isFailFast bool, maxRetries int32) *v1beta1.FlinkApplicationError {
	now := v1.Now()
	return &v1beta1.FlinkApplicationError{AppError: appError, Method: method, ErrorCode: errorCode, IsRetryable: isRetryable, IsFailFast: isFailFast, MaxRetries: maxRetries, LastErrorUpdateTime: &now}
}
