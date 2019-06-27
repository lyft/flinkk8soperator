package client

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/clock"
)

// appError codes
const (
	GlobalFailure      = "FAILED"
	JSONUnmarshalError = "JSONUNMARSHALERROR"
	defaultRetries     = 20
)

// evolving map of retryable errors
var retryableErrors = map[string]int32{
	"GetClusterOverview500":              defaultRetries,
	"GetClusterOverview503":              defaultRetries,
	"GetClusterOverview404NotFound":      defaultRetries,
	"GetClusterOverview" + GlobalFailure: defaultRetries,
	"CheckSavepointStatus500":            defaultRetries,
	"CheckSavepointStatus503":            defaultRetries,
	"CancelJobWithSavepoint500":          defaultRetries,
	"CancelJobWithSavepoint503":          defaultRetries,
}

// evolving map of errors where the operator fails after a single attempt
var failFastErrors = map[string]struct{}{
	"SubmitJob400BadRequest":    {},
	"SubmitJob" + GlobalFailure: {},
}

// FlinkApplicationError implements the error interface to make error handling more structured
type FlinkApplicationError struct {
	appError  error
	method    string
	errorCode string
}

func (f *FlinkApplicationError) Error() string {
	return f.appError.Error()
}

func GetError(err error, method string, errorCode string, message ...string) error {
	var f = new(FlinkApplicationError)
	if err == nil {
		err = errors.New(fmt.Sprintf("%v call failed with status %v and message %v", method, errorCode, message))
	} else {
		err = errors.Wrapf(err, "%v call failed with status %v and message %v", method, errorCode, message)
	}

	f.errorCode = errorCode
	f.appError = err
	f.method = method

	return f
}

func GetErrorKey(error error) string {
	flinkAppError, ok := error.(*FlinkApplicationError)
	if ok && flinkAppError != nil {
		errorKey := flinkAppError.method + flinkAppError.errorCode
		return strings.Replace(errorKey, " ", "", -1)
	}
	if error != nil {
		// For some reason the error was not a FlinkApplicationError, still return an error key
		return error.Error()
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type RetryHandlerInterface interface {
	IsErrorRetryable(err string) bool
	IsRetryRemaining(err string, retryCount int32) bool
	IsErrorFailFast(err string) bool
	WaitOnError(clock clock.Clock, lastUpdatedTime time.Time) (time.Duration, bool)
	GetRetryDelay(retryCount int32) time.Duration
	BackOff(retryCount int32)
}

// A Retryer that has methods to determine if an error is retryable and also does exponential backoff
type RetryHandler struct {
	baseBackOffDuration      time.Duration
	maxErrWaitDuration       time.Duration
	maxBackOffMillisDuration time.Duration
}

func NewRetryHandler(baseBackoff time.Duration, timeToWait time.Duration, maxBackOff time.Duration) RetryHandler {
	rand.Seed(time.Now().UnixNano())
	return RetryHandler{baseBackoff, timeToWait, maxBackOff}
}
func (r RetryHandler) IsErrorRetryable(err string) bool {
	if _, ok := retryableErrors[err]; ok {
		return true
	}
	return false
}

func (r RetryHandler) IsRetryRemaining(err string, retryCount int32) bool {
	if maxRetries, ok := retryableErrors[err]; ok {
		return retryCount <= maxRetries
	}

	return false
}

func (r RetryHandler) IsErrorFailFast(err string) bool {
	if _, ok := failFastErrors[err]; ok {
		return true
	}
	return false
}

func (r RetryHandler) WaitOnError(clock clock.Clock, lastUpdatedTime time.Time) (time.Duration, bool) {
	elapsedTime := clock.Since(lastUpdatedTime)
	return elapsedTime, elapsedTime <= r.maxErrWaitDuration

}
func (r RetryHandler) GetRetryDelay(retryCount int32) time.Duration {
	timeInMillis := int(r.baseBackOffDuration.Nanoseconds() / int64(time.Millisecond))
	delay := 1 << uint(retryCount) * (rand.Intn(timeInMillis) + timeInMillis)
	return time.Duration(min(delay, int(r.maxBackOffMillisDuration))) * time.Millisecond
}
func (r RetryHandler) BackOff(retryCount int32) {
	time.Sleep(r.GetRetryDelay(retryCount))
}
