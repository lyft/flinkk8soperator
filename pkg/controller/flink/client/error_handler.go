package client

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/clock"
)

// appError codes
const (
	globalFailure      = "FAILED"
	jsonUnmarshalError = "JSONUNMARSHALERROR"
	defaultRetries     = 5
)

// evolving map of retryable errors
var retryableErrors = map[string]int32{
	"GetClusterOverview500":              defaultRetries,
	"GetClusterOverview503":              defaultRetries,
	"GetClusterOverview" + globalFailure: defaultRetries,
	"CheckSavepointStatus500":            defaultRetries,
	"CheckSavepointStatus503":            defaultRetries,
	"CancelJobWithSavepoint500":          defaultRetries,
	"CancelJobWithSavepoint503":          defaultRetries,
}

// evolving map of errors where the operator fails after a single attempt
var failFastErrors = map[string]struct{}{
	"SubmitJob400":              {},
	"SubmitJob403":              {},
	"SubmitJob" + globalFailure: {},
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
		return flinkAppError.method + flinkAppError.errorCode
	}
	if error != nil {
		// For some reason the error was not a FlinkApplicationError, still return an error key
		return error.Error()
	}
	return ""
}

// A Retryer that has methods to determine if an error is retryable and also does exponential backoff
type RetryHandler struct {
	baseBackOffMillis int32
	timeToWaitMillis  int32
}

func NewRetryHandler(baseBackoff int32, timeToWait int32) RetryHandler {
	return RetryHandler{baseBackoff, timeToWait}
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
	return elapsedTime, elapsedTime <= time.Millisecond*time.Duration(r.timeToWaitMillis)

}
func (r RetryHandler) GetRetryDelay(retryCount int32) time.Duration {
	return time.Duration(1<<uint(retryCount)*r.baseBackOffMillis) * time.Millisecond
}
func (r RetryHandler) BackOff(retryCount int32) {
	time.Sleep(r.GetRetryDelay(retryCount))
}
