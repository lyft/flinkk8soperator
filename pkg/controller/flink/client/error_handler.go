package client

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

// appError codes
const (
	globalFailure      = "FAILED"
	jsonUnmarshalError = "JSONUNMARSHALERROR"
)

// evolving map of retryable errors
var retryableErrors = map[string]struct{}{
	"GetClusterOverview500":              {},
	"GetClusterOverview503":              {},
	"GetClusterOverview" + globalFailure: {},
	"CheckSavepointStatus500":            {},
	"CheckSavepointStatus503":            {},
	"CancelJobWithSavepoint500":          {},
	"CancelJobWithSavepoint503":          {},
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
	maxRetries        int32
	baseBackOffMillis int32
}

func NewRetryHandler(maxRetries int32, baseBackoff int32) RetryHandler {
	return RetryHandler{maxRetries, baseBackoff}
}
func (r RetryHandler) IsErrorRetryable(err string, retryCount int32) bool {
	if _, ok := retryableErrors[err]; ok && retryCount <= r.maxRetries {
		return true
	}
	return false
}

func (r RetryHandler) GetRetryDelay(retryCount int32) time.Duration {
	return time.Duration(1<<uint(retryCount)*r.baseBackOffMillis) * time.Millisecond
}
func (r RetryHandler) BackOff(retryCount int32) {
	time.Sleep(r.GetRetryDelay(retryCount))
}
