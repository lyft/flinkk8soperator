package client

import "github.com/pkg/errors"

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
}

// FlinkApplicationError implements the error interface to make error handling more structured
type FlinkApplicationError struct {
	appError  string
	method    string
	errorCode string
}

func (f *FlinkApplicationError) Error() string {
	return f.appError
}

func IsErrorRetryable(err string) bool {
	if _, ok := retryableErrors[err]; ok {
		return true
	}
	return false
}

func GetError(err error, method string, errorCode string) error {
	var f = new(FlinkApplicationError)
	if err == nil {
		err = errors.New(method + errorCode)
	} else {
		err = errors.Wrap(err, method+errorCode)
	}
	f.errorCode = errorCode
	f.appError = err.Error()
	f.method = method
	return f
}

func GetErrorKey(error error) string {
	flinkAppError, ok := error.(*FlinkApplicationError)
	if ok && flinkAppError != nil {
		return flinkAppError.method + flinkAppError.errorCode
	}
	return ""
}
