package client

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
)

// appError codes
const (
	GlobalFailure      = "FAILED"
	JSONUnmarshalError = "JSONUNMARSHALERROR"
	DefaultRetries     = 20
	NoRetries          = 0
)

type FlinkMethod string

const (
	CancelJobWithSavepoint FlinkMethod = "CancelJobWithSavepoint"
	ForceCancelJob         FlinkMethod = "ForceCancelJob"
	SubmitJob              FlinkMethod = "SubmitJob"
	CheckSavepointStatus   FlinkMethod = "CheckSavepointStatus"
	GetJobs                FlinkMethod = "GetJobs"
	GetClusterOverview     FlinkMethod = "GetClusterOverview"
	GetLatestCheckpoint    FlinkMethod = "GetLatestCheckpoint"
	GetJobConfig           FlinkMethod = "GetJobConfig"
	GetTaskManagers        FlinkMethod = "GetTaskManagers"
	GetCheckpointCounts    FlinkMethod = "GetCheckpointCounts"
	GetJobOverview         FlinkMethod = "GetJobOverview"
)

// FlinkApplicationError implements the error interface to make error handling more structured
type FlinkApplicationError struct {
	AppError            string       `json:"appError,omitempty"`
	Method              FlinkMethod  `json:"method,omitempty"`
	ErrorCode           string       `json:"errorCode,omitempty"`
	IsRetryable         bool         `json:"isRetryable,omitempty"`
	IsFailFast          bool         `json:"isFailFast,omitempty"`
	MaxRetries          int32        `json:"maxRetries,omitempty"`
	LastErrorUpdateTime *metav1.Time `json:"lastErrorUpdateTime,omitempty"`
}

func NewFlinkApplicationError(appError string, method FlinkMethod, errorCode string, isRetryable bool, isFailFast bool, maxRetries int32) *FlinkApplicationError {
	now := metav1.Now()
	return &FlinkApplicationError{AppError: appError, Method: method, ErrorCode: errorCode, IsRetryable: isRetryable, IsFailFast: isFailFast, MaxRetries: maxRetries, LastErrorUpdateTime: &now}
}

func (f *FlinkApplicationError) Error() string {
	return f.AppError
}

func (f *FlinkApplicationError) DeepCopyInto(out *FlinkApplicationError) {
	*out = *f
	if f.LastErrorUpdateTime != nil {
		f, out := &f.LastErrorUpdateTime, &out.LastErrorUpdateTime
		*out = (*f).DeepCopy()
	}
}

func (f *FlinkApplicationError) DeepCopy() *FlinkApplicationError {
	if f == nil {
		return nil
	}
	out := new(FlinkApplicationError)
	f.DeepCopyInto(out)
	return out
}

func GetRetryableError(err error, method FlinkMethod, errorCode string, maxRetries int32, message ...string) error {
	appError := getErrorValue(err, method, errorCode, message)
	return NewFlinkApplicationError(appError.Error(), method, errorCode, true, false, maxRetries)
}

func GetNonRetryableError(err error, method FlinkMethod, errorCode string, message ...string) error {
	appError := getErrorValue(err, method, errorCode, message)
	return NewFlinkApplicationError(appError.Error(), method, errorCode, false, true, NoRetries)
}

func getErrorValue(err error, method FlinkMethod, errorCode string, message []string) error {
	if err == nil {
		return errors.New(fmt.Sprintf("%v call failed with status %v and message %v", method, errorCode, message))
	}
	return errors.Wrapf(err, "%v call failed with status %v and message %v", method, errorCode, message)
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
	maxErrWaitDuration       time.Duration
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
	flinkAppError, ok := err.(*FlinkApplicationError)
	if ok && flinkAppError != nil {
		return flinkAppError.IsRetryable
	}

	return false
}

func (r RetryHandler) IsRetryRemaining(err error, retryCount int32) bool {
	flinkAppError, ok := err.(*FlinkApplicationError)
	if ok && flinkAppError != nil {
		return retryCount <= flinkAppError.MaxRetries
	}

	return false
}

func (r RetryHandler) WaitOnError(clock clock.Clock, lastUpdatedTime time.Time) (time.Duration, bool) {
	elapsedTime := clock.Since(lastUpdatedTime)
	return elapsedTime, elapsedTime <= r.maxErrWaitDuration

}
func (r RetryHandler) GetRetryDelay(retryCount int32) time.Duration {
	timeInMillis := int(r.baseBackOffDuration.Nanoseconds() / int64(time.Millisecond))
	maxBackoffMillis := int(r.maxBackOffMillisDuration.Nanoseconds() / int64(time.Millisecond))
	delay := 1 << uint(retryCount) * (rand.Intn(timeInMillis) + timeInMillis)
	fmt.Println("Delay!!!", delay)
	return time.Duration(min(delay, maxBackoffMillis)) * time.Millisecond
}
func (r RetryHandler) IsTimeToRetry(clock clock.Clock, lastUpdatedTime time.Time, retryCount int32) bool {
	elapsedTime := clock.Since(lastUpdatedTime)
	return elapsedTime >= r.GetRetryDelay(retryCount)
}
