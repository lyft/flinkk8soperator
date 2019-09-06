package client

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func getTestRetryer() RetryHandler {
	return NewRetryHandler(10*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond)
}

func TestGetError(t *testing.T) {
	testErr := errors.New("Service unavailable")
	ferr := GetNonRetryableError(testErr, "GetTest", "500")
	assert.Equal(t, "GetTest call failed with status 500 and message '': Service unavailable", ferr.Error())

	//nil error
	ferrNil := GetNonRetryableError(nil, "GetTest", "500")
	assert.Equal(t, "GetTest call failed with status 500 and message ''", ferrNil.Error())

	testWrappedErr := errors.Wrap(testErr, "Wrapped errors")
	ferrWrapped := GetNonRetryableError(testWrappedErr, "GetTestWrapped", "400")
	assert.Equal(t, "GetTestWrapped call failed with status 400 and message '': Wrapped errors: Service unavailable", ferrWrapped.Error())

	testMessageErr := errors.New("Test Error")
	ferrMessage := GetNonRetryableErrorWithMessage(testMessageErr, "GetTest", "500", "message")
	assert.Equal(t, "GetTest call failed with status 500 and message 'message': Test Error", ferrMessage.Error())
}

func TestErrors(t *testing.T) {
	retryableError := errors.New("GetClusterOverview500")
	ferr := GetRetryableError(retryableError, "GetTest", "500", DefaultRetries)
	retryer := getTestRetryer()
	assert.True(t, retryer.IsErrorRetryable(ferr))

	failFastError := errors.New("SubmitJob400BadRequest")
	ferr = GetNonRetryableError(failFastError, "GetTest", "500")
	assert.False(t, retryer.IsErrorRetryable(ferr))
}

func TestRetryHandler_GetRetryDelay(t *testing.T) {
	retryHandler := getTestRetryer()
	assert.True(t, retryHandler.GetRetryDelay(20) <= 50*time.Millisecond)
	assert.True(t, retryHandler.GetRetryDelay(1) <= 50*time.Millisecond)
	assert.True(t, retryHandler.GetRetryDelay(200) <= 50*time.Millisecond)
}

func TestRetryHandler_IsRetryRemaining(t *testing.T) {
	retryableError := errors.New("GetClusterOverview500")
	ferr := GetRetryableError(retryableError, "GetTest", "500", DefaultRetries)
	retryer := getTestRetryer()
	assert.True(t, retryer.IsRetryRemaining(ferr, 2))
	assert.False(t, retryer.IsRetryRemaining(ferr, 22))
}

func TestRetryHandler_IsTimeToRetry(t *testing.T) {
	retryer := getTestRetryer()
	currTime := metav1.NewTime(time.Now())
	olderTime := currTime.Add(-5 * time.Second)
	fakeClock := clock.NewFakeClock(currTime.Time)
	fakeClock.SetTime(time.Now())
	// Set retry count to 0 to keep retry delay small
	assert.True(t, retryer.IsTimeToRetry(fakeClock, olderTime, 0))
}
