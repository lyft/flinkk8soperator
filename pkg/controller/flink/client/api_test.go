package client

import (
	"context"
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"

	"github.com/jarcoal/httpmock"
	mockScope "github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/stretchr/testify/assert"

	"strings"

	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
)

const testURL = "http://abc.com"
const invalidTestResponse = "invalid response"
const wrongEntryClassResponse = `{"errors":["Internal server error.","<Exception on server side:\norg.apache.flink.client.program.ProgramInvocationException: The program's entry point class 'com.lyft.streamingplatform.OperatorTestAppX' was not found in the jar file.\n\tat org.apache.flink.client.program.PackagedProgram.loadMainClass(PackagedProgram.java:617)\n\tat org.apache.flink.client.program.PackagedProgram.<init>(PackagedProgram.java:199)\n\tat org.apache.flink.client.program.PackagedProgram.<init>(PackagedProgram.java:149)\n\tat org.apache.flink.runtime.webmonitor.handlers.utils.JarHandlerUtils$JarHandlerContext.toJobGraph(JarHandlerUtils.java:125)\n\tat org.apache.flink.runtime.webmonitor.handlers.JarRunHandler.lambda$getJobGraphAsync$6(JarRunHandler.java:142)\n\tat java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1590)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n\tat java.lang.Thread.run(Thread.java:748)\nCaused by: java.lang.ClassNotFoundException: com.lyft.streamingplatform.OperatorTestAppX\n\tat java.net.URLClassLoader.findClass(URLClassLoader.java:382)\n\tat java.lang.ClassLoader.loadClass(ClassLoader.java:424)\n\tat java.lang.ClassLoader.loadClass(ClassLoader.java:357)\n\tat java.lang.Class.forName0(Native Method)\n\tat java.lang.Class.forName(Class.java:348)\n\tat org.apache.flink.client.program.PackagedProgram.loadMainClass(PackagedProgram.java:614)\n\t... 8 more\n\nEnd of exception on server side>"]}`
const incompatibleSavepointResponse = `{"errors":["Internal server error.","<Exception on server side:\norg.apache.flink.runtime.client.JobSubmissionException: Failed to submit job.\n\tat org.apache.flink.runtime.dispatcher.Dispatcher.lambda$internalSubmitJob$2(Dispatcher.java:309)\n\tat java.util.concurrent.CompletableFuture.uniHandle(CompletableFuture.java:822)\n\tat java.util.concurrent.CompletableFuture$UniHandle.tryFire(CompletableFuture.java:797)\n\tat java.util.concurrent.CompletableFuture$Completion.run(CompletableFuture.java:442)\n\tat akka.dispatch.TaskInvocation.run(AbstractDispatcher.scala:39)\n\tat akka.dispatch.ForkJoinExecutorConfigurator$AkkaForkJoinTask.exec(AbstractDispatcher.scala:415)\n\tat scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)\n\tat scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)\n\tat scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)\n\tat scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)\nCaused by: java.lang.RuntimeException: org.apache.flink.runtime.client.JobExecutionException: Could not set up JobManager\n\tat org.apache.flink.util.function.CheckedSupplier.lambda$unchecked$0(CheckedSupplier.java:36)\n\tat java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1590)\n\t... 6 more\nCaused by: org.apache.flink.runtime.client.JobExecutionException: Could not set up JobManager\n\tat org.apache.flink.runtime.jobmaster.JobManagerRunner.<init>(JobManagerRunner.java:152)\n\tat org.apache.flink.runtime.dispatcher.DefaultJobManagerRunnerFactory.createJobManagerRunner(DefaultJobManagerRunnerFactory.java:76)\n\tat org.apache.flink.runtime.dispatcher.Dispatcher.lambda$createJobManagerRunner$5(Dispatcher.java:351)\n\tat org.apache.flink.util.function.CheckedSupplier.lambda$unchecked$0(CheckedSupplier.java:34)\n\t... 7 more\nCaused by: java.lang.IllegalStateException: Failed to rollback to checkpoint/savepoint file:/checkpoints/flink/savepoints/savepoint-fca98f-4c1a4baeebec. Cannot map checkpoint/savepoint state for operator f3dcb3ca563a8ba134b6239a5c78c939 to the new program, because the operator is not available in the new program. If you want to allow to skip this, you can set the --allowNonRestoredState option on the CLI.\n\tat org.apache.flink.runtime.checkpoint.Checkpoints.loadAndValidateCheckpoint(Checkpoints.java:205)\n\tat org.apache.flink.runtime.checkpoint.CheckpointCoordinator.restoreSavepoint(CheckpointCoordinator.java:1103)\n\tat org.apache.flink.runtime.jobmaster.JobMaster.tryRestoreExecutionGraphFromSavepoint(JobMaster.java:1266)\n\tat org.apache.flink.runtime.jobmaster.JobMaster.createAndRestoreExecutionGraph(JobMaster.java:1190)\n\tat org.apache.flink.runtime.jobmaster.JobMaster.<init>(JobMaster.java:287)\n\tat org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory.createJobMasterService(DefaultJobMasterServiceFactory.java:83)\n\tat org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceFactory.createJobMasterService(DefaultJobMasterServiceFactory.java:37)\n\tat org.apache.flink.runtime.jobmaster.JobManagerRunner.<init>(JobManagerRunner.java:146)\n\t... 10 more\n\nEnd of exception on server side>"]}`
const fakeJobsURL = "http://abc.com/jobs"
const fakeOverviewURL = "http://abc.com/overview"
const fakeJobConfigURL = "http://abc.com/jobs/1/config"
const fakeSavepointURL = "http://abc.com/jobs/1/savepoints/2"
const fakeSubmitURL = "http://abc.com/jars/1/run"
const fakeCancelURL = "http://abc.com/jobs/1/savepoints"
const fakeTaskmanagersURL = "http://abc.com/taskmanagers"

func getTestClient() FlinkJobManagerClient {
	return FlinkJobManagerClient{}
}

func getTestJobManagerClient() FlinkAPIInterface {
	testScope := mockScope.NewTestScope()
	labeled.SetMetricKeys(common.GetValidLabelNames()...)
	return NewFlinkJobManagerClient(config.RuntimeConfig{
		MetricsScope: testScope,
	})
}

func TestGetJobsHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := GetJobsResponse{
		Jobs: []FlinkJob{
			{
				JobID: "j1",
			},
		},
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	httpmock.RegisterResponder("GET", fakeJobsURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobs(ctx, testURL)
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestGetJobsInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(200, invalidTestResponse)
	httpmock.RegisterResponder("GET", fakeJobsURL, responder)

	client := getTestJobManagerClient()
	_, err := client.GetJobs(ctx, testURL)
	assert.NotNil(t, err)
}

func TestGetJobs500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	httpmock.RegisterResponder("GET", fakeJobsURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobs(ctx, testURL)
	assert.Nil(t, resp)
	assert.EqualError(t, err, "GetJobs call failed with status 500 and message ''")
}

func TestGetJobsError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	httpmock.RegisterResponder("GET", fakeJobsURL, nil)

	client := getTestJobManagerClient()
	resp, err := client.GetJobs(ctx, testURL)
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "GetJobs call failed with status FAILED"))
}

func TestGetJobsFlinkJobUnmarshal(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	mockJobsResponse := `{"jobs":[{"id":"abc","status":"RUNNING"}]}`
	responder := httpmock.NewStringResponder(200, mockJobsResponse)
	httpmock.RegisterResponder("GET", fakeJobsURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobs(ctx, testURL)
	assert.NotNil(t, resp)
	assert.Nil(t, err)
	assert.Equal(t, resp.Jobs[0].Status, JobState("RUNNING"))
	assert.Equal(t, resp.Jobs[0].JobID, "abc")
}

func TestGetClusterOverviewHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := ClusterOverviewResponse{
		TaskManagerCount: 4,
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	httpmock.RegisterResponder("GET", fakeOverviewURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, testURL)
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestGetClusterOverviewInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(200, invalidTestResponse)
	httpmock.RegisterResponder("GET", fakeOverviewURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, testURL)
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestGetCluster500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	httpmock.RegisterResponder("GET", fakeOverviewURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, testURL)
	assert.Nil(t, resp)
	assert.EqualError(t, err, "GetClusterOverview call failed with status 500 and message ''")
}

func TestGetCluster503Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(503, nil)
	httpmock.RegisterResponder("GET", fakeOverviewURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, testURL)
	assert.Nil(t, resp)
	assert.EqualError(t, err, "GetClusterOverview call failed with status 503 and message ''")
}

func TestGetClusterOverviewError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	httpmock.RegisterResponder("GET", fakeOverviewURL, nil)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, testURL)
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "GetClusterOverview call failed with status FAILED"))
}

func TestGetJobConfigHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := JobConfigResponse{
		JobID: "j1",
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	httpmock.RegisterResponder("GET", fakeJobConfigURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobConfig(ctx, testURL, "1")
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestGetJobConfigInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(200, invalidTestResponse)
	httpmock.RegisterResponder("GET", fakeJobConfigURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobConfig(ctx, testURL, "1")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestGetJobConfig500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	httpmock.RegisterResponder("GET", fakeJobConfigURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobConfig(ctx, testURL, "1")
	assert.Nil(t, resp)
	assert.EqualError(t, err, "GetJobConfig call failed with status 500 and message ''")
}

func TestGetJobConfigError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	httpmock.RegisterResponder("GET", fakeJobConfigURL, nil)

	client := getTestJobManagerClient()
	resp, err := client.GetJobConfig(ctx, testURL, "1")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "GetJobConfig call failed with status FAILED"))
}

func TestCheckSavepointHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := SavepointResponse{
		SavepointStatus: SavepointStatusResponse{
			Status: SavePointInProgress,
		},
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	httpmock.RegisterResponder("GET", fakeSavepointURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.CheckSavepointStatus(ctx, testURL, "1", "2")
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestCheckSavepointInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(200, invalidTestResponse)
	httpmock.RegisterResponder("GET", fakeSavepointURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.CheckSavepointStatus(ctx, testURL, "1", "2")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestCheckSavepoint500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	httpmock.RegisterResponder("GET", fakeSavepointURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.CheckSavepointStatus(ctx, testURL, "1", "2")
	assert.Nil(t, resp)
	assert.EqualError(t, err, "CheckSavepointStatus call failed with status 500 and message ''")
}

func TestCheckSavepointError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	httpmock.RegisterResponder("GET", fakeSavepointURL, nil)

	client := getTestJobManagerClient()
	resp, err := client.CheckSavepointStatus(ctx, testURL, "1", "2")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "CheckSavepointStatus call failed with status FAILED"))
}

func TestSubmitJobHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := SubmitJobResponse{
		JobID: "1",
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	httpmock.RegisterResponder("POST", fakeSubmitURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, testURL, "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestSubmitJobInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(200, invalidTestResponse)
	httpmock.RegisterResponder("POST", fakeSubmitURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, testURL, "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestSubmitJob500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder := httpmock.NewStringResponder(500, "could not submit")
	httpmock.RegisterResponder("POST", fakeSubmitURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, testURL, "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Nil(t, resp)

	assert.EqualError(t, err, "SubmitJob call failed with status 500 and message 'could not submit'")
}

func TestSubmitStartupFail(t *testing.T) {
	// Tests the case where we submit a job that fails to start up we want to immediately roll back, even though
	// the Flink API unhelpfully returns a 500 in that case
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()

	responder := httpmock.NewStringResponder(500, wrongEntryClassResponse)
	httpmock.RegisterResponder("POST", fakeSubmitURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, testURL, "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Nil(t, resp)
	flinkAppError, _ := err.(*v1beta1.FlinkApplicationError)
	assert.True(t, flinkAppError.IsFailFast)

	assert.EqualError(t, err, "SubmitJob call failed with status 500 and message '"+
		wrongEntryClassResponse+"'")
}

func TestIncompatibleSavepointFail(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()

	responder := httpmock.NewStringResponder(500, incompatibleSavepointResponse)
	httpmock.RegisterResponder("POST", fakeSubmitURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, testURL, "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Nil(t, resp)
	flinkAppError, _ := err.(*v1beta1.FlinkApplicationError)
	assert.True(t, flinkAppError.IsFailFast)

	assert.EqualError(t, err, "SubmitJob call failed with status 500 and message '"+
		incompatibleSavepointResponse+"'")
}

func TestSubmitJobError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	httpmock.RegisterResponder("POST", fakeSubmitURL, nil)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, testURL, "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "SubmitJob call failed with status FAILED"))
}

func TestCancelJobHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := SavepointJobResponse{
		TriggerID: "133",
	}
	responder, _ := httpmock.NewJsonResponder(203, response)
	httpmock.RegisterResponder("POST", fakeCancelURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.CancelJobWithSavepoint(ctx, testURL, "1")
	assert.Equal(t, response.TriggerID, resp)
	assert.NoError(t, err)
}

func TestCancelJobInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(200, invalidTestResponse)
	httpmock.RegisterResponder("POST", fakeCancelURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.CancelJobWithSavepoint(ctx, testURL, "1")
	assert.Empty(t, resp)
	assert.NotNil(t, err)
}

func TestCancelJob500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	httpmock.RegisterResponder("POST", fakeCancelURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.CancelJobWithSavepoint(ctx, testURL, "1")
	assert.Empty(t, resp)
	assert.EqualError(t, err, "CancelJobWithSavepoint call failed with status 500 and message ''")
}

func TestCancelJobError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	httpmock.RegisterResponder("POST", fakeCancelURL, nil)

	client := getTestJobManagerClient()
	resp, err := client.CancelJobWithSavepoint(ctx, testURL, "1")
	assert.Empty(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "CancelJobWithSavepoint call failed with status FAILED"))
}

func TestHttpGetNon200Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := GetJobsResponse{
		Jobs: []FlinkJob{
			{
				JobID: "j1",
			},
		},
	}
	responder, _ := httpmock.NewJsonResponder(500, response)
	httpmock.RegisterResponder("GET", fakeJobsURL, responder)

	client := getTestJobManagerClient()
	_, err := client.GetJobs(ctx, testURL)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "GetJobs call failed with status 500 and message ''")
}

func TestClientInvalidMethod(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	client := getTestClient()
	_, err := client.executeRequest(context.Background(), "random", testURL, nil)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Invalid method random in request")
}

func TestGetTaskManagersValidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := TaskManagersResponse{
		TaskManagers: []TaskManagerStats{
			{
				TimeSinceLastHeartbeat: 1555611965910,
				SlotsNumber:            3,
				FreeSlots:              0,
			},
		},
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	httpmock.RegisterResponder("GET", fakeTaskmanagersURL, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetTaskManagers(ctx, testURL)
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestGetTaskManagersInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(200, invalidTestResponse)
	httpmock.RegisterResponder("GET", fakeTaskmanagersURL, responder)

	client := getTestJobManagerClient()
	_, err := client.GetJobs(ctx, testURL)
	assert.NotNil(t, err)
}
