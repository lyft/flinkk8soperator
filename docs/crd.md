# Flink Application Custom Resource Definition
The [flinkapplication](https://github.com/lyft/flinkk8soperator/blob/master/deploy/crd.yaml) is a [kubernetes custom resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/). Once the *flinkapplication* custom resource is created in Kubernetes, the FlinkK8sOperator watches the resource and tries to move it through a series of states until the desired state is reached.

[FlinkApplication Custom Resource Example](https://github.com/lyft/flinkk8soperator/blob/master/example/example.yaml)

Below is the list of fields in the custom resource and their description

* **Spec** `type:FlinkApplicationSpec required=True`
  Contains the entire specification of the flink application.

  * **Image** `type:string required=True`
    The image name format should be registry/repository[:tag] to pull by tag, or registry/repository[@digest] to pull by digest

  * **TaskManagerConfig** `type:TaskManagerConfig required=true`
    Configuration for the Flink task manager

    * **Resources** `type:ResourceRequirements`
      Resources for the task manager. This includes cpu, memory, storage, and ephemeral-storage. If empty the operator will
      use a default value for cpu and memory.

    * **Environment** `type:EnvironmentConfig`
      Configuration for setting environment variables in the task manager.

    * **TaskSlots** `type:int32 required=true`
      Number of task slots per task manager
    
    * **OffHeapMemoryFraction** `type:float64`
      A value between 0 and 1 that represents % of container memory dedicated to system / off heap. The
      remaining memory is allocated for heap.
    
  * **JobManagerConfig** `type:JobManagerConfig`
    Configuration for the Flink job manager

    * **Resources** `type:ResourceRequirements`
      Resources for the job manager. This includes cpu, memory, storage, and ephemeral-storage. If empty the operator will
      use a default value for cpu and memory.

    * **Environment** `type:EnvironmentConfig`
      Configuration for setting environment variables in the job manager.

    * **Replicas** `type:int32 required=true`
      Number of job managers for the flink cluster. If multiple job managers are provided, the user has to ensure that
      correct environment variables are set for High availability mode.
      
    * **OffHeapMemoryFraction** `type:float64`
      A value between 0 and 1 that represents % of container memory dedicated to system / off heap. The
      remaining memory is allocated for heap.

  * **JarName** `type:string required=true`
    Name of the jar file to be run. The application image needs to ensure that the jar file is present at the right location, as
    the operator uses the Web API to submit jobs.

  * **Parallelism** `type:int32 required=true`
    Job level parallelism for the Flink Job

  * **EntryClass** `type:string`
    Entry point for the Flink job

  * **ProgramArgs** `type:string`
    External configuration parameters to be passed as arguments to the job like input and output sources, etc

  * **SavepointInfo** `type:SavepointInfo`
    Optional Savepoint info that can be passed in to indicate that the Flink job must resume from the corresponding savepoint.
  
  * **FlinkVersion** `type:string required=true`
    The version of Flink to be managed. This version must match the version in the image.

  * **DeploymentMode** `type:DeploymentMode`
    Indicates the type of deployment that operator should perform if the custom resource is updated.

    SINGLE  - This deployment mode is intended for applications where a small downtime during deployment is acceptable. The operator first deletes the existing Flink cluster and recreates a new one. Between the time the first cluster is deleted and the second cluster is started, the application is not running.

    DUAL - This deployment mode is intended for applications where downtime during deployment needs to be as minimal as possible. In this deployment mode, the operator brings up a second Flink cluster with the new image, while the original Flink cluster is still active. Once the pods and containers in the new flink cluster are ready, the Operator cancels the job in the first Cluster with savepoint, deletes the cluster and starts the job in the second cluster. (More information in the state machine section below). This mode is suitable for real time processing applications. 

  * **RestartNonce** `type:string`
    Can be set or modified to force a restart of the cluster