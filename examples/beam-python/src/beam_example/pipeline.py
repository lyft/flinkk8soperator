from __future__ import print_function

from __future__ import absolute_import

import sys
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

if __name__ == "__main__":
    # --job_endpoint argument supplied by the Flink entry point
    args = [
        "--runner=PortableRunner",
        "--streaming",
        "--sdk_worker_parallelism=2",
        "--job_name=beam-on-flinkk8soperator",
        "--environment_type=PROCESS",
        "--environment_config={\"command\": \"/opt/apache/beam/boot\"}",
    ]
    # command line options override defaults
    args.extend(sys.argv[1:])
    print("args: " + str(args))
    pipeline = beam.Pipeline(options=PipelineOptions(args))
    pcoll = (pipeline
             | beam.Create([0, 1, 2])
             | beam.Map(lambda x: x))
    result = pipeline.run()
    # streaming job does not finish
    #result.wait_until_finish()
