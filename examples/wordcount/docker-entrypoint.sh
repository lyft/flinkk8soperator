#!/bin/sh

# Map config from FlinkK8sOperator to base container
# https://github.com/lyft/flinkk8soperator/issues/135
# https://github.com/docker-flink/docker-flink/pull/91
if [ -n "$OPERATOR_FLINK_CONFIG" ]; then
    export FLINK_PROPERTIES="`echo \"${OPERATOR_FLINK_CONFIG}\" | envsubst`"
fi

exec /docker-entrypoint.sh "$@"
