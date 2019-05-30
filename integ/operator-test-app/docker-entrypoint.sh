#!/bin/sh

drop_privs_cmd() {
    if [ -x /sbin/su-exec ]; then
        # Alpine
        echo su-exec
    else
        # Others
        echo gosu
    fi
}


envsubst < /usr/local/flink-conf.yaml > $FLINK_HOME/conf/flink-conf.yaml

# As the taskmanager pods are accessible only by (cluster) ip address,
# we must manually configure this based on the podIp kubernetes
# variable, which is assigned to TASKMANAGER_HOSTNAME env var by the
# operator.
if [ -n "$TASKMANAGER_HOSTNAME" ]; then
    echo "taskmanager.host: $TASKMANAGER_HOSTNAME" >> "$FLINK_HOME/conf/flink-conf.yaml"
fi

# Add in extra configs set by the operator
if [ -n "$OPERATOR_FLINK_CONFIG" ]; then
    echo "$OPERATOR_FLINK_CONFIG" >> "$FLINK_HOME/conf/flink-conf.yaml"
fi

COMMAND=$@

if [ $# -lt 1 ]; then
    COMMAND="local"
fi

if [ "$COMMAND" = "help" ]; then
    echo "Usage: $(basename "$0") (jobmanager|taskmanager|local|help)"
    exit 0
elif [ "$COMMAND" = "jobmanager" ]; then
    echo "Starting Job Manager"
    echo "config file: " && grep '^[^\n#]' "$FLINK_HOME/conf/flink-conf.yaml"
    exec $(drop_privs_cmd) flink "$FLINK_HOME/bin/jobmanager.sh" start-foreground
elif [ "$COMMAND" = "taskmanager" ]; then
    echo "Starting Task Manager"
    echo "config file: " && grep '^[^\n#]' "$FLINK_HOME/conf/flink-conf.yaml"
    exec $(drop_privs_cmd) flink "$FLINK_HOME/bin/taskmanager.sh" start-foreground
elif [ "$COMMAND" = "local" ]; then
    echo "Starting local cluster"
    exec $(drop_privs_cmd) flink "$FLINK_HOME/bin/jobmanager.sh" start-foreground local
fi

exec "$@"
