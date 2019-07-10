#!/bin/sh

drop_privs_cmd() {
    if [ $(id -u) != 0 ]; then
        # Don't need to drop privs if EUID != 0
        return
    elif [ -x /sbin/su-exec ]; then
        # Alpine
        echo su-exec
    else
        # Others
        echo gosu flink
    fi
}

envsubst < /usr/local/flink-conf.yaml > $FLINK_HOME/conf/flink-conf.yaml

if [ "$FLINK_DEPLOYMENT_TYPE" = "jobmanager" ]; then
    echo "jobmanager.rpc.address: $HOST_NAME" >> "$FLINK_HOME/conf/flink-conf.yaml"
fi

# As the taskmanager pods are accessible only by (cluster) ip address,
# we must manually configure this based on the podIp kubernetes
if [ "$FLINK_DEPLOYMENT_TYPE" = "taskmanager" ]; then
    echo "taskmanager.host: $HOST_IP" >> "$FLINK_HOME/conf/flink-conf.yaml"
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
elif [ "$FLINK_DEPLOYMENT_TYPE" = "jobmanager" ]; then
    echo "Starting Job Manager"
    echo "config file: " && grep '^[^\n#]' "$FLINK_HOME/conf/flink-conf.yaml"
    exec $(drop_privs_cmd) "$FLINK_HOME/bin/jobmanager.sh" start-foreground
elif [ "$FLINK_DEPLOYMENT_TYPE" = "taskmanager" ]; then
    echo "Starting Task Manager"
    echo "config file: " && grep '^[^\n#]' "$FLINK_HOME/conf/flink-conf.yaml"
    exec $(drop_privs_cmd) "$FLINK_HOME/bin/taskmanager.sh" start-foreground
elif [ "$COMMAND" = "local" ]; then
    echo "Starting local cluster"
    exec $(drop_privs_cmd) "$FLINK_HOME/bin/jobmanager.sh" start-foreground local
fi

exec "$@"
