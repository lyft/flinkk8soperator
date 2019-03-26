#!/usr/bin/env bash

mkdir /tmp/checkpoints
microk8s.kubectl create -f integ/test_app.yaml

function test_job() {
  curl -L -v localhost:8001/api/v1/namespaces/default/services/operator-test-app-jm:8081/proxy/jobs | grep RUNNING
}

echo "waiting for success"
test_job > /dev/null 2>&1
while [[ $? -ne 0 ]]
do
  echo -n "."
  sleep 1
  test_job > /dev/null 2>&1
done

echo "Done!"
