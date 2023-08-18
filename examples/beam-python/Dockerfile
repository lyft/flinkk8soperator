FROM flink:1.16.2-scala_2.12-java11 AS flink
FROM apachebeam/python3.6_sdk:2.17.0

# Install dependencies
RUN set -ex \
  && apt-get update \
  && apt-get -y install \
       gettext-base \
       openjdk-11-jre-headless \
       openjdk-11-jdk-headless \
  && rm -rf /var/lib/apt/lists/*

# add Flink from the official Flink image
ENV FLINK_HOME=/opt/flink
ENV PATH=$PATH:$FLINK_HOME/bin
COPY --from=flink $FLINK_HOME $FLINK_HOME

# Install the job server, this will be the Flink entry point
RUN \
  mkdir -p /opt/flink/flink-web-upload \
  && ( \
    cd /opt/flink/flink-web-upload \
    && curl -f -O https://repository.apache.org/content/groups/public/org/apache/beam/beam-runners-flink-1.16-job-server/2.49.0/beam-runners-flink-1.16-job-server-2.49.0.jar \
    && ln -s beam-runners-flink-1.16-job-server*.jar beam-runner.jar \
    ) \
  && echo 'jobmanager.web.upload.dir: /opt/flink' >> $FLINK_HOME/conf/flink-conf.yaml

# Application code - this can be moved to an s2i assemble script
COPY . /code
WORKDIR /code/src
RUN \
   pip install -r /code/src/requirements.txt

# entry point for FlinkK8sOperator Flink config
COPY docker-entrypoint.sh /

ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
CMD ["local"]
