FROM openjdk:8-jdk

# Prepare environment
ENV FLINK_HOME=/opt/flink
ENV MAVEN_HOME=/opt/maven
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$FLINK_HOME/bin:$HADOOP_HOME/bin:$MAVEN_HOME/bin:$PATH

COPY . /code

# Configure Flink version
ENV FLINK_VERSION=1.8.0 \
    HADOOP_SCALA_VARIANT=scala_2.11

# Install dependencies
RUN set -ex; \
  apt-get update; \
  apt-get -y install libsnappy1v5; \
  apt-get -y install netcat net-tools; \
  apt-get -y install gettext-base; \
  rm -rf /var/lib/apt/lists/*

# Grab gosu for easy step-down from root
ENV GOSU_VERSION 1.7
RUN set -ex; \
  wget -nv -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
  wget -nv -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
  export GNUPGHOME="$(mktemp -d)"; \
  rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
  chmod +x /usr/local/bin/gosu; \
  gosu nobody true

# Install Maven
RUN \
  wget http://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.5.3/apache-maven-3.5.3-bin.tar.gz; \
  tar -zxvf apache-maven-3.5.3-bin.tar.gz; \
  mv apache-maven-3.5.3 $MAVEN_HOME; \
  rm apache-maven-3.5.3-bin.tar.gz

WORKDIR /code

RUN \
  mvn package; \
  mkdir -p /opt/flink/flink-web-upload; \
  cp flink-conf.yaml /usr/local/; \
  cp /code/target/operator-test-app-1.0.0-SNAPSHOT.jar /opt/flink/flink-web-upload/

RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink
WORKDIR $FLINK_HOME

ENV FLINK_URL_FILE_PATH=flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-${HADOOP_SCALA_VARIANT}.tgz
ENV FLINK_TGZ_URL=https://mirrors.ocf.berkeley.edu/apache/$FLINK_URL_FILE_PATH

# Install Flink
RUN set -ex; \
  wget -nv -O flink.tgz "$FLINK_TGZ_URL"; \
  \
  tar -xf flink.tgz --strip-components=1; \
  rm flink.tgz; \
  \
  chown -R flink:flink .;

#  control script expects manifest.yaml at this location
RUN chown -R flink:flink /var
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
CMD ["local"]
