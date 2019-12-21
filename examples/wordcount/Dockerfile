FROM flink:1.9.1-scala_2.12

# Prepare environment
ENV MAVEN_HOME=/opt/maven
ENV PATH=$MAVEN_HOME/bin:$PATH

# Install dependencies
RUN set -ex; \
  apt-get update \
  && apt-get -y install openjdk-8-jdk-headless \
  && rm -rf /var/lib/apt/lists/*

# Install Maven
ENV MAVEN_VERSION 3.6.1
RUN \
  wget https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/$MAVEN_VERSION/apache-maven-$MAVEN_VERSION-bin.tar.gz; \
  tar -zxvf apache-maven-$MAVEN_VERSION-bin.tar.gz; \
  mv apache-maven-$MAVEN_VERSION $MAVEN_HOME; \
  rm apache-maven-$MAVEN_VERSION-bin.tar.gz

# Build application jar
COPY . /code
WORKDIR /code
RUN \
  JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 mvn package \
  && ln -s /code/target $FLINK_HOME/flink-web-upload

CMD ["help"]
