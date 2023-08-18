FROM maven:3.9.3 as builder

COPY src /usr/src/app/src
COPY pom.xml /usr/src/app

RUN mvn -f /usr/src/app/pom.xml clean package

FROM flink:1.16.2-scala_2.12-java11

COPY --from=builder /usr/src/app/target/ /code/target
RUN ln -s /code/target $FLINK_HOME/flink-web-upload

CMD ["help"]
