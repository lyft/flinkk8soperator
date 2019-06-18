FROM flink:1.8.0-scala_2.12-alpine

# Put all Flink examples on the classpath
RUN cp -r /opt/flink/examples/ /opt/flink/lib/

# Add Prometheus, Table and Queryable state jars. Add additional jars if you need
RUN mv /opt/flink/opt/flink-metrics-prometheus-1.8.0.jar /opt/flink/lib/
RUN mv /opt/flink/opt/flink-queryable-state-runtime_2.12-1.8.0.jar /opt/flink/lib/
RUN mv /opt/flink/opt/flink-table_2.12-1.8.0.jar /opt/flink/lib/

# Add Prometheus metric reporter to config
RUN echo "metrics.reporters: prom" >> "/opt/flink/conf/flink-conf.yaml"
RUN echo "metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter" >> "/opt/flink/conf/flink-conf.yaml"
RUN echo "metrics.reporter.prom.port: 9249" >> "/opt/flink/conf/flink-conf.yaml"

# Add permissions for OpenShift
RUN chmod -R 777 /opt/flink

# Update entrypoint
COPY docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh /
