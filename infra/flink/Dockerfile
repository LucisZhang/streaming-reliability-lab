ARG FLINK_IMAGE=flink:1.20.4-scala_2.12-java11
FROM ${FLINK_IMAGE}

# The official Flink 1.20.4 image already ships the metrics-prometheus plugin.
COPY conf/flink-conf.yaml /opt/flink/conf/flink-conf.yaml

USER root
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints \
    && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints
USER flink
