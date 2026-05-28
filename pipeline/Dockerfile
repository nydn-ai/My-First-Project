FROM gcr.io/dataflow-templates-base/python311-template-launcher-base

ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="vertex-ml-logs-pubsub-to-kafka-avro" \
      org.opencontainers.image.description="Vertex ML Logs Dataflow Flex Template: PubSub → Kafka Avro" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.created="${BUILD_DATE}"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY pipeline/ /app/pipeline/

WORKDIR /app
ENV PYTHONPATH="/app"

RUN pip install --no-cache-dir --no-deps ./pipeline

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/pipeline/main.py"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="/app/pipeline/setup.py"

RUN python - <<'PY'
import pipeline.main
from pipeline.transforms.kafka_sink import WriteToKafkaAvro
print("Import health check OK")
PY

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
