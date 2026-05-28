"""
Package setup for the Vertex ML Logs pipeline.

Required by Dataflow so that worker VMs can discover and import the
`pipeline` package after the SDK container is launched.
"""

import setuptools

setuptools.setup(
    name="vertex-ml-logs-pipeline",
    version="1.0.0",
    description="PubSub → Kafka Avro Dataflow Flex Template for Vertex ML logs",
    # setup.py lives inside the pipeline/ directory, so we must tell setuptools
    # that the `pipeline` package maps to the sdist root ("").  Without this,
    # find_packages() only discovers the sub-packages (transforms, utils) and
    # Dataflow workers fail with: ModuleNotFoundError: No module named 'pipeline'
    package_dir={"pipeline": ""},
    packages=["pipeline", "pipeline.transforms", "pipeline.utils"],
    python_requires=">=3.11",
    install_requires=[
        # Pipeline framework — [gcp] extra includes pubsub, storage, auth, grpcio
        "apache-beam[gcp]>=2.56.0",
        # Avro serialisation / Confluent wire format
        "fastavro==1.10.0",
        # Kafka producer (Google Managed Kafka, SASL_SSL + OAUTHBEARER via oauth_cb).
        # confluent-kafka ships librdkafka in its binary wheel — no separate C lib
        # install required.  Pinned to match requirements.txt.
        "confluent-kafka==2.6.1",
        # ADC token management (kafka_sink.py oauth_cb, schema_registry.py)
        "google-auth==2.39.0",
        # DLQ GCS writes (WriteToDlq DoFn)
        "google-cloud-storage>=2.16.0",
        # ReadFromPubSub source — also pulled in transitively by apache-beam[gcp]
        "google-cloud-pubsub>=2.21.1",
        # HTTP client — SchemaRegistryClient schema fetch/registration,
        # and google.auth.transport.requests.Request
        "requests>=2.31.0",
        # GCS checksum acceleration — C extension CRC32; ~50× faster than pure Python
        "crcmod>=1.7",
    ],
)
