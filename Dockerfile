# Spark job runner.
#
# Spark runs in a container, not on the host. Two reasons, both of which belong in the article:
#   1. Native PySpark on Windows needs winutils.exe + HADOOP_HOME and fails on local file writes.
#   2. A reader on any OS gets the same result from a clean clone. That is the reproducibility
#      contract; a tutorial nobody can run is worth nothing.
#
# Jars are baked in at build time rather than resolved from Maven at runtime, so the first
# `spark-submit` doesn't need network and can't fail on a transient Maven outage.

FROM python:3.11-slim

ARG SPARK_VERSION=4.0.1
ARG DELTA_VERSION=4.0.0
ARG SCALA_BINARY=2.13
ARG PG_JDBC_VERSION=42.7.4

# Base image is pinned to a Debian release deliberately. Floating `python:3.11-slim` moved
# from bookworm to trixie and silently removed the Java 17 packages — exactly the kind of
# drift that makes a tutorial unreproducible six months after publication.

# Java 21, not 17: python:3.11-slim tracks Debian trixie, which dropped the openjdk-17
# packages entirely. Spark 4.0 supports both 17 and 21, so 21 is the one that still installs.
# `openjdk-21-jdk-headless` (not -jre-headless) is the name trixie actually ships.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jdk-headless curl procps \
 && rm -rf /var/lib/apt/lists/* \
 && java -version

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

RUN pip install --no-cache-dir \
      "pyspark==${SPARK_VERSION}" \
      "delta-spark==${DELTA_VERSION}" \
      "psycopg2-binary==2.9.9" \
      "pytest==8.3.4"

# Delta + Postgres JDBC jars into pyspark's own jars dir → available without --packages
RUN SPARK_JARS="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
 && curl -fsSL -o "${SPARK_JARS}/delta-spark_${SCALA_BINARY}-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_BINARY}/${DELTA_VERSION}/delta-spark_${SCALA_BINARY}-${DELTA_VERSION}.jar" \
 && curl -fsSL -o "${SPARK_JARS}/delta-storage-${DELTA_VERSION}.jar" \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
 && curl -fsSL -o "${SPARK_JARS}/postgresql-${PG_JDBC_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar" \
 && ls -la "${SPARK_JARS}" | grep -E "delta|postgresql"

WORKDIR /app
