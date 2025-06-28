FROM bitnami/spark:3.4

USER root

# Install only essential packages (no ML dependencies)
RUN pip install --no-cache-dir --upgrade pip

# Install minimal requirements for ETL pipeline only
RUN pip install --no-cache-dir \
    minio==7.1.15 \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    psycopg2-binary==2.9.9

# Copy source code
COPY ./src/ /opt/bitnami/spark/src/

USER 1001