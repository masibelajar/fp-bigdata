# Menggunakan image dasar Spark yang sudah ditentukan
FROM bitnami/spark:3.4

# Pindah ke user root untuk bisa meng-install paket sistem
USER root

# Install curl (alat untuk mengunduh dari internet)
RUN install_packages curl

# Kembali ke user non-root default untuk keamanan
USER 1001

# Membuat direktori untuk menyimpan file JARs
RUN mkdir -p /opt/bitnami/spark/jars/

# Mengunduh JARs yang dibutuhkan langsung ke dalam direktori yang sudah dibuat
# Ini terjadi hanya sekali saat image di-build
RUN curl -L -o /opt/bitnami/spark/jars/delta-core_2.12-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar
RUN curl -L -o /opt/bitnami/spark/jars/delta-storage-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar
RUN curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar

# Menyalin dan meng-install library Python jika ada
COPY ./src/requirements.txt /opt/bitnami/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt
