FROM apache/spark:3.5.3

USER root

# Add Hadoop AWS and AWS SDK jars for S3/MinIO connectivity
ADD --chmod=644 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD --chmod=644 https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# Copy startup scripts
COPY --chmod=755 start-master.sh /opt/spark/start-master.sh
COPY --chmod=755 start-worker.sh /opt/spark/start-worker.sh

# Create work directory
RUN mkdir -p /opt/spark/work && chown -R spark:spark /opt/spark/work

USER spark
