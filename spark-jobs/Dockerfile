FROM apache/spark:3.5.0

USER root
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

# Command to submit the job
# We need the Spark-Kafka connector and Spark-SQL connector
# Explicitly limiting resources to fit in the small Docker worker
CMD ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--executor-memory", "512m", \
    "--total-executor-cores", "1", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
    "processor.py"]
