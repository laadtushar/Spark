FROM apache/spark:3.5.0

USER root
WORKDIR /app

# Install Python dependencies required for the Spark jobs
# This ensures that both the driver and the executors have the libraries
RUN pip install --no-cache-dir redis==5.0.1 pyspark==3.5.0

# Switch back to spark user if needed, but root is often easier for local dev
USER spark
