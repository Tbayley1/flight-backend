FROM bitnamilegacy/spark:3.5
USER root
RUN pip install --no-cache-dir pyspark==3.5.0 kafka-python pandas
WORKDIR /app
COPY . .
USER 1001