FROM bitnamilegacy/spark:3.5
USER root
RUN pip install --no-cache-dir pyspark==3.5.0 kafka-python pandas
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install psycopg2-binary pandas
WORKDIR /app
COPY . .
USER 1001