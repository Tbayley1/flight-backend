FROM bitnamilegacy/spark:3.5
USER root
# Download the PostgreSQL JDBC driver
RUN curl https://jdbc.postgresql.org/download/postgresql-42.7.2.jar -o /opt/bitnami/spark/jars/postgresql-42.7.2.jar
WORKDIR /app
USER 1001