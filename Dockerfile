FROM openjdk:8-jre-slim-buster

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark==3.3.0

COPY spark_job.py /app/spark_job.py

COPY dummy_data.csv /app/dummy_data.csv

COPY dummy_data2.csv /app/dummy_data2.csv

COPY test_spark_job.py /app/test_spark_job.py

RUN pip3 install delta-spark==1.0.0

CMD ["spark-submit", "--packages", "io.delta:delta-core_2.12:1.0.0", "/app/spark_job.py"]