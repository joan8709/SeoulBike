FROM apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu
WORKDIR /opt/spark-app
COPY job_curate_to_local.py .
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/opt/spark-app/job_curate_to_local.py"]
