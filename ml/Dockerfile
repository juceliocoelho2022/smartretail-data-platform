FROM apache/spark:4.0.1-java21-python3

USER root

COPY requirements.txt /tmp/ml-requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/ml-requirements.txt

WORKDIR /opt/spark/work-dir
COPY src /opt/spark/work-dir/ml-src

USER spark
