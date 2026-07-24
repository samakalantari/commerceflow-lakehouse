ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

USER root

COPY requirements.txt /tmp/jupyter-requirements.txt

RUN /opt/bitnami/python/bin/python3 -m pip install \
      --no-cache-dir \
      -r /tmp/jupyter-requirements.txt \
    && mkdir -p /home/jupyter/notebooks \
    && chown -R 1001:0 /home/jupyter

USER 1001

ENV HOME=/home/jupyter \
    PYSPARK_PYTHON=/opt/bitnami/python/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python3

WORKDIR /home/jupyter/notebooks

CMD ["/opt/bitnami/python/bin/python3", "-m", "jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.root_dir=/home/jupyter/notebooks"]
