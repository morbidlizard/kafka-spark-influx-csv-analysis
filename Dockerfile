FROM bw-sw-spark
ENV LANG=en_US.UTF-8

ADD . /sflow-analysis

WORKDIR /sflow-analysis
RUN ["pip3", "install", "-r", "requirements.txt"]

COPY ./docker-entrypoint.sh /
COPY ./log4j.properties /usr/lib/spark/conf/

ENV SPARK_CONF_DIR /usr/lib/spark/conf

RUN ["chmod", "+x", "/docker-entrypoint.sh"]
ENTRYPOINT ["/docker-entrypoint.sh"]
