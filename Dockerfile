FROM bw-sw-spark
ENV LANG=en_US.UTF-8

ADD . /sflow-analysis

WORKDIR /sflow-analysis
RUN ["pip3", "install", "-r", "requirements.txt"]

COPY ./docker-entrypoint.sh /

RUN ["chmod", "+x", "/docker-entrypoint.sh"]
ENTRYPOINT ["/docker-entrypoint.sh"]
