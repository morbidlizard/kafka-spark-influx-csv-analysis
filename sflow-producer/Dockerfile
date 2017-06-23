FROM confluentinc/cp-kafka

RUN apt-get update && apt-get install build-essential -y \
	&& wget http://www.inmon.com/bin/sflowtool-3.22.tar.gz \
        && tar -xvzf sflowtool-3.22.tar.gz && cd sflowtool-3.22 \
	&& ./configure && make && make install

RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN pip3 install kafka

COPY ./entrypoint.sh .
COPY ./producer.py .

RUN chmod +x entrypoint.sh

ENTRYPOINT "./entrypoint.sh"
