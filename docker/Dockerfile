FROM ubuntu:16.04

ENV DEBIAN_FRONTEND noninteractive
ENV LANG=en_US.UTF-8

RUN apt-get update && \
apt-get upgrade -y && \
apt-get install -q -y openjdk-8-jdk libsnappy-dev python3-pip

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.5 1

RUN apt-get --assume-yes install language-pack-en

RUN python -m pip install --upgrade pip requests

RUN apt-get --assume-yes install wget pandoc && \\
python -m pip install nose pypandoc && \\
python -m pip install pandas && \\
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.1.0-bin-hadoop2.7.tgz && \\
tar -xvf spark-2.1.0-bin-hadoop2.7.tgz && \\
cd spark-2.1.0-bin-hadoop2.7/python/ && \\
python setup.py install && \\
apt-get clean

CMD /bin/bash
