FROM fedora:30
ARG APACHE_MIRROR=http://www.nic.funet.fi/pub/mirrors/apache.org
ARG SCALA_VERSION=2.12
ARG KAFKA_VERSION=2.4.1

RUN dnf install -y java-openjdk python3 git make
ADD requirements-dev.txt /opt/
RUN pip3 install -r /opt/requirements-dev.txt
RUN curl "$APACHE_MIRROR/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz" | tar xvzf - -C /opt

RUN curl https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -o /opt/wait-for-it.sh
ADD container/start.sh /opt
RUN chmod +x /opt/start.sh /opt/wait-for-it.sh
RUN mkdir /app
ADD . /app
WORKDIR /app
RUN make
ENTRYPOINT ["/bin/bash", "/opt/start.sh"]
