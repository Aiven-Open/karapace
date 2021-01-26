FROM fedora:30
ARG APACHE_MIRROR=https://archive.apache.org/dist
ARG SCALA_VERSION=2.12
ARG KAFKA_VERSION=2.4.1

RUN dnf install -y java-openjdk python3 git make gcc python3-devel
RUN groupadd -r karapace && useradd -r -g karapace karapace
RUN mkdir /app

RUN curl https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -o /opt/wait-for-it.sh
RUN curl "$APACHE_MIRROR/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz" | tar xvzf - -C /opt

ADD requirements-dev.txt /opt/
ADD . /app

RUN pip3 install -r /opt/requirements-dev.txt
RUN chmod +x /opt/start.sh /opt/wait-for-it.sh
RUN make

USER karapace
WORKDIR /app

ENTRYPOINT ["/bin/bash", "/app/container/start.sh"]
