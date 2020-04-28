FROM fedora:30
ARG APACHE_MIRROR=http://www.nic.funet.fi/pub/mirrors/apache.org
ARG SCALA_VERSION=2.12
ARG KAFKA_VERSION=2.4.1
ARG CONFLUENT_MAJOR_VERSION=5
ARG CONFLUENT_MINOR_VERSION=5
ARG CONFLUENT_PATCH_VERSION=0
ARG CONFLUENT_RPM_REPO=https://packages.confluent.io/rpm

RUN dnf install -y java-openjdk
RUN dnf install -y python3
RUN dnf install -y wget
RUN dnf install -y git
ADD requirements-dev.txt /opt/
RUN pip3 install -r /opt/requirements-dev.txt
RUN curl "$APACHE_MIRROR/kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz" | tar xvzf - -C /opt
RUN mkdir /rpm && cd /rpm \
  && curl "${CONFLUENT_RPM_REPO}/${CONFLUENT_MAJOR_VERSION}.${CONFLUENT_MINOR_VERSION}/archive.key" -o /tmp/confluent.key \
  && echo "${CONFLUENT_RPM_REPO}/${CONFLUENT_MAJOR_VERSION}.${CONFLUENT_MINOR_VERSION}/archive.key" \
  && rpm --import /tmp/confluent.key && rm /tmp/confluent.key \
  && dnf install -y \
    $CONFLUENT_RPM_REPO/$CONFLUENT_MAJOR_VERSION.$CONFLUENT_MINOR_VERSION/confluent-common-$CONFLUENT_MAJOR_VERSION.$CONFLUENT_MINOR_VERSION.${CONFLUENT_PATCH_VERSION}-1.noarch.rpm \
    $CONFLUENT_RPM_REPO/$CONFLUENT_MAJOR_VERSION.$CONFLUENT_MINOR_VERSION/confluent-rest-utils-$CONFLUENT_MAJOR_VERSION.$CONFLUENT_MINOR_VERSION.${CONFLUENT_PATCH_VERSION}-1.noarch.rpm \
    $CONFLUENT_RPM_REPO/$CONFLUENT_MAJOR_VERSION.$CONFLUENT_MINOR_VERSION/confluent-schema-registry-$CONFLUENT_MAJOR_VERSION.$CONFLUENT_MINOR_VERSION.${CONFLUENT_PATCH_VERSION}-1.noarch.rpm \
    $CONFLUENT_RPM_REPO/$CONFLUENT_MAJOR_VERSION.$CONFLUENT_MINOR_VERSION/confluent-kafka-rest-$CONFLUENT_MAJOR_VERSION.$CONFLUENT_MINOR_VERSION.${CONFLUENT_PATCH_VERSION}-1.noarch.rpm

RUN wget https://raw.githubusercontent.com/vishnubob/wait-for-it/master/wait-for-it.sh -O /opt/wait-for-it.sh
ADD container/start.sh /opt
RUN chmod +x /opt/start.sh /opt/wait-for-it.sh
RUN mkdir /app
ADD . /app
WORKDIR /app
ENTRYPOINT ["/bin/bash", "/opt/start.sh"]
