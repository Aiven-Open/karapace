FROM fedora:30


RUN dnf install -y java-openjdk
RUN cd /opt && curl "http://www.nic.funet.fi/pub/mirrors/apache.org/kafka/2.4.1/kafka_2.12-2.4.1.tgz" | tar xvzf -
RUN mkdir /rpm && cd /rpm && curl https://packages.confluent.io/rpm/5.0/archive.key -o confluent.key && rpm --import confluent.key
RUN cd /rpm && dnf install -y https://packages.confluent.io/rpm/5.0/confluent-common-5.0.3-1.noarch.rpm https://packages.confluent.io/rpm/5.0/confluent-rest-utils-5.0.3-1.noarch.rpm https://packages.confluent.io/rpm/5.0/confluent-schema-registry-5.0.3-1.noarch.rpm

COPY start-all.sh /opt

ENTRYPOINT ["/bin/bash", "/opt/start-all.sh"]
