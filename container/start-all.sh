#!/bin/bash

ZK=$1
KAFKA=$2
REGISTRY=$3

# Configure and start the zookeeper
cat >/opt/zookeeper.properties <<- EOF
dataDir=/var/lib/zookeeper
clientPort=$ZK
maxClientCnxns=0
EOF
mkdir /var/lib/zookeeper
/opt/kafka_2.12-2.1.1/bin/zookeeper-server-start.sh /opt/zookeeper.properties >/var/log/zk.log 2>&1 &

# Configure and start the kafka server
cat >/opt/server.properties <<- EOF
broker.id=0
listeners=PLAINTEXT://localhost:$KAFKA
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/var/lib/kafka
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:$ZK
zookeeper.connection.timeout.ms=6000
group.initial.rebalance.delay.ms=0
EOF
mkdir /var/lib/kafka
/opt/kafka_2.12-2.1.1/bin/kafka-server-start.sh /opt/server.properties >/var/log/kafka.log 2>&1 &

# Optionally configure and start the schema registry
if [ -n "$REGISTRY" ]; then
    cat > /opt/schema-registry.properties <<- EOF
listeners=http://0.0.0.0:$REGISTRY
kafkastore.connection.url=localhost:$ZK
kafkastore.topic=_schemas
debug=false
EOF
    while ! grep 'Startup complete' /var/log/kafka.log; do sleep 1; done
    /usr/bin/schema-registry-start /opt/schema-registry.properties >/var/log/schema-registry.log 2>&1 &
fi

wait
