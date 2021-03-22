SHORT_VER = $(shell git describe --tags --abbrev=0 | cut -f1-)
LONG_VER = $(shell git describe --long 2>/dev/null || echo $(SHORT_VER)-0-unknown-g`git describe --always`)
KAFKA_PATH = kafka_$(SCALA_VERSION)-$(KAFKA_VERSION)
KAFKA_TAR = $(KAFKA_PATH).tgz
PYTHON_SOURCE_DIRS = karapace/
PYTHON_TEST_DIRS = tests/
ALL_PYTHON_DIRS = $(PYTHON_SOURCE_DIRS) $(PYTHON_TEST_DIRS)
GENERATED = karapace/version.py
PYTHON = python3
DNF_INSTALL = sudo dnf install -y

# Keep these is sync with tests/integration/conftest.py
KAFKA_VERSION=2.7.0
SCALA_VERSION=2.13

KAFKA_IMAGE = karapace-test-kafka
ZK = 2181
KAFKA = 9092

default: $(GENERATED)

clean:
	rm -rf $(KAFKA_PATH)*

.PHONY: $(KAFKA_IMAGE)
$(KAFKA_IMAGE):
	podman build -t $(KAFKA_IMAGE) -f container/Dockerfile .

.PHONY: start-$(KAFKA_IMAGE)
start-$(KAFKA_IMAGE):
	@podman run -d --rm -p $(ZK):$(ZK) -p $(KAFKA):$(KAFKA) -p $(REGISTRY):$(REGISTRY) -p $(REST):$(REST) $(KAFKA_IMAGE) "all"
	@podman ps

karapace/version.py: version.py
	$(PYTHON) $^ $@

$(KAFKA_TAR):
	wget "https://archive.apache.org/dist/kafka/$(KAFKA_VERSION)/$(KAFKA_PATH).tgz"

$(KAFKA_PATH): $(KAFKA_TAR)
	tar zxf "$(KAFKA_TAR)"

.PHONY: fetch-kafka
fetch-kafka: $(KAFKA_PATH)

.PHONY: start-kafka
start-kafka: fetch-kafka
	$(KAFKA_PATH)/bin/zookeeper-server-start.sh $(KAFKA_PATH)/config/zookeeper.properties &
	$(KAFKA_PATH)/bin/kafka-server-start.sh $(KAFKA_PATH)/config/server.properties &

.PHONY: stop-kafka
stop-kafka:
	$(KAFKA_PATH)/bin/kafka-server-stop.sh 9 || true
	$(KAFKA_PATH)/bin/zookeeper-server-stop.sh 9 || true
	rm -rf /tmp/kafka-logs /tmp/zookeeper

.PHONY: kafka
kafka: start-kafka

.PHONY: pylint
pylint: $(GENERATED)
	pre-commit run pylint --all-files

.PHONY: flake8
flake8: $(GENERATED)
	pre-commit run flake8 --all-files

.PHONY: copyright
copyright:
	grep -EL "Copyright \(c\) 20.* Aiven" $(shell git ls-files "*.py" | grep -v __init__.py)

.PHONY: unittest
unittest: $(GENERATED)
	python3 -m pytest -s -vvv tests/unit/

.PHONY: integrationtest
integrationtest: fetch-kafka $(GENERATED)
	python3 -m pytest -s -vvv tests/integration/

.PHONY: test
test: lint copyright unittest

.PHONY: isort
isort:
	pre-commit run isort --all-files

.PHONY: yapf
yapf:
	pre-commit run yapf --all-files

.PHONY: reformat
reformat: isort yapf

.PHONY: pre-commit
pre-commit: $(GENERATED)
	pre-commit run --all-files

.PHONY: lint
lint: pre-commit
