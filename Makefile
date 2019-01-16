SHORT_VER = 0.1.0
LONG_VER = $(shell git describe --long 2>/dev/null || echo $(SHORT_VER)-0-unknown-g`git describe --always`)
KAFKA_PATH = kafka_2.12-2.1.0
KAFKA_TAR = $(KAFKA_PATH).tgz
PYTHON_SOURCE_DIRS = karapace/
PYTHON_TEST_DIRS = tests/
ALL_PYTHON_DIRS = $(PYTHON_SOURCE_DIRS) $(PYTHON_TEST_DIRS)
GENERATED = karapace/version.py
PYTHON = python3

default: $(GENERATED)

clean:
	rm -rf rpm/

.PHONY: build-dep-fedora
build-dep-fedora:
	sudo dnf builddep karapace.spec

karapace/version.py: version.py
	$(PYTHON) $^ $@

$(KAFKA_TAR):
	wget "http://www.nic.funet.fi/pub/mirrors/apache.org/kafka/2.1.0/$(KAFKA_PATH).tgz"

$(KAFKA_PATH): $(KAFKA_TAR)
	gtar zxf "$(KAFKA_TAR)"

.PHONY: kafka
kafka: $(KAFKA_PATH)
	"$(KAFKA_PATH)/bin/zookeeper-server-start.sh" "$(KAFKA_PATH)/config/zookeeper.properties" &
	"$(KAFKA_PATH)/bin/kafka-server-start.sh" "$(KAFKA_PATH)/config/server.properties" &

.PHONY: pylint
pylint:
	python3 -m pylint --rcfile .pylintrc $(ALL_PYTHON_DIRS)

.PHONY: flake8
flake8:
	python3 -m flake8 --config .flake8 $(ALL_PYTHON_DIRS)

.PHONY: copyright
copyright:
	grep -EL "Copyright \(c\) 20.* Aiven" $(shell git ls-files "*.py" | grep -v __init__.py)

.PHONY: unittest
unittest:
	python3 -m pytest -s -vvv tests/

.PHONY: test
test: flake8 pylint copyright unittest

.PHONY: isort
isort:
	time isort --recursive $(ALL_PYTHON_DIRS)

.PHONY: yapf
yapf:
	time yapf --parallel --recursive --in-place $(ALL_PYTHON_DIRS)

.PHONY: reformat
reformat: isort yapf

.PHONY: rpm
rpm: $(GENERATED)
	git archive --output=karapace-rpm-src.tar --prefix=karapace/ HEAD
	# add generated files to the tar, they're not in git repository
	tar -r -f karapace-rpm-src.tar --transform=s,karapace/,karapace/karapace/, $(GENERATED)
	rpmbuild -bb karapace.spec \
		--define '_topdir $(PWD)/rpm' \
		--define '_sourcedir $(CURDIR)' \
		--define 'major_version $(SHORT_VER)' \
		--define 'minor_version $(subst -,.,$(subst $(SHORT_VER)-,,$(LONG_VER)))'
	$(RM) karapace-rpm-src.tar
