FEDORA = $(shell source /etc/os-release && echo $$VERSION_ID)
PROJ = kafka-python
PYNAME = kafka
GITHUB_ACCOUNT = aiven
COMMIT = 2.0.1-6-g73197b2c7f62b955c9eb1ea299667b8e7db78ed4
SHORT_COMMIT = $(shell echo $(COMMIT) | cut -b 1-20)
VERSION = $(shell echo $(SHORT_COMMIT) | cut -d- -f1)
RELEASE = $(shell echo $(SHORT_COMMIT) | cut -d- -f2-).1.karapace.fc$(FEDORA)

all: rpm

build-dep:

clean:
	$(RM) -r rpm/ "$(PROJ)"/

$(PROJ):
	git clone "git@github.com:$(GITHUB_ACCOUNT)/$(PROJ).git"

rpm: $(PROJ)
	$(RM) -r "$@" "$(PROJ)/dist"
	(cd "$(PROJ)" && git fetch && git checkout -f "$(COMMIT)")
	sed -e "s/name=.$(PROJ)./name='python3-$(PYNAME)'/" \
	    -e "s/version=.*/version='$(VERSION)',/" \
	    -i "$(PROJ)/setup.py"
	(cd "$(PROJ)" && python3 setup.py bdist_rpm --release "$(RELEASE)")
	mkdir "$@"/
	cp -a "$(PROJ)/dist"/*.rpm rpm/
	find rpm/ -type f
