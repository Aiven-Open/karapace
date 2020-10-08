FEDORA = $(shell source /etc/os-release && echo $$VERSION_ID)
PROJ = aiokafka
PYNAME = aiokafka
GITHUB_ACCOUNT = aio-libs
COMMIT = v0.6.0-0-g39d734a1613adf0dba1cf54886f394cd728c62e6
VERSION = $(COMMIT)
RELEASE = 0.aiven.fc$(FEDORA)

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
	(cd "$(PROJ)" && python3 setup.py bdist_rpm --release "$(RELEASE)" --force-arch=noarch)
	mkdir "$@"/
	cp -a "$(PROJ)/dist"/*.rpm rpm/
	find rpm/ -type f
