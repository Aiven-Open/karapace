FEDORA = $(shell source /etc/os-release && echo $$VERSION_ID)
RELEASE = 1.karapace.fc$(FEDORA)
PROJ = aiohttp-socks
PYNAME = aiohttp_socks
GITHUB_ACCOUNT = romis2012
COMMIT = 0.2.2

all: rpm

build-dep:

clean:
	rm -rf rpm/ "$(PROJ)"/

$(PROJ):
	git clone "git@github.com:$(GITHUB_ACCOUNT)/$(PROJ).git"

rpm: $(PROJ)
	$(RM) -r "$@"/ "$(PROJ)"/dist
	(cd "$(PROJ)" && git fetch && git checkout -f "$(COMMIT)")
	sed -e "s/name=.$(PYNAME)./name='python3-$(PYNAME)'/" \
	    -e "s/version=.*/version='`cd $(PROJ) && git describe --tags`',/" \
	    -i "$(PROJ)/setup.py"
	(cd "$(PROJ)" && python3 setup.py bdist_rpm \
		--release "$(RELEASE)" \
		--requires "python3-aiohttp >= 3.1.3" \
	)
	mkdir "$@"
	cp -a "$(PROJ)/dist"/*.rpm rpm/
	ls rpm/
