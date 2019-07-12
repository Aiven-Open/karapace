FEDORA = $(shell source /etc/os-release && echo $$VERSION_ID)
RELEASE = 1.karapace.fc$(FEDORA)
PROJ = aiosocksy
PYNAME = aiosocksy
GITHUB_ACCOUNT = romis2012
COMMIT = b3c3e5f067341f7b9ef1d3e1a61f9e5bf28a7df1

all: rpm

build-dep:

clean:
	rm -rf rpm/ "$(PROJ)"/

$(PROJ):
	git clone "git@github.com:$(GITHUB_ACCOUNT)/$(PROJ).git"
	git -C aiosocksy/ tag -a 0.0.0 -m dummyatag 812082f13c880164613fb0aefbf687aeb999623a

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
