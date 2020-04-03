GITHUB_ACCOUNT = apache
PROJ = avro
PYNAME = avro
COMMIT = bf20128ca6138a830b2ea13e0490f3df6b035639
VERSION_FILE = $(CURDIR)/$(PROJ)/version.karapace
VERSION = $$(<$(VERSION_FILE))
RELEASE = 1.karapace

all: rpm

build-dep:

clean:
	rm -rf rpm/ "$(PROJ)"/

$(PROJ):
	git clone "git@github.com:$(GITHUB_ACCOUNT)/$(PROJ).git"

rpm: $(PROJ)
	$(RM) -r "$@"/ "$(PROJ)"/lang/py3/dist
	(cd "$(PROJ)/lang/py3" && git fetch && git checkout -f "$(COMMIT)")
	sed -e "s/name = .*/name='python3-$(PYNAME)',/" \
	    -i "$(PROJ)/lang/py3/setup.py"
	mkdir -p "rpmbuild/SOURCES"
	sed -e s,-SNAPSHOT,.g$$(git -C "$(PROJ)" describe --always), "$(PROJ)/share/VERSION.txt" > $(VERSION_FILE)
	git -C "$(PROJ)" archive "--output=../rpmbuild/SOURCES/python3-$(PROJ)-$(VERSION).tar.gz" "--prefix=python3-$(PROJ)-$(VERSION)/" HEAD
	rpmbuild -bb python3-avro.spec \
		--define "_topdir $(CURDIR)/rpmbuild" \
		--define "upstream_version $$(<avro/version.karapace)" \
		--define "karapace_release $(RELEASE)"
	mkdir "$@"
	cp -a "rpmbuild/RPMS/noarch/"*.rpm "rpm/"
	ls "rpm/"
