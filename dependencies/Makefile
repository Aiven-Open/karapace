
LOCAL_PACKAGES = \
	python3-aiohttp_socks \
	python3-avro \
	python3-kafka

DEP_DIRS = $(foreach dir,$(LOCAL_PACKAGES),$(dir)/rpm)
DEP_CLEAN = $(foreach dir,$(LOCAL_PACKAGES),clean-$(dir))

all: install

%/rpm: %/Makefile
	mkdir -p rpm/
	$(MAKE) -C "$*" clean
	@if git -C "$*" ls-files . --exclude-standard --others | grep .; then \
	    echo "Untracked files in $* after make clean"; \
	    false; \
	fi
	$(MAKE) -C "$*" build-dep rpm
	cp "$@"/*.rpm rpm/
	touch --reference "$*" "$@"  # prevents rebuild on the next run

clean: clean-builds clean-rpms

clean-rpms:
	rm -rvf rpm/

clean-builds: $(DEP_CLEAN)

clean-%:
	$(MAKE) -C "$*" clean

install: clean $(DEP_DIRS)
	 $(foreach file, rpm/*.noarch.rpm, sudo dnf install -y $(file))

# This trick makes it possible to call "make python3-avro"
.SECONDEXPANSION:
$(LOCAL_PACKAGES): $$@/rpm

.PHONY: all clean clean-rpms clean-builds clean-% install
