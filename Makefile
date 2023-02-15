PYTHON = python3

default: karapace/version.py

clean:
	# remove all the versions of kafka
	rm -rf kafka_*
	# delete cache files
	find . -iname '*.pyc' -delete
	find . -iname '__pycache__' -delete
	# delete packaging directories
	rm -rf dist karapace.egg-info
	# delete generate files
	rm karapace/version.py

karapace/version.py: version.py
	$(PYTHON) $^ $@

.PHONY: copyright
copyright:
	grep -EL "Copyright \(c\) 20.* Aiven" $(shell git ls-files "*.py" | grep -v __init__.py)
