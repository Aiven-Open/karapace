FROM fedora:31 AS karapace

# python3: required by karapace
# git: required to install dependencies from git
# gcc, python3: required to install multidict, a dependency of aiohttp
RUN dnf install -y python3 git gcc python3-devel
RUN groupadd --system karapace && useradd --system --gid karapace karapace
RUN mkdir /opt/karapace /var/log/karapace && chown karapace:karapace /opt/karapace /var/log/karapace

# Files are added selectively to improve cacheability. The dir `.git` is
# intentionally avoided since pulling from a remote would invalidate the cache.

# Add the requirements and install the dependencies
ADD ./requirements-dev.txt /app/
RUN pip3 install --requirement /app/requirements-dev.txt

# Add the files needed to install karapace
ADD ./setup.py ./README.rst ./version.py /app/
ADD ./karapace/ /app/karapace/
RUN pip3 install /app

# Add shell script needed to run the container
ADD ./container/start.sh /app/container/
RUN chown --recursive karapace:karapace /app/

WORKDIR /app
USER karapace

FROM karapace AS karapace_registry
ENTRYPOINT ["/bin/bash", "/app/container/start.sh", "registry"]

FROM karapace AS karapace_rest
ENTRYPOINT ["/bin/bash", "/app/container/start.sh", "rest"]
