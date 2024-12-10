Karapace website
================

A static HTML site, generated with Sphinx. You can find the website source in the ``source/`` folder.

Dependencies
------------

You need Python 3.10+. Install the dependencies with ``pip``::

    pip install -r requirements.txt

For building the stylesheets, you need ``node`` and ``npm``. Install the dependencies with ``npm``::

    npm install

After that, the Tailwind CSS assets can be build with::

    make tailwind-build

or to continuously watch for changes and automatically build them::

    make tailwind-watch

Build the site::

    make html

Watch for changes and automatically build the site::

    make livehtml

Watch for all changes, this is recommended for local development when developing with style changes::

    make livehtmlall

The website can be found at: ``build/html``.
