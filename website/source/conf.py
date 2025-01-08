"""
karapace - website configuration

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------

project = "Karapace"
copyright = "2022, Aiven"
author = "Aiven"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx_external_toc"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

# Same ones as TailwindCSS variables defined in `tailwind.config.js`.
_color_definitions = {"grey-80": "#3A3A44", "main-bg": "#f9f9ff"}

light_css_variables = {
    "color-background-primary": _color_definitions["main-bg"],
    "color-background-secondary": _color_definitions["main-bg"],
    "color-brand-primary": _color_definitions["grey-80"],
    "color-brand-content": _color_definitions["grey-80"],
}

dark_css_variables = {
    "color-brand-primary": "white",
    "color-brand-content": "white",
}

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
html_theme_options = {
    "light_css_variables": light_css_variables,
    "dark_css_variables": dark_css_variables,
    "navigation_with_keys": True,
    "sidebar_hide_name": True,
    "light_logo": "images/karapace-light-mode.svg",
    "dark_logo": "images/karapace-dark-mode.svg",
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["css/aiven.min.css"]
html_js_files = ["js/snowplow.js"]

# Additional templates that should be rendered to pages, maps page names to
# template names.
html_additional_pages = {"index": "index.html"}
