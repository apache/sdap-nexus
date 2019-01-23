# NEXUS

The next generation cloud-based science data service platform. More information can be found here http://incubator-sdap-nexus.readthedocs.io/en/latest/index.html


## Building the Docs

Ensure sphinx, sphinx-autobuild, and recommonmark are installed. We use the recommonmark module for parsing Markdown files.

    pip install sphinx sphinx-autobuild recommonmark

Run sphinx-autobuild to view the docs locally.

    cd docs
    sphinx-autobuild . _build/html
