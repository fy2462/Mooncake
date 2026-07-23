SPHINXOPTS ?= -W --keep-going
SPHINXBUILD ?= /home/fy2462/Mooncake/.venv/bin/sphinx-build
SOURCEDIR = source
BUILDDIR = build

.PHONY: help clean html serve check

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

clean:
	@$(SPHINXBUILD) -M clean "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

html: check
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

check:
	@/home/fy2462/Mooncake/.venv/bin/python tools/check_docs.py

serve: html
	@/home/fy2462/Mooncake/.venv/bin/python -m http.server 8000 -d build/html
