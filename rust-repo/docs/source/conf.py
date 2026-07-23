from __future__ import annotations

from pathlib import Path
import sys


DOCS_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = DOCS_ROOT.parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))

project = "Mooncake Rust Store 学习指南"
author = "Mooncake Contributors"
copyright = "2026, Mooncake Contributors"
version = "current"
release = "current"
language = "zh_CN"

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
]

source_suffix = {".md": "markdown"}
master_doc = "index"
exclude_patterns = ["_build", "build", "Thumbs.db", ".DS_Store"]
nitpicky = True
keep_warnings = True

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
    "substitution",
    "tasklist",
]
myst_heading_anchors = 4

templates_path: list[str] = []
html_theme = "pydata_sphinx_theme"
html_title = project
html_static_path = ["_static"]
html_css_files = ["css/custom.css"]
html_show_sourcelink = False
html_search_language = "zh"
html_theme_options = {
    "collapse_navigation": False,
    "show_nav_level": 2,
    "navigation_depth": 4,
    "secondary_sidebar_items": ["page-toc"],
    "navbar_align": "left",
}

mermaid_version = "11.4.1"
mermaid_init_js = "{startOnLoad:true,theme:'neutral',securityLevel:'strict'}"
