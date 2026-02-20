"""Sphinx configuration for eumdac-fetch documentation."""

import sys
from pathlib import Path

# Make the package importable without a pip install (needed for Read the Docs).
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

project = "eumdac-fetch"
copyright = "2026, digital-idiot"
author = "digital-idiot"
release = "1.0.0"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
    "sphinx_click",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = ["_static"]

# MyST settings
myst_enable_extensions = [
    "colon_fence",
    "deflist",
]
myst_fence_as_directive = ["mermaid"]

# Autodoc settings
autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_mock_imports = [
    "eumdac",
    "aiohttp",
    "fsspec",
    "rich",
    "yaml",
    "click",
    "requests",
]

# Napoleon settings
napoleon_google_style = True
napoleon_numpy_style = False

# Mermaid settings
mermaid_version = "11"
