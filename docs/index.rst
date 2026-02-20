eumdac-fetch
============

A CLI tool for bulk downloading satellite data from EUMETSAT's Data Store.

Define download jobs in YAML, and eumdac-fetch handles parallel async downloads,
resume/retry, MD5 verification, session management, and optional post-processing.

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   getting-started
   configuration
   cli
   session
   filters
   post-processing
   state-database

.. toctree::
   :maxdepth: 2
   :caption: Architecture

   architecture

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/index
