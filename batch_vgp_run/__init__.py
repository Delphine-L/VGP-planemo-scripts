"""
VGP Planemo Scripts - Automated VGP Assembly Pipeline

This package provides tools for running VGP assembly workflows through planemo.

## Module Organization

- **utils**: General utilities (paths, URLs, data extraction)
- **logging_utils**: Logging configuration and helpers
- **galaxy_client**: Galaxy API interactions
- **workflow_manager**: Workflow download, upload, and resolution
- **workflow_prep**: YAML job file generation
- **metadata**: Profile and metadata management
- **orchestrator**: Batch processing coordination

## Main Scripts

- **prepare_single**: Unified workflow preparation tool
- **run_all**: Automated batch orchestrator
- **get_urls**: GenomeArk data URL fetcher
"""

__version__ = "1.0.0"
__author__ = "VGP Team"

# Make organized modules easily accessible
from . import utils
from . import logging_utils
from . import galaxy_client
from . import workflow_manager
from . import workflow_prep
from . import metadata
from . import orchestrator

__all__ = [
    'utils',
    'logging_utils',
    'galaxy_client',
    'workflow_manager',
    'workflow_prep',
    'metadata',
    'orchestrator',
]
