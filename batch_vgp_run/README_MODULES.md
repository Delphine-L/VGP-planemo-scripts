# VGP Pipeline Module Organization

This document describes the reorganized module structure for better maintainability and clarity.

## Module Structure

The codebase has been reorganized from a single monolithic `function.py` file (2500+ lines, 41 functions) into thematic modules:

```
batch_vgp_run/
├── utils.py              # General utilities
├── logging_utils.py      # Logging setup
├── galaxy_client.py      # Galaxy API interactions
├── workflow_manager.py   # Workflow management
├── workflow_prep.py      # YAML preparation
├── metadata.py           # Metadata management
├── orchestrator.py       # Batch processing
└── function.py          # Legacy module (compatibility)
```

## Module Details

### `utils.py` - General Utilities

**Purpose**: Path normalization, data extraction, file operations

**Functions:**
- `fix_parameters(suffix, galaxy_url)` - Normalize suffix and Galaxy URL
- `fix_directory(path)` - Ensure directory paths end with `/`
- `find_duplicate_values(input_dict)` - Find duplicate values in dictionaries
- `download_file(url, save_path)` - HTTP file download with error handling
- `get_working_assembly(row, infos, index)` - Extract working assembly ID from dataframe
- `get_custom_path_for_genomeark(row, assembly_id, infos, index)` - Format GenomeArk custom paths

**Usage Example:**
```python
from batch_vgp_run import utils

# Normalize parameters
suffix, url = utils.fix_parameters("_v2", "usegalaxy.org")
# Result: suffix="_v2", url="https://usegalaxy.org"

# Extract working assembly from dataframe
for i, row in df.iterrows():
    assembly_id = utils.get_working_assembly(row, df, i)
```

### `logging_utils.py` - Logging Configuration

**Purpose**: Centralized logging setup and helper functions

**Functions:**
- `setup_logging(quiet=False)` - Configure logging level and format
- `log_info(message)` - Log informational messages (suppressed in quiet mode)
- `log_warning(message)` - Log warnings (always shown)
- `log_error(message)` - Log errors (always shown)

**Usage Example:**
```python
from batch_vgp_run import logging_utils

# Setup logging
logging_utils.setup_logging(quiet=args.quiet)

# Log messages
logging_utils.log_info("Processing species...")
logging_utils.log_warning("Invocation not found, skipping")
logging_utils.log_error("Failed to connect to Galaxy")
```

### `galaxy_client.py` - Galaxy API Interactions

**Purpose**: All Galaxy instance interactions, invocation management, status checking

**Functions:**
- `get_datasets_ids_from_json(json_path)` - Extract dataset IDs from planemo JSON
- `get_datasets_ids(invocation)` - Extract dataset IDs from invocation object
- `check_invocation_complete(gi, invocation_id)` - Check if invocation is complete
- `check_mitohifi_failure(gi, invocation_id)` - Diagnose mitochondrial workflow failures
- `check_required_outputs_exist(gi, invocation_id, required_outputs)` - Check specific outputs exist
- `get_or_find_history_id(gi, list_metadata, assembly_id, invocation_id, is_resume)` - Find or retrieve history ID
- `build_invocation_cache(gi, history_id)` - Pre-fetch invocations from history
- `fetch_invocation_from_history(gi, history_name, workflow_name, list_metadata, assembly_id, workflow_key)` - Find invocation in history
- `poll_until_invocation_complete(gi, invocation_id, timeout_seconds, check_interval)` - Poll invocation status
- `poll_until_outputs_ready(gi, invocation_id, required_outputs, timeout_seconds, check_interval)` - Poll until outputs ready
- `download_invocation_report(gi, invocation_id, output_path)` - Download PDF reports
- `batch_update_metadata_from_histories(gi, list_metadata, profile_data, suffix_run, download_reports)` - Batch update metadata

**Usage Example:**
```python
from batch_vgp_run import galaxy_client
from bioblend.galaxy import GalaxyInstance

gi = GalaxyInstance(url, api_key)

# Check invocation status
is_complete, status = galaxy_client.check_invocation_complete(gi, invocation_id)

# Extract dataset IDs
invocation = gi.invocations.show_invocation(invocation_id)
datasets = galaxy_client.get_datasets_ids(invocation)
```

### `workflow_manager.py` - Workflow Management

**Purpose**: Workflow download, version resolution, upload to Galaxy

**Functions:**
- `get_workflow_version(workflow_name, version)` - Get workflow metadata from GitHub
- `get_worfklow(workflow_name, version, workflow_dir)` - Download workflow from iwc-workflows (note: typo preserved for compatibility)
- `is_workflow_id(value)` - Detect if string is Galaxy workflow ID (16-char hex)
- `upload_workflow_to_galaxy(gi, workflow_path)` - Upload workflow file to Galaxy
- `resolve_workflow(gi, workflow_spec, workflow_name, workflow_dir)` - Auto-detect and resolve workflow ID/version

**Usage Example:**
```python
from batch_vgp_run import workflow_manager

# Auto-resolve workflow (version or ID)
workflow_id, version, path = workflow_manager.resolve_workflow(
    gi,
    "0.5",  # Could be version "0.5" or ID "abc123def456"
    "kmer-profiling-hifi-VGP1",
    "./workflows/"
)
```

### `workflow_prep.py` - YAML Job File Preparation

**Purpose**: Generate YAML job files for each workflow type

**Functions:**
- `prepare_yaml_wf4(assembly_id, list_metadata, profile_data)` - Workflow 4 (Assembly + HiC phasing)
- `prepare_yaml_wf8(assembly_id, list_metadata, invocation_wf4, profile_data, haplotype_code)` - Workflow 8 (Haplotype scaffolding)
- `prepare_yaml_wf0(assembly_id, list_metadata, profile_data)` - Workflow 0 (Mitochondrial assembly)
- `prepare_yaml_wf9(assembly_id, list_metadata, invocation_wf8, profile_data, haplotype_code, fcs_mode)` - Workflow 9 (Decontamination)
- `prepare_yaml_precuration(assembly_id, list_metadata, profile_data)` - Pre-curation (PretextMap)

**Usage Example:**
```python
from batch_vgp_run import workflow_prep

# Prepare Workflow 4 job file
workflow_prep.prepare_yaml_wf4(
    "mHomSap1",
    metadata,
    profile_data
)
# Creates: ./mHomSap1/job_files/mHomSap1_Workflow_4.yml
```

### `metadata.py` - Metadata Management

**Purpose**: Profile loading, metadata persistence, connection setup

**Functions:**
- `load_profile(profile_path)` - Load and validate profile YAML file
- `setup_galaxy_connection(profile_data)` - Setup Galaxy connection from profile
- `load_metadata(metadata_dir, suffix_run)` - Load metadata from JSON files
- `save_metadata(metadata_dir, list_metadata, suffix_run, dico_workflows)` - Save metadata to JSON
- `save_species_metadata(species_id, species_metadata, metadata_dir, suffix_run)` - Save per-species metadata
- `mark_invocation_as_failed(list_metadata, assembly_id, workflow_key, invocation_id, reason)` - Track failed invocations
- `wait_for_invocations(gi, invocations_to_wait, timeout_seconds)` - Wait for multiple invocations

**Usage Example:**
```python
from batch_vgp_run import metadata

# Load profile and connect to Galaxy
profile = metadata.load_profile("profile.yaml")
gi, galaxy_url = metadata.setup_galaxy_connection(profile)

# Load/save metadata
list_metadata, workflows = metadata.load_metadata("./metadata/", "_run1")
metadata.save_metadata("./metadata/", list_metadata, "_run1", workflows)
```

### `orchestrator.py` - Batch Processing Coordination

**Purpose**: Parallel species processing, workflow dependency management

**Functions:**
- `run_species_workflows(gi, assembly_id, list_metadata, profile_data, dico_workflows, is_resume)` - Run workflows for one species
- `process_species_wrapper(assembly_id, list_metadata, profile_data, dico_workflows, results_lock, results_status, is_resume)` - Thread-safe wrapper for parallel processing

**Usage Example:**
```python
from batch_vgp_run import orchestrator
from concurrent.futures import ThreadPoolExecutor

# Process multiple species in parallel
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(
            orchestrator.process_species_wrapper,
            species_id,
            metadata,
            profile,
            workflows,
            lock,
            results,
            is_resume=True
        )
        for species_id in species_list
    ]
```

## Migration Guide

### For Developers

The new organization is **fully backward compatible**. Existing code using `batch_vgp_run.function` will continue to work without changes.

**Old way (still works):**
```python
import batch_vgp_run.function as function
function.fix_parameters(suffix, url)
function.load_profile(profile_path)
```

**New recommended way:**
```python
from batch_vgp_run import utils, metadata
utils.fix_parameters(suffix, url)
metadata.load_profile(profile_path)
```

### Gradual Migration

You can migrate gradually:

1. **Phase 1**: Use new modules for new code
2. **Phase 2**: Update imports in existing files when modifying them
3. **Phase 3**: Eventually deprecate `function.py` (optional)

## Benefits

1. **Discoverability**: Clear module names indicate purpose
2. **Maintainability**: Smaller files, focused responsibilities
3. **Testing**: Easier to test individual modules
4. **Documentation**: Module-level docs explain purpose
5. **Collaboration**: Multiple developers can work on different modules
6. **Import clarity**: `from batch_vgp_run import utils` vs `import batch_vgp_run.function`

## Implementation Notes

### Current Structure

The reorganization uses a **facade pattern**:
- New modules (utils.py, galaxy_client.py, etc.) import from `function.py`
- This provides organizational structure without code duplication
- Full backward compatibility maintained

### Future Improvements (Optional)

Functions can be gradually moved from `function.py` into their respective modules:

1. Move function implementation to new module
2. In `function.py`, import from new module and re-export
3. Eventually, `function.py` becomes a simple compatibility shim

**Example:**
```python
# In function.py (compatibility layer)
from batch_vgp_run.utils import fix_parameters, fix_directory
from batch_vgp_run.galaxy_client import get_datasets_ids
# ... etc
```

## Quick Reference

| Task | Module | Key Functions |
|------|--------|---------------|
| Normalize paths/URLs | `utils` | `fix_parameters`, `fix_directory` |
| Extract dataframe data | `utils` | `get_working_assembly`, `get_custom_path_for_genomeark` |
| Setup logging | `logging_utils` | `setup_logging`, `log_*` |
| Check invocation status | `galaxy_client` | `check_invocation_complete`, `poll_until_*` |
| Extract dataset IDs | `galaxy_client` | `get_datasets_ids` |
| Download workflows | `workflow_manager` | `get_worfklow`, `resolve_workflow` |
| Prepare job files | `workflow_prep` | `prepare_yaml_wf*` |
| Load profile/metadata | `metadata` | `load_profile`, `load_metadata` |
| Process multiple species | `orchestrator` | `process_species_wrapper` |

## Questions?

For questions about the new organization or migration help, please refer to:
- Module docstrings: `help(batch_vgp_run.utils)`
- CLAUDE.md: Overall project documentation
- This file: Organization and migration guide
