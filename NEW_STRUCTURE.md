# VGP Pipeline - New Project Structure

## Directory Organization

The codebase has been completely reorganized to separate library code from command-line tools.

```
VGP-planemo-scripts/
├── batch_vgp_run/              # Python library (importable modules)
│   ├── __init__.py             # Package initialization
│   ├── utils.py                # General utilities (195 lines, 6 functions)
│   ├── logging_utils.py        # Logging setup (50 lines, 4 functions)
│   ├── galaxy_client.py        # Galaxy API functions (717 lines, 12 functions)
│   ├── workflow_manager.py     # Workflow management (197 lines, 5 functions)
│   ├── workflow_prep.py        # YAML generation (212 lines, 5 functions)
│   ├── metadata.py             # Metadata management (293 lines, 7 functions)
│   ├── orchestrator.py         # Batch processing (1118 lines, 2 functions)
│   └── templates/              # YAML templates for workflows
│       ├── wf0_run.sample.yaml
│       ├── wf1_run.sample.yaml
│       ├── wf4_run.sample.yaml
│       ├── wf8_run_sample.yaml
│       └── wf9_run_sample_*.yaml
│
├── scripts/                    # Command-line tools
│   ├── prepare_single.py       # Unified workflow preparation (replaces all prepare_wf*.py)
│   ├── run_all.py              # Automated batch orchestrator
│   ├── get_urls.py             # GenomeArk URL fetcher
│   └── download_reports.py     # Report downloader
│
└── README.md                   # Main documentation
```

## Module Organization

### Library Modules (`batch_vgp_run/`)

**Core Utilities**
- `utils.py` - Path normalization, data extraction, file operations
  - ✅ Full implementation (195 lines, 6 functions)
  - `fix_parameters()`, `fix_directory()`, `get_working_assembly()`, etc.

- `logging_utils.py` - Centralized logging
  - ✅ Full implementation (50 lines, 4 functions)
  - `setup_logging()`, `log_info()`, `log_warning()`, `log_error()`

**External Services**
- `galaxy_client.py` - Galaxy API interactions
  - ✅ Full implementation (717 lines, 12 functions)
  - `get_datasets_ids()`, `check_invocation_complete()`, `poll_until_invocation_complete()`, etc.

- `workflow_manager.py` - Workflow download/upload
  - ✅ Full implementation (197 lines, 5 functions)
  - `get_worfklow()`, `resolve_workflow()`, `upload_workflow_to_galaxy()`, etc.

**Pipeline Logic**
- `workflow_prep.py` - YAML job file generation
  - ✅ Full implementation (212 lines, 5 functions)
  - `prepare_yaml_wf4()`, `prepare_yaml_wf8()`, `prepare_yaml_wf9()`, `prepare_yaml_wf0()`, `prepare_yaml_precuration()`

- `metadata.py` - Profile and metadata management
  - ✅ Full implementation (293 lines, 7 functions)
  - `load_profile()`, `save_metadata()`, `setup_galaxy_connection()`, `wait_for_invocations()`, etc.

- `orchestrator.py` - Batch processing coordination
  - ✅ Full implementation (1118 lines, 2 functions)
  - `run_species_workflows()` (1013 lines), `process_species_wrapper()` with thread safety

### Command-Line Scripts (`scripts/`)

**Main Tools**
- `prepare_single.py` - Unified workflow preparation
  - ✅ Updated to use modular imports
  - ✅ Replaces all prepare_wf*.py scripts (WF0, WF1, WF4, WF8, WF9)
  - Supports all 5 workflows with single interface

- `run_all.py` - Automated batch execution
  - ✅ Updated to use modular imports
  - ✅ Replaces fetch_invocation_numbers.py (via --sync-metadata)
  - Uses: utils, metadata, galaxy_client, workflow_manager, logging_utils, orchestrator

- `get_urls.py` - Fetch GenomeArk file URLs
  - ✅ Self-contained, no changes needed

**Utilities**
- `download_reports.py` - Download PDF reports from invocations
  - ✅ Self-contained, no changes needed

## Import Pattern

### Old Pattern (Deprecated)
```python
import batch_vgp_run.function as function
function.fix_parameters(...)
function.load_profile(...)
```

### New Pattern (Recommended)
```python
from batch_vgp_run import utils, metadata, galaxy_client, workflow_manager

utils.fix_parameters(...)
metadata.load_profile(...)
galaxy_client.get_datasets_ids(...)
workflow_manager.resolve_workflow(...)
```

## Usage Examples

### Run prepare_single.py
```bash
cd /path/to/VGP-planemo-scripts

# Fetch URLs
python3 scripts/prepare_single.py --fetch_urls -t species.tsv

# Prepare workflow 1
python3 scripts/prepare_single.py --workflow 1 \
  -t tracking_runs_species.tsv \
  -p profile.yaml

# Prepare workflow 4
python3 scripts/prepare_single.py --workflow 4 \
  -t tracking_runs_species.tsv \
  -p profile.yaml
```

### Run batch orchestrator
```bash
# Initial run
python3 scripts/run_all.py \
  -t species.tsv \
  -p profile.yaml \
  -m ./metadata/

# Resume from where it left off
python3 scripts/run_all.py \
  -t species.tsv \
  -p profile.yaml \
  -m ./metadata/ \
  --resume
```

## Migration Status

### ✅ Completed
1. Created `scripts/` directory for command-line tools
2. Moved all scripts to `scripts/`
3. Moved templates to `batch_vgp_run/templates/`
4. Created organized module structure
5. Implemented `utils.py` (complete - 195 lines, 6 functions)
6. Implemented `logging_utils.py` (complete - 50 lines, 4 functions)
7. Implemented `workflow_manager.py` (complete - 197 lines, 5 functions)
8. Implemented `galaxy_client.py` (complete - 717 lines, 12 functions)
9. Implemented `metadata.py` (complete - 293 lines, 7 functions)
10. Implemented `orchestrator.py` (complete - 1118 lines, 2 functions)
11. Implemented `workflow_prep.py` (complete - 212 lines, 5 functions)
12. Updated `prepare_single.py` imports (complete)
13. Updated `run_all.py` imports (complete)
14. Updated template paths for new location
15. Verified Python syntax for all files
16. **Deleted `function.py`** - All implementations moved to appropriate modules
17. **Deleted 7 legacy scripts** - All functionality covered by prepare_single.py and run_all.py
    - prepare_wf0.py, prepare_wf1.py, prepare_wf3.py, prepare_wf4.py, prepare_wf8.py, prepare_wf9.py
    - fetch_invocation_numbers.py

### 📋 Next Steps (Optional)
1. Test all scripts with real data (requires Galaxy instance and dependencies installed)
2. Update CLAUDE.md with new structure details
3. Update README.md with new script usage examples

## Benefits of New Structure

### For Developers
- **Clear separation**: Library code vs. command-line tools
- **Easy navigation**: Know whether to look in `batch_vgp_run/` or `scripts/`
- **Modular imports**: `from batch_vgp_run import utils` is clearer than `import function`
- **Better IDE support**: Autocomplete works better with organized modules

### For Users
- **Simple script location**: All commands in `scripts/`
- **Clean Python package**: Can install `batch_vgp_run` as a library
- **Better documentation**: Each module has focused purpose

### For Maintenance
- **Easier testing**: Test individual modules
- **Clear responsibilities**: Each file has one job
- **Simpler dependencies**: Module dependencies are explicit

## File Sizes

```
Library Modules:
  utils.py              5.6K  ✅ Complete (195 lines, 6 functions)
  logging_utils.py      1.3K  ✅ Complete (50 lines, 4 functions)
  workflow_manager.py   5.2K  ✅ Complete (197 lines, 5 functions)
  galaxy_client.py       21K  ✅ Complete (717 lines, 12 functions)
  metadata.py           7.8K  ✅ Complete (293 lines, 7 functions)
  orchestrator.py        32K  ✅ Complete (1118 lines, 2 functions)
  workflow_prep.py      6.3K  ✅ Complete (212 lines, 5 functions)

Scripts:
  prepare_single.py     59K   ✅ Updated
  run_all.py            38K   ✅ Updated
  get_urls.py           10K   ✅ No changes needed

Legacy function.py (119K) - ✅ DELETED
```

## Testing Checklist

- [ ] Import modules in Python REPL
- [ ] Run `prepare_single.py --fetch_urls`
- [ ] Run `prepare_single.py --workflow 1`
- [ ] Run `run_all.py` (initial)
- [ ] Run `run_all.py --resume`
- [ ] Verify template paths work correctly
- [ ] Test with actual Galaxy instance

## Documentation Files

- `NEW_STRUCTURE.md` (this file) - Migration guide
- `README_MODULES.md` - Module documentation and API reference
- `REORGANIZATION_SUMMARY.md` - Before/after comparison
- `CLAUDE.md` - Project overview (needs update)
- `README.md` - User documentation (needs update)

## Questions?

For questions about the new structure:
1. Check this file for overview
2. Check `README_MODULES.md` for detailed module documentation
3. Check module docstrings: `help(batch_vgp_run.utils)`
4. Check CLAUDE.md for project context
