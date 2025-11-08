# VGP Command-Line Tools

After running `bash installs.sh`, the following command-line tools are available:

## Available Commands

### vgp-run-all
**Automated pipeline orchestrator**
- Runs all VGP workflows (1, 4, 0, 8, 9, precuration) automatically
- Handles workflow dependencies and parallel execution
- Built-in URL fetching with `--fetch-urls` flag

**Examples:**
```bash
# Fetch URLs and start pipeline automatically
vgp-run-all --fetch-urls -t species_list.tsv -p profile.yaml -m ./metadata --id

# Resume a previous run
vgp-run-all --resume -t tracking_table.tsv -p profile.yaml -m ./metadata --id
```

### vgp-prepare-single
**Unified workflow preparation tool**
- Prepare any individual workflow (0, 1, 4, 8, 9, precuration)
- Built-in URL fetching with `--fetch-urls` flag
- Replaces legacy prepare_wf*.py scripts

**Examples:**
```bash
# Fetch GenomeArk URLs
vgp-prepare-single --fetch-urls -t species_list.tsv

# Prepare Workflow 1
vgp-prepare-single --workflow 1 -t tracking_table.tsv -p profile.yaml

# Prepare Workflow 8 for both haplotypes
vgp-prepare-single --workflow 8 -t tracking_table.tsv -p profile.yaml -p

# Prepare Workflow 9 with FCS mode for haplotype 1
vgp-prepare-single --workflow 9 -t tracking_table.tsv -p profile.yaml -1 -f
```

### vgp-download-reports
**Download workflow PDF reports**
- Downloads reports for completed invocations
- Reads metadata from run_all.py

**Examples:**
```bash
# Download all reports
vgp-download-reports -p profile.yaml -m ./metadata

# Skip existing reports
vgp-download-reports -p profile.yaml -m ./metadata --skip-existing

# Download for specific species
vgp-download-reports -p profile.yaml -m ./metadata --species bTaeGut2
```

## Removed Commands

### vgp-get-urls (REMOVED)
**Reason:** Functionality integrated into `vgp-run-all` and `vgp-prepare-single` via `--fetch-urls` flag

**Migration:**
```bash
# Old way
vgp-get-urls -t species_list.tsv

# New way - using vgp-prepare-single
vgp-prepare-single --fetch-urls -t species_list.tsv

# Or use vgp-run-all with --fetch-urls to fetch and run in one command
vgp-run-all --fetch-urls -t species_list.tsv -p profile.yaml -m ./metadata --id
```

**For adding species to existing table:**
```bash
# Still available as standalone script
python scripts/get_urls.py -t tracking_table.tsv --add -s Species_name -a Assembly_id
```

## Installation

```bash
# Clone repository
git clone https://github.com/Delphine-L/VGP-planemo-scripts.git
cd VGP-planemo-scripts

# Run installation script (installs dependencies + creates command-line tools)
bash installs.sh
```

This will:
1. Install Python dependencies from requirements.txt
2. Install the VGP package with `pip install -e .` (creates command-line tools)
3. Install NCBI datasets tool (required for Workflow 9)
4. Verify all installations

## Running Without Installation

If you don't want to install, you can still run scripts directly:

```bash
# Instead of: vgp-run-all
python scripts/run_all.py

# Instead of: vgp-prepare-single
python scripts/prepare_single.py

# Instead of: vgp-download-reports
python scripts/download_reports.py
```

## Benefits of Installation

1. **Convenience**: Shorter commands that work from any directory
2. **Proper imports**: Package structure ensures clean imports
3. **PATH integration**: Tools available system-wide after adding to PATH
4. **Development mode**: `-e` flag means changes to code are immediately available

## setup.py Entry Points

The command-line tools are defined in `setup.py`:

```python
entry_points={
    "console_scripts": [
        "vgp-run-all=scripts.run_all:main",
        "vgp-download-reports=scripts.download_reports:main",
        "vgp-prepare-single=scripts.prepare_single:main",
    ],
}
```

Each entry point maps a command name to a Python module and function.
