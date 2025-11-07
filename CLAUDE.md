# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

VGP-planemo-scripts is a Python-based pipeline automation system for running VGP (Vertebrate Genomes Project) assembly workflows through Galaxy using planemo. The scripts fetch genomic data from AWS GenomeArk, prepare YAML job files, and execute bioinformatics workflows for genome assembly.

**Key workflows supported:**
- VGP1: Kmer profiling with HiFi data
- VGP3/VGP4: Assembly with HiFi and Hi-C phasing
- VGP8: Haplotype-specific assembly
- VGP9: Decontamination (Kraken2 legacy or NCBI FCS-GX)
- VGP0: Mitochondrial assembly

## Architecture

### Core Components

**`batch_vgp_run/function.py`**: Shared utility functions used across all workflow preparation scripts
- `get_worfklow()`: Downloads workflows from iwc-workflows GitHub and adds version to workflow name
- `get_datasets_ids()`: Extracts dataset IDs from Galaxy invocation objects
- `fix_parameters()`: Normalizes user inputs (URLs, suffixes)
- `download_file()`: HTTP file downloader with error handling
- `find_duplicate_values()`: Dictionary value deduplication checker

**`batch_vgp_run/get_urls.py`**: Initial data discovery step
- Queries AWS GenomeArk S3 bucket using AWS CLI (`aws s3 ls --no-sign-request`)
- Detects HiC type (arima vs dovetail) and locates PacBio HiFi reads
- Generates tracking table with file paths for downstream workflows
- Supports adding new species to existing tables
- **Custom paths**: Handles species with non-standard GenomeArk directory structure via optional third column
  - Standard path: `{assembly_id}/genomic_data/`
  - Custom path: `{assembly_id}/{custom_path}/genomic_data/` (e.g., "somatic", "gametic")
- **Multiple assemblies**: Optional fourth column (suffix) enables running multiple assemblies from the same species
  - Creates unique working IDs: `{assembly_id}_{suffix}` (e.g., "kcIchGage1_somatic", "kcIchGage1_gametic")
  - Each entry gets separate directories, metadata, and Galaxy histories

**`batch_vgp_run/prepare_wf{1,3,4,8,9,0}.py`**: Workflow-specific preparation scripts
- Each script follows the same pattern:
  1. Load tracking table from previous step
  2. Connect to Galaxy instance via bioblend
  3. Fetch invocation details from previous workflow
  4. Extract dataset IDs and populate YAML template
  5. Generate planemo command lines
  6. Update tracking table with new columns
- Templates located in `batch_vgp_run/templates/`
- Uses regex pattern `\["field_name"\]` for template field replacement

**`batch_vgp_run/fetch_invocation_numbers.py`**: Recovery utility
- Retrieves missing invocation IDs from Galaxy histories
- Handles interrupted planemo runs or manual workflow fixes
- Updates tracking table columns (e.g., `Invocation_wf1`, `Invocation_wf4`)

**`batch_vgp_run/run_all.py`**: Batch execution orchestrator
- Automated pipeline execution with workflow dependency management
- Uses threading to process multiple species in parallel
- **Non-blocking design**: Launches workflows with `--no_wait` flag and exits quickly
- **Stateless and resumable**: Run periodically (e.g., hourly via cron) to check status and launch next workflows
- Checks prerequisite workflows are complete before launching dependent workflows
- Stores invocation IDs and status in metadata JSON files
- Workflow dependencies:
  - WF4 requires WF1 complete
  - WF0 requires WF4 launched (doesn't wait for completion)
  - WF8 requires WF4 complete
  - WF9 requires WF8 complete

### Workflow Execution Flow

1. **get_urls.py** → Creates `tracking_runs_{table}.tsv` with GenomeArk file paths
2. **prepare_wf1.py** → Adds `Job_File_wf1`, `Results_wf1`, `Command_wf1`, `Invocation_wf1` columns
3. Execute planemo commands manually or via shell (disconnection interrupts process!)
4. **prepare_wf4.py** → Reads `Invocation_wf1`, generates wf4 jobs, adds wf4 columns
5. **prepare_wf8.py** → Requires haplotype flag (`-1`, `-2`, `-p`, `-m`), generates haplotype-specific jobs
6. **prepare_wf9.py** → Requires NCBI datasets tool, supports legacy (`-l`) or FCS-GX (`-f`) mode

### Directory Structure Generated Per Species

```
{assembly_id}/
├── job_files/          # YAML job definitions (wf1_*.yml, wf4_*.yml, etc.)
├── invocations_json/   # Planemo output JSON files
├── reports/            # PDF reports from workflows
└── planemo_log/        # Planemo execution logs
```

### Template System

YAML templates in `batch_vgp_run/templates/` use placeholder syntax `["field_name"]`:
- `["Pacbio"]` → Replaced with HiFi reads collection YAML
- `["hic"]` → Replaced with HiC paired reads collection YAML
- `["species_name"]`, `["assembly_name"]` → Species metadata
- Other fields → Dataset IDs from previous workflow invocations

## Common Commands

**Install dependencies:**

**Recommended: Use the install script (installs Python packages + NCBI datasets tool):**
```bash
bash installs.sh
```

**Manual installation:**
```bash
# Python dependencies
pip install -r requirements.txt

# NCBI datasets tool (required for Workflow 9 - decontamination)
# macOS:
curl -o ~/.local/bin/datasets https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/mac/datasets
chmod +x ~/.local/bin/datasets

# Linux:
curl -o ~/.local/bin/datasets https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets
chmod +x ~/.local/bin/datasets

# Add to PATH if needed:
export PATH="$HOME/.local/bin:$PATH"
```

**Quick manual install (Python packages only):**
```bash
pip install awscli pandas planemo bioblend pyyaml requests
```

**Typical workflow sequence:**

1. Create initial species table (TSV with columns: Species_Name, Assembly_ID, [Custom_Path], [Suffix]):
```bash
# Standard species (2 columns)
python batch_vgp_run/get_urls.py -t species_list.tsv

# Species with non-standard GenomeArk structure (3 columns)
# Example species_list.tsv with custom paths:
# Homo_sapiens	GCA_000001405.15
# Ichthyomyzon_gagei	kcIchGage1	somatic
python batch_vgp_run/get_urls.py -t species_list_with_custom.tsv

# Multiple assemblies from same species (4 columns with suffix)
# Example: somatic and gametic assemblies from same species
# Ichthyomyzon_gagei	kcIchGage1	somatic	somatic
# Ichthyomyzon_gagei	kcIchGage1	gametic	gametic
# Creates working IDs: kcIchGage1_somatic and kcIchGage1_gametic
python batch_vgp_run/get_urls.py -t species_list_multiple.tsv
```

2. Prepare and run VGP1:
```bash
python batch_vgp_run/prepare_wf1.py -t tracking_runs_species_list.tsv \
  -g https://usegalaxy.org/ -k $GALAXY_API_KEY \
  --from_file -w ./workflows/ -v 0.5

# Execute generated commands (WARNING: disconnection interrupts!)
# Copy commands from output or tracking table column "Command_wf1"
```

3. Prepare VGP4 (after VGP1 completes):
```bash
python batch_vgp_run/prepare_wf4.py -t tracking_runs_species_list.tsv \
  -g https://usegalaxy.org/ -k $GALAXY_API_KEY \
  --from_file -w ./workflows/ -v 0.4
```

4. Prepare VGP8 for haplotype 1:
```bash
python batch_vgp_run/prepare_wf8.py -t tracking_runs_species_list.tsv \
  -g https://usegalaxy.org/ -k $GALAXY_API_KEY \
  --from_file -w ./workflows/ -v 3.1 -1
```

5. Recover missing invocation numbers (if planemo interrupted):
```bash
python batch_vgp_run/fetch_invocation_numbers.py \
  -t tracking_runs_species_list.tsv \
  -g https://usegalaxy.org/ -k $GALAXY_API_KEY
```

**Automated workflow orchestration with run_all.py:**

The `run_all.py` script automates the entire pipeline with a non-blocking, resumable design:

```bash
# Initial run to set up metadata and launch initial workflows
python batch_vgp_run/run_all.py \
  -t species_list.tsv \
  -p profile.yaml \
  -m ./metadata/ \
  --id  # or --version for workflow versions

# Resume from where it left off (run periodically, e.g., hourly via cron)
python batch_vgp_run/run_all.py \
  -t species_list.tsv \
  -p profile.yaml \
  -m ./metadata/ \
  --resume
```

**How it works:**
- First run: Creates metadata files, generates WF1 job files, checks prerequisites
- With `--resume`: Loads metadata, checks workflow statuses, launches next workflows when ready
- **Non-blocking**: Uses `--no_wait` flag, script exits quickly (minutes, not days)
- **Idempotent**: Safe to run multiple times - skips completed/running workflows
- Script checks workflow status via Galaxy API and only launches when dependencies are met
- Run periodically (e.g., `*/30 * * * * cd /path && python run_all.py --resume`) to progress pipeline

**Profile file format** (`profile.yaml`):
```yaml
Galaxy_instance: https://vgp.usegalaxy.org
Galaxy_key: your_api_key_here
Workflow_1: workflow_id_or_version  # Can mix IDs and versions
Workflow_0: workflow_id_or_version
Workflow_4: workflow_id_or_version
Workflow_8: workflow_id_or_version
Workflow_9: workflow_id_or_version
```

**Workflow ID/Version Auto-Detection:**
- The script automatically detects whether each workflow value is an ID (16-char hex) or version (e.g., "0.5")
- If a **version** is provided:
  1. Downloads workflow from GitHub
  2. Uploads to Galaxy instance
  3. Updates profile with the workflow ID
  4. Creates backup (`profile.yaml.bak`)
- If an **ID** is provided: Uses it directly
- You can mix IDs and versions in the same profile (e.g., use version for WF1, ID for WF4)

**Add species to existing tracking table:**
```bash
# Standard species
python batch_vgp_run/get_urls.py -t tracking_runs_species_list.tsv \
  --add -s Taeniopygia_guttata -a bTaeGut2

# Species with non-standard GenomeArk directory structure
python batch_vgp_run/get_urls.py -t tracking_runs_species_list.tsv \
  --add -s Ichthyomyzon_gagei -a kcIchGage1 -c somatic -x somatic

# Adding multiple entries from same species (e.g., somatic and gametic)
python batch_vgp_run/get_urls.py -t tracking_runs_species_list.tsv \
  --add -s Ichthyomyzon_gagei -a kcIchGage1 -c somatic -x somatic
python batch_vgp_run/get_urls.py -t tracking_runs_species_list.tsv \
  --add -s Ichthyomyzon_gagei -a kcIchGage1 -c gametic -x gametic
```

## Key Implementation Details

### Workflow Download Mechanism
- Workflows fetched from `https://github.com/iwc-workflows/{workflow_name}/archive/refs/tags/v{version}.zip`
- Version number added to workflow name in JSON: `"name": "{original_name} - v{version}"`
- Local caching: checks if `{workflow_dir}/{workflow_name}.ga` exists before downloading

### Custom GenomeArk Paths and Multiple Assemblies
- Some species have non-standard directory structures in GenomeArk
- Standard path: `genomeark/species/{species_name}/{assembly_id}/genomic_data/`
- Custom path: `genomeark/species/{species_name}/{assembly_id}/{custom_path}/genomic_data/`
- Examples of custom paths: `somatic`, `gametic` (for diploid/polyploid assemblies)
- Specify via optional third column in input table (Species, Assembly, Custom_Path, [Suffix])
- Empty or missing Custom_Path values default to standard path structure

**Multiple assemblies from the same species:**
- Use optional 4th column (Suffix) to distinguish multiple entries with same Species/Assembly
- Example: Running both somatic and gametic assemblies
  - Row 1: `Ichthyomyzon_gagei  kcIchGage1  somatic  somatic`
  - Row 2: `Ichthyomyzon_gagei  kcIchGage1  gametic  gametic`
- Creates working IDs: `kcIchGage1_somatic` and `kcIchGage1_gametic`
- Each entry gets separate:
  - Metadata dictionary key
  - Directory (e.g., `./kcIchGage1_somatic/`, `./kcIchGage1_gametic/`)
  - Galaxy history name
  - Job files, logs, and reports
- Original assembly ID preserved in metadata for workflow parameters

### HiC Read Pairing
- Detects R1/R2 pairing using regex `r'R1'` and `r'R2'`
- Sorts forward and reverse read lists to ensure matching order
- Generates paired collection YAML with identifiers matching filename stems

### Invocation ID Resolution
- Tries three sources in order:
  1. JSON result file from planemo (`invocation_jsons/wf*_*.json`)
  2. Tracking table `Invocation_wf*` column
  3. Manual recovery via `fetch_invocation_numbers.py`
- Error `IndexError: list index out of range` typically means missing invocation ID

### Galaxy Instance Connection
- Uses bioblend library: `GalaxyInstance(url, api_key)`
- URL normalization: prepends `https://` if missing
- Common instances: `https://usegalaxy.org/`, `https://vgp.usegalaxy.org/`

### Planemo Command Pattern
```bash
planemo run {workflow_path} {job_yaml} \
  --engine external_galaxy \
  --galaxy_url {url} \
  --simultaneous_uploads \
  --check_uploads_ok \
  --galaxy_user_key $MAINKEY \
  --history_name {assembly_id}{suffix} \
  --test_output_json {result_json} \
  > {log_file} 2>&1 &
```

## Testing Notes

- No formal test suite present
- Manual testing workflow:
  1. Use small test dataset (single species)
  2. Verify tracking table column updates after each step
  3. Check YAML file generation in `{assembly_id}/job_files/`
  4. Validate planemo commands don't error on syntax

## Known Issues and Workarounds

1. **Planemo interruption**: If terminal disconnects, invocation IDs lost
   - Solution: Use `fetch_invocation_numbers.py` or manually add to tracking table

2. **Missing HiFi reads**: Script prints warning, sets `Hifi_reads='NA'`, skips species
   - Solution: Verify species name and assembly ID on GenomeArk

3. **File extension matching**: Uses regex `r'\.f(ast)?q(sanger)?\.gz$'`
   - Handles: `.fq.gz`, `.fastq.gz`, `.fqsanger.gz`, `.fastqsanger.gz`
   - Recent fix for `.fq.gz` extension detection (commit a7fcea0)

4. **Workflow version compatibility**: Changing `-v` parameter may break if input labels changed
   - Solution: Use default versions or verify template compatibility

## File Naming Conventions

- Job files: `wf{N}_{assembly_id}_{suffix}.yml`
- Result JSONs: `wf{N}_{assembly_id}_{suffix}.json`
- Haplotype-specific: `wf8_{assembly_id}_{haplotype}.yml` (haplotype = 1, 2, p, m)
- Tracking table: `tracking_runs_{original_table_name}.tsv`
- Workflow files: `{workflow_name}.ga`
