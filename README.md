# VGP Planemo Scripts

Automated tools to run VGP assembly pipelines through planemo. Scripts designed to import data from the GenomeArk AWS repository and orchestrate Galaxy workflow execution.

**Note**: Pre-Curation workflow (PretextMap generation) is now integrated into the automated pipeline - see "Pre-Curation Workflow" section below.

**Trio data**: Not yet supported.

## Installation

### Install from PyPI (Recommended)

````bash
pip install vgp-planemo-scripts
````

### Install from source

````bash
# Clone the repository
git clone https://github.com/Delphine-L/VGP-planemo-scripts.git
cd VGP-planemo-scripts

# Install in development mode
pip install -e .

# Or install normally
pip install .
````

### Additional Requirements

**NCBI datasets command-line tool** (required for Workflow 9 with FCS decontamination):

````bash
# Install following instructions at:
# https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/download-and-install/
````

**AWS CLI** (included with pip installation, required for `--fetch-urls` option):

When installing via pip, AWS CLI is installed automatically. If running from source without pip, install it manually:

````bash
pip install awscli
````

### Verify Installation

After installation, verify the tools are available:

````bash
# Check main pipeline tool
vgp-run-all --help

# Check utility tools
vgp-get-urls --help
vgp-download-reports --help

# Verify AWS CLI (for --fetch-urls option)
aws --version
````

## Command-Line Tools

After installation, the following commands are available:

**Main automated pipeline:**
- `vgp-run-all` - Run all VGP workflows automatically (replaces `python batch_vgp_run/run_all.py`)

**Utility tools:**
- `vgp-get-urls` - Get GenomeArk file URLs for species (replaces `python batch_vgp_run/get_urls.py`)
- `vgp-download-reports` - Download workflow reports (replaces `python batch_vgp_run/download_reports.py`)
- `vgp-fetch-invocations` - Fetch invocation numbers from Galaxy history

**Manual workflow preparation:**
- `vgp-prepare-wf0` - Prepare Workflow 0 (mitogenome)
- `vgp-prepare-wf1` - Prepare Workflow 1 (kmer profiling)
- `vgp-prepare-wf3` - Prepare Workflow 3 (decontamination before phasing)
- `vgp-prepare-wf4` - Prepare Workflow 4 (assembly + phasing)
- `vgp-prepare-wf8` - Prepare Workflow 8 (scaffolding)
- `vgp-prepare-wf9` - Prepare Workflow 9 (decontamination)

## First step - Prepare a file with the species informations

Create a tabulated file with the following columns:

1. Species Name (no space, underscores) (e.g. Taeniopygia_guttata)
2. Assembly ID (e.g. bTaeGut2)

Usage:

````bash
# If installed via pip
vgp-get-urls -t <Table with Species and Assembly ID>

# Or if running from source
python batch_vgp_run/get_urls.py -t <Table with Species and Assembly ID>
````

### Output:

A tabular file containing the names of PacBio, Arima, and Bionano files on Genomark

e.g.

````tabular
Taeniopygia_guttata	bTaeGut2	m54306U_210519_154448.hifi_reads.fastq.gz m54306U_210521_004211.hifi_reads.fastq.gz m54306Ue_210629_211205.hifi_reads.fastq.gz m54306Ue_210719_083927.hifi_reads.fastq.gz m64055e_210624_223222.hifi_reads.fastq.gz	bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L1_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L2_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L3_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L4_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L5_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L6_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L7_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L8_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMMCCXY_L6_R1.fq.gz	bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L1_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L2_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L3_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L4_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L5_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L6_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L7_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L8_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMMCCXY_L6_R2.fq.gz	bTaeGut2_Saphyr_DLE1_3172351.cmap
````

### Add a species to the generated table

Usage:

````bash
# If installed via pip
vgp-get-urls -t <Table with Species and Assembly ID> --add -s <Species Name> -a <Species ID>

# Or if running from source
python batch_vgp_run/get_urls.py -t <Table with Species and Assembly ID> --add -s <Species Name> -a <Species ID>
````

## Automated Pipeline - Run All Workflows (run_all.py)

**Recommended approach**: `run_all.py` automates the entire VGP assembly pipeline (Workflows 1, 4, 0, 8, 9) for multiple species with minimal user intervention.

### Features

- **Fully automated**: Runs all workflows in the correct dependency order
- **Automatic polling**: Continuously monitors workflow completion status and automatically launches the next workflow (configurable polling intervals)
- **Automatic URL fetching**: Optional `--fetch-urls` flag to automatically fetch GenomeArk file paths
- **Parallel processing**: Process multiple species concurrently (default: 3 species)
- **Smart resuming**: Automatically recovers invocation data from Galaxy history if JSON files are missing
- **Incremental metadata saving**: Per-species metadata files preserve progress at each workflow checkpoint
- **Failed invocation detection**: Automatically detects and reports failed workflows on resume
- **Automatic retry**: Optional `--retry-failed` flag to re-launch failed workflows
- **Hi-C trimming auto-detection**: Automatically configures trimming based on Hi-C technology (Arima/Dovetail)
- **Non-blocking execution**: Uses `--no_wait` flag to avoid long-running terminal sessions
- **Stateless design**: Can be safely interrupted and resumed (ideal for cron jobs)
- **API optimized**: Minimizes Galaxy API calls (~80% reduction vs manual approach)

### Workflow Execution Order

For each species, workflows run in this order:
1. **Workflow 1** (kmer-profiling): Generates k-mer profile and QC data
2. **Workflow 4** (assembly-phasing): Generates hap1 and hap2 assemblies (waits for WF1 completion)
3. **Workflow 0** (mitogenome): Assembles mitochondrial genome (runs after WF4 launch, doesn't wait)
4. **Workflow 8** (scaffolding): Scaffolds both haplotypes with Hi-C (waits for WF4 completion, runs in parallel)
5. **Workflow 9** (decontamination): Decontaminates both haplotypes (waits for WF8 completion, runs in parallel)
6. **Pre-Curation** (optional): Generates Pretext contact maps for manual curation (waits for WF4 and WF9 completion)

### Setup: Create a Profile File

Create a YAML profile file (e.g., `profile.yaml`) with your configuration. See `batch_vgp_run/templates/profile.sample.yaml` for a template.

**Example profile.yaml:**

````yaml
Galaxy_instance: https://vgp.usegalaxy.org/
Galaxy_key: your_api_key_here

# Workflow IDs (find these in your Galaxy account)
Workflow_1: abc123def456  # kmer-profiling-hifi-VGP1
Workflow_0: ghi789jkl012  # Mitogenome-assembly-VGP0
Workflow_4: mno345pqr678  # Assembly-Hifi-HiC-phasing-VGP4
Workflow_8: stu901vwx234  # Scaffolding-HiC-VGP8
Workflow_9: yza567bcd890  # Assembly-decontamination-VGP9

# Optional: Pre-Curation Workflow (generates Pretext maps for manual curation)
# Workflow_PreCuration: efg123hij456  # PretextMap-Generation

# Optional: Workflow 9 decontamination method
wf9_version: fcs  # Use 'fcs' (default, NCBI FCS-GX) or 'legacy' (Kraken2)

# Optional: Polling intervals for workflow completion (in minutes)
# How often to check if workflows have completed before launching the next workflow
# poll_interval_wf1: 30     # Workflow 1 polling interval (default: 30 minutes)
# poll_interval_other: 60   # Other workflows polling interval (default: 60 minutes)
````

**Note**: When using `wf9_version: fcs`, taxon IDs are automatically queried from NCBI dataset for each species. The `ncbi dataset` command-line tool must be installed (see https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/download-and-install/).

### Usage

**Recommended: Use existing workflows in Galaxy** (--id):

````bash
# If installed via pip
vgp-run-all \
  -t <Table with file paths> \
  -p <Profile YAML file> \
  -m <Metadata directory> \
  --id \
  [-s <Suffix>] \
  [-c <Concurrent processes>]

# Or if running from source
python batch_vgp_run/run_all.py -t <Table> -p <Profile> -m <Metadata dir> --id
````

**Alternative: Use workflow versions** (--version, downloads workflows automatically):

````bash
# If installed via pip
vgp-run-all \
  -t <Table with file paths> \
  -p <Profile YAML file> \
  -m <Metadata directory> \
  --version \
  [-s <Suffix>] \
  [-c <Concurrent processes>]

# Or if running from source
python batch_vgp_run/run_all.py -t <Table> -p <Profile> -m <Metadata dir> --version
````

**Automatic URL fetching** (--fetch-urls, fetches GenomeArk file paths automatically):

Use this option when your input table only contains Species and Assembly columns (2 columns, no headers). The script will automatically fetch file URLs from GenomeArk.

````bash
# If installed via pip
vgp-run-all \
  -t <Simple species table> \
  -p <Profile YAML file> \
  -m <Metadata directory> \
  --fetch-urls \
  --id \
  [-s <Suffix>] \
  [-c <Concurrent processes>]

# Example with a simple 2-column input file:
# species_list.tsv contents:
#   Taeniopygia_guttata	bTaeGut2
#   Corvus_moneduloides	bCorMon1

vgp-run-all -t species_list.tsv -p profile.yaml -m ./metadata --fetch-urls --id

# Output:
# ============================================================
# Fetching GenomeArk file URLs...
# ============================================================
# Fetching URLs for bTaeGut2 (Taeniopygia_guttata)...
#   ✓ Found arima Hi-C data
# Fetching URLs for bCorMon1 (Corvus_moneduloides)...
#   ✓ Found dovetail Hi-C data
#
# ✓ GenomeArk URLs saved to: tracking_runs_species_list.tsv
# ============================================================
````

**Note**: The `--fetch-urls` option requires AWS CLI (included with pip installation). It cannot be used with `--resume`.

**Resume a previous run**:

````bash
# If installed via pip
vgp-run-all \
  -t <Table with file paths> \
  -p <Profile YAML file> \
  -m <Metadata directory> \
  --resume \
  [--version | --id] \
  [--retry-failed]

# Or if running from source
python batch_vgp_run/run_all.py -t <Table> -p <Profile> -m <Metadata dir> --resume --id
````

### Parameters

- **-t, --table**: Table with species information. Can be:
  - Full tracking table with file paths (output from `get_urls.py`)
  - Simple 2-column table (Species, Assembly) when using `--fetch-urls`
- **-p, --profile**: Path to profile YAML file with Galaxy credentials and workflow IDs/versions
- **-m, --metadata_directory**: Directory to store run metadata (default: `./`)
- **--id**: Use workflow IDs from profile (workflows must exist in your Galaxy account) - **Recommended**
- **--version**: Use workflow versions from profile (downloads workflows automatically)
- **--fetch-urls**: Automatically fetch GenomeArk file paths (AWS CLI included with installation, input table must be 2 columns only)
- **-s, --suffix**: Optional suffix for this run (e.g., `v2.0`)
- **-c, --concurrent**: Number of species to process in parallel (default: 3)
- **--resume**: Resume a previous run using saved metadata
- **--retry-failed**: When used with `--resume`, automatically retry failed or cancelled invocations

### Output Files

**Metadata directory** (`<metadata_directory>/`):
- `metadata_run<suffix>.json`: Complete run metadata (invocations, file paths, history IDs)
- `metadata_workflow<suffix>.json`: Workflow paths and versions
- `results_run<suffix>.json`: Final status for each species
- `metadata_<assembly_id>_run<suffix>.json`: Per-species metadata (temporary, deleted after successful completion)

**Per species** (`<assembly_id>/`):
- `job_files/`: YAML job files for each workflow
- `invocations_json/`: Planemo invocation JSON files
- `planemo_log/`: Planemo execution logs
- `reports/`: Workflow PDF reports (when available)

### Resuming Runs

The `--resume` flag allows you to continue a previous run:

- **Recovers lost data**: Automatically searches Galaxy history for missing invocations
- **Incremental recovery**: Loads per-species metadata files to recover progress from partial runs
- **Failed invocation detection**: Checks all stored invocations and reports any that failed or were cancelled
- **Automatic retry**: Use `--retry-failed` to automatically re-launch failed workflows
- **Skips completed work**: Doesn't re-run workflows that already have invocations
- **Waits for prerequisites**: Only runs workflows when dependencies are complete
- **Safe to run repeatedly**: Idempotent design - can be run in a cron job

**When to use `--resume`:**
- Terminal session was interrupted
- Script crashed mid-run (per-species metadata preserves progress)
- Workflow JSON files were deleted but workflows succeeded in Galaxy
- Checking on long-running workflows
- Recovering from failed workflows (combine with `--retry-failed`)
- Running as a periodic check (e.g., hourly cron job)

**Incremental Metadata Saving:**

The script automatically saves per-species metadata files (`metadata_<assembly_id>_run<suffix>.json`) at key checkpoints:
- After Workflow 1 completes
- After Workflow 4 completes
- After Workflow 0 is launched
- After Workflow 8 completes (both haplotypes)
- After Workflow 9 completes (both haplotypes)

These files are merged into the global metadata file when each species completes successfully, then deleted. If a run is interrupted, `--resume` will load these per-species files to recover the most recent state.

**Handling Failed Invocations:**

When using `--resume`, the script automatically checks the status of all stored invocations:

````bash
# Resume and check for failures (warning only)
vgp-run-all -t table.tsv -p profile.yaml -m ./metadata --resume --id

# Output if failures found:
============================================================
⚠  WARNING: Found failed/cancelled invocations:
============================================================
  - bTaeGut2 Workflow_4: failed (invocation: abc123...)
  - klBraLanc5 Workflow_8_hap1: cancelled (invocation: def456...)
============================================================
These workflows will be skipped unless you re-run them manually
or remove their invocation IDs from metadata.
Use --retry-failed flag to automatically retry failed invocations.
````

To automatically retry failed workflows:

````bash
# Resume and retry all failed invocations
vgp-run-all -t table.tsv -p profile.yaml -m ./metadata --resume --id --retry-failed

# Output:
============================================================
🔄 Found failed/cancelled invocations - will retry:
============================================================
  - bTaeGut2 Workflow_4: failed (invocation: abc123...)
  - klBraLanc5 Workflow_8_hap1: cancelled (invocation: def456...)
============================================================
Resetting failed invocations to allow retry...
  Reset bTaeGut2 Workflow_4
  Reset klBraLanc5 Workflow_8_hap1

Failed workflows will be re-launched during this run.
````

### Example Complete Workflow

````bash
# 1. Get species data URLs
vgp-get-urls -t species_list.tsv

# Output: species_list.tsv with file paths

# 2. Create profile.yaml with Galaxy credentials and workflow IDs
cat > profile.yaml <<EOF
Galaxy_instance: https://vgp.usegalaxy.org/
Galaxy_key: YOUR_API_KEY
Workflow_1: abc123
Workflow_0: def456
Workflow_4: ghi789
Workflow_8: jkl012
Workflow_9: mno345
wf9_version: fcs
EOF

# 3. Start the automated pipeline (processes 5 species in parallel)
vgp-run-all \
  -t species_list.tsv \
  -p profile.yaml \
  -m ./metadata \
  --id \
  -c 5

# 4. Check progress later (safe to run multiple times)
vgp-run-all \
  -t species_list.tsv \
  -p profile.yaml \
  -m ./metadata \
  --resume \
  --id

# 5. View results
cat metadata/results_run.json
````

### Advantages vs Manual Workflow Execution

- **Time savings**: No need to monitor workflows and manually trigger next steps
- **Resource efficiency**: Script exits quickly with `--no_wait` (minutes vs days)
- **Error recovery**: Automatically handles missing invocation files via Galaxy history search
- **Parallel execution**: Process multiple species simultaneously
- **Reproducibility**: All parameters saved in profile and metadata files
- **Resume-friendly**: Can be interrupted and resumed without data loss

---

## Download Workflow Reports (download_reports.py)

After workflows complete, you can download PDF reports for all finished invocations using the `download_reports.py` script. This script reads metadata from `run_all.py` and downloads reports only for invocations with state='ok'.

### Usage

**Download all reports:**

````bash
# If installed via pip
vgp-download-reports -p <Profile YAML file> -m <Metadata directory> [-s <Suffix>]

# Or if running from source
python batch_vgp_run/download_reports.py -p <Profile> -m <Metadata dir>
````

**Skip reports that already exist:**

````bash
vgp-download-reports -p <Profile YAML file> -m <Metadata directory> --skip-existing
````

**Download reports for a specific species:**

````bash
vgp-download-reports -p <Profile YAML file> -m <Metadata directory> --species <Assembly ID>
````

### Parameters

- **-p, --profile**: Path to profile YAML file (same as used with `run_all.py`)
- **-m, --metadata_directory**: Directory containing metadata files (default: `./`)
- **-s, --suffix**: Optional suffix used in your run (e.g., `v2.0`)
- **--skip-existing**: Skip downloading reports that already exist on disk
- **--species**: Only download reports for a specific species (assembly ID)

### Features

- **Automatic state checking**: Only downloads reports for completed invocations (state='ok')
- **Progress tracking**: Shows detailed progress and summary statistics
- **Error handling**: Continues processing if individual downloads fail
- **Metadata integration**: Uses paths from `metadata_run.json` (same as `run_all.py`)
- **All workflows**: Downloads reports for WF1, WF4, WF0, WF8 (both haplotypes), WF9 (both haplotypes)

### Output

Reports are saved to the paths specified in the metadata file:
- `<assembly_id>/reports/<assembly_id>_Workflow_<N>_report.pdf`

### Example

````bash
# After running run_all.py, download all reports
vgp-download-reports -p profile.yaml -m ./metadata

# Output:
============================================================
Processing bTaeGut2
============================================================
  Workflow_1: Downloading report...
  Workflow_1: ✓ Report saved to ./bTaeGut2/reports/bTaeGut2_Workflow_1_report.pdf
  Workflow_4: Downloading report...
  Workflow_4: ✓ Report saved to ./bTaeGut2/reports/bTaeGut2_Workflow_4_report.pdf
  Workflow_8_hap1: Invocation incomplete (state: scheduled) - skipping

============================================================
Download Summary
============================================================
Total invocations checked: 14
Reports downloaded: 8
Skipped (already exists): 0
Skipped (incomplete): 4
Skipped (no invocation): 2
Errors: 0
============================================================
````

---

## Pre-Curation Workflow (Optional)

The Pre-Curation workflow generates Pretext contact maps for manual curation of genome assemblies. This optional workflow is now integrated into the automated pipeline.

### What It Does

The Pre-Curation workflow creates Pretext maps that allow curators to:
- Visualize Hi-C contact patterns in the assembled genomes
- Identify misjoins, inversions, and other assembly errors
- Make manual corrections before final assembly release

### Inputs

The workflow uses outputs from previously completed workflows:
- **HiFi reads**: "HiFi reads without adapters" from Workflow 4
- **Hi-C reads**: "Trimmed Hi-C reads" from Workflow 4
- **Haplotype 1**: "Final Decontaminated Assembly" from Workflow 9 haplotype 1
- **Haplotype 2**: "Final Decontaminated Assembly" from Workflow 9 haplotype 2

### Configuration

To enable Pre-Curation, add it to your profile.yaml:

````yaml
# Add this line to enable pre-curation (find workflow ID in your Galaxy account)
Workflow_PreCuration: your_workflow_id_here  # PretextMap-Generation
````

**Notes:**
- Pre-Curation is **optional** - if not specified in the profile, it will be skipped
- Automatically runs after Workflow 4 and Workflow 9 complete for both haplotypes
- Uses already-processed data from previous workflows:
  - **Hi-C reads**: Already trimmed by Workflow 4 (trimming disabled in Pre-Curation)
  - **HiFi reads**: Already adapter-removed by Workflow 4
  - **Haplotypes**: Already decontaminated by Workflow 9
- Haplotype suffixes are automatically set to "H1" and "H2"

### Example

````bash
# Enable pre-curation in your profile.yaml
echo "Workflow_PreCuration: abc123def456" >> profile.yaml

# Run the pipeline as normal - pre-curation will run automatically
vgp-run-all -t species_table.tsv -p profile.yaml -m ./metadata --id

# Output shows pre-curation execution:
--- Pre-Curation workflow ---
Launching pre-curation workflow for bTaeGut2...
Pre-curation workflow launched: inv_abc123xyz789
````

### Outputs

Pre-Curation generates Pretext maps (.pretext files) that can be opened in Pretext software for manual curation.

---

## Manual Workflow Execution

The following sections describe how to run workflows individually. This approach gives more control but requires manual coordination of workflow dependencies.

## Prepare files for Workflow 1

General Inputs:

1. The name of the table with the paths to the data (output of the previous step) **(-t)**.
2. The target Galaxy instance (e.g. `https://usegalaxy.org/`) **(-g)**.
3. The API key for the selected galaxy instance **(-k)**.
4. Optional: a suffix for the analysis **(-s)** (e.g. `2.0` will produce files called `wf1_$S{assembly_ID}_2.0.yaml`).

Upload a new workflow to Galaxy **(--from_file)**:

1. The directory containing the workflows **(-w)**. If the directory doesn't exist, it will be created. If the workflow file isn't in the directory, the workflow will be downloaded.
2. Optional: Specify a workflow version to use, Default is 0.5 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

Use a workflow existing in Galaxy **(--from_id)**:

1. The Galaxy ID of workflow VGP1 **(-i)**.

Usage:

````bash
  python <path to scripts>/batch_vgp_run/prepare_wf1.py -t <Table with file paths> -g <Galaxy url> -k <API Key>  
          [--from_file -w <Workflow Directory>  -v <Workflow version> ] 
          [--from_id -i <Workflow ID>] 
      -s <Suffix> 
````

### Output

For each Species:

- A Yaml File containing the input paths and the job parameters named `wf1_${assembly_ID}.yml` in the folder `${assembly_ID}/job_files`

For all:

- A table named `wf_run_${Input_table}` containing the input table plus columns listing : 
  - The yaml file to use for running workflow 1
  - The json file that will contain the results of the workflow 1 run
  - The command line to paste on your shell to run workflow 1 on the select Galaxy instance. Set or replace `$MAINKEY` variable with your Galaxy API Key.

### Run Workflow 1

- To change a parameter for one species, modify the file `${assembly_ID}/job_files/wf1_${assembly_ID}.yml`. To change the parameters for all jobs, modify the file `wf1_run.sample.yaml` before runing  `prepare_wf1.py`
- Use the generated command line to upload the data and run the workflow.

>> WARNING: Disconnecting your terminal before the command finish will interrupt the process!

## Prepare files for workflow 3 or 4

> Note: If the json file with the invocation details is missing or in error, but the invocation is successful in Galaxy, add the invocation number to the generated table in the column `Invocation_wf1` or run `fetch_invocation_numbers.py`. This can happen if:
>
>- The planemo command for wf1 has been interrupted,
>- The Workflow had errors but you fixed it in the interface
>
> Try this if you get an error `IndexError: list index out of range` when running `prepare_wf4.py` or `prepare_wf3.py`

General inputs:

1. The table named `wf_run_$Input_table` generated by `prepare_wf1.py` **(-t)**
2. The target Galaxy instance (e.g. `https://usegalaxy.org/`)  **(-g)**
3. The API key for your Galaxy instance  **(-k)**
4. Optional: a suffix for the analysis **(-s)** (e.g. `2.0` will produce files called `wf4_${assembly_ID}_2.0.yaml` )

Upload a new workflow to Galaxy **(--from_file)**:

1. The directory containing the workflows **(-w)**. If the directory doesn't exist, it will be created. If the workflow file isn't in the directory, the workflow will be downloaded.
2. Optional: Specify a workflow version to use, Default is 0.4 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

Use a workflow existing in Galaxy **(--from_id)**:

1. The Galaxy ID of workflow VGP3 or VGP4 **(-i)**.

Usage workflow 4:

````bash
  python <path to scripts>/batch_vgp_run/prepare_wf4.py  -t  <Tracking table>  -g <Galaxy Instance> -k <API Key> 
          [--from_file -w <Workflow Directory>  -v <Workflow version> ] 
          [--from_id -i <Workflow ID>]
      -s <Optional suffix>
````

Usage workflow 3:

````bash
  python <path to scripts>/batch_vgp_run/prepare_wf3.py  -t  <Tracking table> -g <Galaxy Instance> -k <API Key>  [--from_file -w <Workflow Directory>  -v <Workflow version> ] [--from_id -i <Workflow ID>] -s <Optional suffix>
````

> Note: You can run this step even if some of your invocations are not finished. The script will skip lines with incomplete invocations and lines that have already been processed. If you want to re-generate the files and command for a species, delete the file `${assembly_ID}/job_files/w4_${assembly_ID}.yaml`.

To change the parameters of all jobs, modify the file `w4_run.sample.yaml`

**Hi-C Read Trimming:** The script automatically configures Hi-C read trimming based on the Hi-C technology type from your input table:
- **Arima Hi-C**: Trimming enabled (`Trim Hi-C reads?: true`)
- **Dovetail Hi-C**: Trimming disabled (`Trim Hi-C reads?: false`)

This ensures optimal preprocessing for each Hi-C technology.

### Outputs

For each Species:

- A Yaml File containing the input paths and the job parameters named `${assembly_ID}/job_files/wf[3/4]_${assembly_ID}.yml` in the folder `job_files`

For all:

- The updated Tracking table named containing the previous data plus columns listingsss:
  - Tha path to the PDF reports of WF1
  - The path to the yaml files to use for running workflow 3 or 4
  - The path to the json files that will contain the results of the workflow 3 or 4 run
  - The command lines to paste on your shell to run workflow 3 or 4 on the select Galaxy instance. Set or replace `$MAINKEY` variable with your Galaxy API ID.

### Run Workflow 4

- To change a parameter for one species, modify the file `${assembly_ID}/job_files/wf[3/4]_${assembly_ID}.yml`. To change the parameters for all jobs, modify the file `wf[3/4]_run.sample.yaml` before runing  `prepare_wf[3/4].py`
- Use the generated command line to upload the data and run the workflow.

>> WARNING: Disconnecting your terminal before the command finish will interrupt the process!

## Prepare files for workflow 8 (after wf4)

> Note: If the json file with the invocation details is missing or in error, but the invocation is successful in Galaxy, add the invocation number to the generated table in the column `Invocation_wf4` or run `fetch_invocation_numbers.py`. This can happen if:
>
>- The planemo command for wf4 has been interrupted,
>- The Workflow had errors but you fixed it in the interface
>
> Try this if you get an error `IndexError: list index out of range` when running `prepare_wf8.py`

General inputs:

1. The table named `wf_run_$Input_table` generated by `prepare_wf[3/4].py` **(-t)**
2. The target Galaxy instance (e.g. `https://usegalaxy.org/`)  **(-g)**
3. The API key for your Galaxy instance  **(-k)**
4. The haplotype being assembled: **-1** for Haplotype 1,  **-2** for Haplotype 2,  **-p** for paternal haplotype , or  **-m** for maternal haplotype.
5. Optional: a suffix for the analysis **(-s)** (e.g. `2.0` will produce files called `wf8_$S{assembly_ID}_2.0.yaml` )
6. Optional: Use fast simultaneous uploads to Galaxy  **(-f)** . Warning: May cause errors if there are failed datasets in your history. Use with caution.

Upload a new workflow to Galaxy **(--from_file)**:

1. The directory containing the workflows **(-w)**. If the directory doesn't exist, it will be created. If the workflow file isn't in the directory, the workflow will be downloaded.
2. Optional: Specify a workflow version to use, Default is 3.1 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

Use a workflow existing in Galaxy **(--from_id)**:

1. The Galaxy ID of workflow VGP8 **(-i)**.

Usage:

````bash
  python <path to scripts>/batch_vgp_run/prepare_wf8.py  -t  <Tracking table>  -g <Galaxy Instance> -k <API Key>  
          [--from_file -w <Workflow Directory>  -v <Workflow version> ] 
          [--from_id -i <Workflow ID>] 
      -s <Optional suffix> -1
````

> Note: You can run this step even if some of your invocations are not finished. The script will skip lines with incomplete invocations and lines that have already been processed. If you want to re-generate the files and command for a species, delete the file `${assembly_ID}/job_files/w8_${assembly_ID}_${haplotype}.yaml`.

### Output

For each Species:

- A Yaml File containing the input paths and the job parameters named `wf8_${assembly_ID}_${haplotype}.yml` in the folder `${assembly_ID}/job_files`

For all:

- A table named `wf_run_${Input_table}` containing the input table plus columns listing : 
  - The path to the PDF reports of WF4 
  - The yaml files to use for running workflow 8
  - The json files that will contain the results of the workflow 8 run on the specified haplotype
  - The command lines to paste on your shell to run workflow 8 on the select Galaxy instance.  Set or replace `$MAINKEY` variable with your Galaxy API Key.

## Prepare Files for workflow 9 (after workflow 8)

>> Warning: you need the tool NCBI dataset installed on your system : https://www.ncbi.nlm.nih.gov/datasets/docs/v2/command-line-tools/download-and-install/

> Note: If the json file with the invocation details is missing or in error, but the invocation is successful in Galaxy, add the invocation number to the generated table in the column `Invocation_wf8_hap` or run `fetch_invocation_numbers.py`. This can happen if:
>
>- The planemo command for wf4 has been interrupted,
>- The Workflow had errors but you fixed it in the interface
>
> Try this if you get an error `IndexError: list index out of range` when running `prepare_wf9.py`

General inputs:

1. The table named `wf_run_$Input_table` generated by `prepare_wf8.py` **(-t)**
2. The target Galaxy instance (e.g. `https://usegalaxy.org/`)  **(-g)**
3. The API key for your Galaxy instance  **(-k)**
4. The haplotype being assembled: **-1** for Haplotype 1,  **-2** for Haplotype 2,  **-p** for paternal haplotype , or  **-m** for maternal haplotype.
5. Optional: a suffix for the analysis **(-s)** (e.g. `2.0` will produce files called `wf9_$S{assembly_ID}_2.0.yaml` )

Upload a new workflow to Galaxy **(--from_file)**:

1. The directory containing the workflows **(-w)**. If the directory doesn't exist, it will be created. If the workflow file isn't in the directory, the workflow will be downloaded.
2. Optional: Specify a workflow version to use, Default is 1.1 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

Use a workflow existing in Galaxy **(--from_id)**:

1. The Galaxy ID of workflow VGP1 **(-i)**.
2. The version of the decontamination workflow:
    - Legacy **(-l)** (before 0.9) with Kraken2.
    - New **(-f)** (from 1.0) with NCBI FCS-GX.


Usage:

````bash
  python <path to scripts>/batch_vgp_run/prepare_wf8.py  -t  <Tracking table>  -g <Galaxy Instance> -k <API Key>  
        [--from_file -w <Workflow Directory>  -v <Workflow version> ] 
        [--from_id -i <Workflow ID>] 
    -s <Optional suffix> -1
````

>> WARNING: By default this command will use the decontamination workflow with Kraken. To use the version with FCS-Gx, download the workflow and provide the path to the file instead of the version number after the option `-v`

> Note: You can run this step even if some of your invocations are not finished. The script will skip lines with incomplete invocations and lines that have already been processed. If you want to re-generate the files and command for a species, delete the file `${assembly_ID}/job_files/w9_${assembly_ID}_${haplotype}.yaml`.

### Output

For each Species:

- A Yaml File containing the input paths and the job parameters named `wf9_${assembly_ID}_${haplotype}.yml` in the folder `${assembly_ID}/job_files`

For all:

- A table named `wf_run_${Input_table}` containing the input table plus columns listing:
  - The path to the PDF reports of WF8
  - The yaml files to use for running workflow 9
  - The json files that will contain the results of the workflow 9 run on the specified haplotype
  - The command lines to paste on your shell to run workflow 9 on the select Galaxy instance.  Set or replace `$MAINKEY` variable with your Galaxy API Key.

## Prepare Files for workflow 0

> Note: If the json file with the invocation details is missing or in error, but the invocation is successful in Galaxy, add the invocation number to the generated table in the column `Invocation_wf1` or run `fetch_invocation_numbers.py`. This can happen if: 
>
>- The planemo command for wf4 has been interrupted,
>- The Workflow had errors but you fixed it in the interface
>
> Try this if you get an error `IndexError: list index out of range` when running `prepare_wf9.py`

General inputs:

1. The table named `wf_run_$Input_table` generated by `prepare_wf1.py` or later workflows **(-t)**
2. The target Galaxy instance (e.g. `https://usegalaxy.org/`)  **(-g)**
3. The API key for your Galaxy instance  **(-k)**
4. Your email adress, requested to run MitoHifi.
5. Optional: a suffix for the analysis **(-s)** (e.g. `2.0` will produce files called `wf9_$S{assembly_ID}_2.0.yaml` )

Upload a new workflow to Galaxy **(--from_file)**:

1. The directory containing the workflows **(-w)**. If the directory doesn't exist, it will be created. If the workflow file isn't in the directory, the workflow will be downloaded.
2. Optional: Specify a workflow version to use, Default is 0.2 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

Use a workflow existing in Galaxy **(--from_id)**:

1. The Galaxy ID of workflow VGP0 **(-i)**.

Usage:

````bash
  python <path to scripts>/batch_vgp_run/prepare_wf0.py  -t  <Tracking table>  -g <Galaxy Instance> -k <API Key>  
        [--from_file -w <Workflow Directory>  -v <Workflow version> ] 
        [--from_id -i <Workflow ID>] 
    -e <Email> -s <Optional suffix> -1
````

> Note: You can run this step even if some of your invocations are not finished. The script will skip lines with incomplete invocations and lines that have already been processed. If you want to re-generate the files and command for a species, delete the file `${assembly_ID}/job_files/w9_${assembly_ID}_${haplotype}.yaml`.

### Output

For each Species:

- A Yaml File containing the input paths and the job parameters named `wf9_${assembly_ID}_${haplotype}.yml` in the folder `${assembly_ID}/job_files`

For all:

- A table named `wf_run_${Input_table}` containing the input table plus columns listing:
  - The yaml files to use for running workflow 0
  - The json files that will contain the results of the workflow 0
  - The command lines to paste on your shell to run workflow 0 on the select Galaxy instance.  Set or replace `$MAINKEY` variable with your Galaxy API Key.

## Fetch invocation numbers

This tool fetch the invocations linked to a history and fill the invocation numbers in the appropriate columns. If several histories with the same name exist, it will print a warning and use the most recent histories. If multiple non-failed invocations of the same workflow exist in the same history, it will print a warning and use the most recent.

Inputs:

1. The table named `wf_run_$Input_table` generated by `prepare_wf1.py` or later workflows **(-t)**
2. The target Galaxy instance (e.g. `https://usegalaxy.org/`)  **(-g)**
3. The API key for your Galaxy instance  **(-k)**

Usage:

````bash
  python <path to scripts>/batch_vgp_run/fetch_invocation_numbers.py  -t  <Tracking table>  -g <Galaxy Instance> -k <API Key>  
````

Outputs:

1. The input table with filled invocation numbers.
