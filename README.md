# Run Assembly Scripts through the command line

Scripts to run the VGP pipelines through planemo - Do not Support Trio data yet
Designed to import data from the Genomeark AWS repository.

Note : For Pre-Curation Workflow, go to the pre_curation folder after installing the dependencies.

## Dependencies

See the file installs.sh for the list of dependencies

## First step - Prepare a file with the species informations

Create a tabulated file with the following columns:

1. Species Name (no space, underscores) (e.g. Taeniopygia_guttata)
2. Assembly ID (e.g. bTaeGut2)

Usage:

````bash
  python <path to scripts>/batch_vgp_run/get_urls.py -t <Table with Species and Assembly ID> 
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
  python <path to scripts>/batch_vgp_run/get_urls.py -t <Table with Species and Assembly ID> --add -s <Species Name> -a <Species ID>
````

## Prepare files for Workflow 1

General Inputs:

1. The name of the table with the paths to the data (output of the previous step) **(-t)**.
2. The target Galaxy instance (e.g. `https://usegalaxy.org/`) **(-g)**.
3. The API key for the selected galaxy instance **(-k)**.
4. Optional: a suffix for the analysis **(-s)** (e.g. `2.0` will produce files called `wf1_$S{assembly_ID}_2.0.yaml`).

Upload a new workflow to Galaxy **(--from_file)**:

1. The directory containing the workflows **(-w)**. If the directory doesn't exist, it will be created. If the workflow file isn't in the directory, the workflow will be downloaded.
2. Optional: Specify a workflow version to use, Default is 0.4 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

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
2. Optional: Specify a workflow version to use, Default is 0.3.13 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

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

Upload a new workflow to Galaxy **(--from_file)**:

1. The directory containing the workflows **(-w)**. If the directory doesn't exist, it will be created. If the workflow file isn't in the directory, the workflow will be downloaded.
2. Optional: Specify a workflow version to use, Default is 3.0 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

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
2. Optional: Specify a workflow version to use, Default is 0.2 **(-v)**. Warning: Changing this parameter may cause errors if the workflow inputs are different from the default version.

Use a workflow existing in Galaxy **(--from_id)**:

1. The Galaxy ID of workflow VGP1 **(-i)**.
2. The version of the decontamination workflow:
    - Legacy **(-l)** (before 0.9) with Kraken2.
    - New **(-k)** (from 0.9) with NCBI FCS-GX.


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
