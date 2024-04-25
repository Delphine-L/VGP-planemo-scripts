# VGP-planemo-scripts

Scripts to run the VGP pipelines through planemo - Do not Support Trio data yet
Designed to import data from the Genomeark AWS repository.

## Dependencies

See the file installs.sh for the list of dependencies

## First step - Add a species to the file tracking table

You need: 
1. the species name (no space, underscores) (e.g. Taeniopygia_guttata)-
2. the specimen ID (e.g. bTaeGut2), and 
3. the path of the output table (e.g. ./list_file.tab)

````bash
sh VGP-planemo-scripts/get_files_names.sh $Species_name $Specimen_ID $output
````

### Output : 

A tabular file containing the names of PacBio, Arima, and Bionano files on Genomark

e.g.

````tabular
Taeniopygia_guttata	bTaeGut2	m54306U_210519_154448.hifi_reads.fastq.gz m54306U_210521_004211.hifi_reads.fastq.gz m54306Ue_210629_211205.hifi_reads.fastq.gz m54306Ue_210719_083927.hifi_reads.fastq.gz m64055e_210624_223222.hifi_reads.fastq.gz	bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L1_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L2_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L3_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L4_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L5_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L6_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L7_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L8_R1.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMMCCXY_L6_R1.fq.gz	bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L1_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L2_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L3_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L4_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L5_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L6_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L7_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMFCCXY_L8_R2.fq.gz bTaeGut2_ARI8_001_USPD16084394-AK5146_HJFMMCCXY_L6_R2.fq.gz	bTaeGut2_Saphyr_DLE1_3172351.cmap
````



## Prepare files for wf1 

You need: 
1. the name of the table output of the previous step
2. The prefix for the yaml files used to run the workflow (e.g. `./test` will produce files called `./test_wf1_$Specimen_ID.yaml` )

````bash
sh VGP-planemo-scripts/prepare_wf1.py $Input_table $Yaml_prefix
````

To change the parameters of all jobs, modify the file `wf1_run.sample.yaml`


### Output : 

For each Species : 
- A Yaml File containing the input paths and the job parameters named `$Yaml_prefix_wf1_$Specimen_ID.yml` (To modify individual job parameter modify these)

For all : 
- A table named `wf_run_$Input_table` containing the input table plus columns listing : 
  - The yaml file to use for the workflow
  - The json file that will contain the results of the workflow
  - The command line to paste on your shell to run the workflow on Galaxy.org (Change the command line if you want to run against another galaxy instance). Set or replace `$MAINKEY` variable with your Galaxy API ID.


## Wait for the invocations to be ready before preparing files for workflow 3 or 4


## Prepare files for workflow 3 or 4 