# Run the Precuration workflow from the Command Line

## Install dependencies

See parent directory. 

## Get Files for a selected set of species

### Input 

Create a table with one species per line and with the following columns :
	1. The species name with underscores and no spaces. E.g. : `Peponocephala_electra`
	2. The Species ID. E.g. : `mPepEle1`
	3. The path on genomeark : E.g. : `genomeark/species/Ictidomys_tridecemlineatus/mIctTri1/`
	4. The assembly technology used : `HiC`, `standard`, or `trio` (careful of the word casing)
	5. The Technology used for generating the Hi-C data : `arima` or `dovetail`

### Run the Script to fetch the data paths

Run: 

```bash
sh VGP-planemo-scripts/pre_curation/get_files_pre_curation.sh table_of_species.tsv  output.tsv 

```

The script will generate a table with the path for all the necessary files with the columns:
	1. Species name
	2. Species ID
	3. Assembly method
	4. Hi-C Technology
	5. Names of Fastq files containing the HiFi reads
	6. Names of Fastq files containing the Hi-C forward reads
	7. Names of Fastq files containing the Hi-C reverse reads
	8. The path to  Haploptype 1 (or primary or paternal)
	9. The path to  Haploptype 2 (or alt or maternal)
  

### Verify the table

Look at :
	- Empty columns. The data may not be at the right place on the genomeark repo. If so, remove the line and run this specie manually
	- That the forward and reverse Hi-C files match. In cases where the Hi_C files names are on the format `species_1.1` and `species_1.2`, the script has a hard time determining which file is forward and wich is reverse. In this situation, correct manually in the table. 

## Prepare the yaml files and command lines

Set a shell variable with your Galaxy API Key for the target instance. 

```bash
api_key="my_key"
```
Run :

```bash
python VGP-planemo-scripts/pre_curation/prepare_pre_curation.py -s files_table.tsv -g https://vgp.usegalaxy.org/ -d output_repository -a api_key

```
Note that the output repository must exist.

For each species, a `.yaml` file is created that contains the path to the inputs and the parameters of the workflow. 

Note: If you wish to modify the worfklow parameters for all the species, modify the file `pre_curation_run.sample.yaml` before running this step.

The command will display the command lines for each species. You can also fin the command line and other informations in a created table called `wf_run_[...].tsv`

Warning: This script does NOT run the workflows for you. You will need to copy and paste the command lines and run them yourself. This is voluntary as to avoid creating hundreds of invocations by accident.  

## Run the Command line : 

Make sure you have installed planemo and set your API Key.
Paste the command line created by the previous step : 

```bash
planemo run VGP-planemo-scripts/pre_curation/PretextMap_Generation.ga test_run/pre_curation_mPlaHel1.yaml --engine external_galaxy --galaxy_url https://vgp.usegalaxy.org/ --galaxy_user_key $MAINKEY --history_name mPlaHel1 --no_wait --test_output_json test_run/pre_curation_invocation_mPlaHel1.json &
```

This command will upload the input files to the target Galaxy instance in a new history, and upload the workflow to your galaxy account. 
Once the files are uploaded, the workflow will be run on the galaxy instance.
This will create a `.json` file containing the invocation information. 


## Default Parameters

The default parameters are set depending on the assembly methods and Hi-C technology used: 
- Assembly Methods:
  - Standard: only the primary assembly will be used to generate the Pretextmap. Scaffold suffix is set to "H1"
  - Hi-C Phasing:  Both hap1 and hap2 are used to generate the Pretextmap. Scaffold suffixes are set to "H1" and "H2"
  - Trio:  Both pat and mat are used to generate the Pretextmap. Scaffold suffixes are set to "pat_H1" and "mat_H2"
- Hi-C Technology:
  - Arima: Hi-C reads trimming option set to "yes"
  - Dovetail:  Hi-C reads trimming option set to "no"