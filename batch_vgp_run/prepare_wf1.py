#!/usr/bin/env python3


import pandas
import argparse
import re
import pathlib
import function
import sys
from io import StringIO
import textwrap
import os



def main():

	parser = argparse.ArgumentParser(
						prog='prepare_wf1',
						description='After running wf1, download the qc and prepare the job files and command line to run wf4',
						usage='prepare_wf1.py -t <Table with file paths> -g <Galaxy url>  -w <Workflow Directory>  -v <Workflow version> -s <Suffix> ',
						formatter_class=argparse.RawTextHelpFormatter,
						epilog=textwrap.dedent('''
											General outputs: 
											- wf_run_{table}: A table with the details of the batch run
											For each species in {table}:
											- {assembly_id}/job_files/wf1_{assembly_id}_{suffix}.yaml: The yaml file with the job inputs and parameters.
											- {assembly_id}/invocations_json/wf1_invocation_{assembly_id}_{suffix}.json:  The json file with the invocation details.
											'''))
	
	parser.add_argument('-t', '--table', dest="species",required=True, help='File containing the species and input files (Produced with get_files_names.sh)')  
	parser.add_argument('-g', '--galaxy_instance', dest="instance", required=True, help='The URL of your prefered Galaxy instance')  
	parser.add_argument('-w', '--wfl_dir', dest="wfl_dir",  required=True,  default="", help="Directory containing the workflows. If the directory doesn't exist, it will be created and the workflow downloaded.") 
	parser.add_argument('-v', '--wfl_version', dest="wfl_version",  required=False,  default="0.4", help="Optional: Specify which version of the workflow to run. Must be compatible with the sample yaml files (default: 0.4)")    
	parser.add_argument('-s', '--suffix', dest="suffix",  required=False,  default="", help="Optional: Specify a suffix for your run (e.g. 'v2.0' to name the job file wf1_mCteGun2_v2.0.yaml)") 
	args = parser.parse_args()




	
	path_script=str(pathlib.Path(__file__).parent.resolve())

	suffix_run,wfl_dir,galaxy_instance=function.fix_parameters(args.suffix, args.wfl_dir, args.instance)


	Compatible_version=args.wfl_version
	workflow_name="kmer-profiling-hifi-VGP1"

	worfklow_path,release_number=function.get_worfklow(Compatible_version, workflow_name, wfl_dir)


	infos=pandas.read_csv(args.species, header=0, sep="\t")
	list_yml=[]
	list_res=[]
	commands=[]
	for i,row in infos.iterrows():
		spec_name=infos.iloc[i]['Species']
		spec_id=infos.iloc[i]['Assembly']
		hifi_col=infos.iloc[i]['Hifi_reads']
		if hifi_col=='NA':
			print('Warning: '+spec_id+' has been skipped because it has no PacBio reads.')
			continue
		list_pacbio=hifi_col.split(',')
		species_path="./"+spec_id+"/"
		os.makedirs(species_path, exist_ok=True)
		os.makedirs(species_path+"job_files/", exist_ok=True)
		os.makedirs(species_path+"invocations_json/", exist_ok=True)
		os.makedirs(species_path+"reports/", exist_ok=True)
		os.makedirs(species_path+"planemo_log/", exist_ok=True)
		str_elements=""
		yml_file=species_path+'job_files/wf1_'+spec_id+suffix_run+'.yml'
		res_file=species_path+'invocations_json/wf1_'+spec_id+suffix_run+'.json'
		list_yml.append(yml_file)
		log_file=species_path+"planemo_log/"+spec_id+suffix_run+"_wf1.log"
		list_res.append(res_file)
		for i in list_pacbio:
			name=re.sub(r"\.f(ast)?q(sanger)?\.gz","",i)
			str_elements=str_elements+"\n  - class: File\n    identifier: "+name+"\n    path: gxfiles://genomeark/species/"+spec_name+"/"+spec_id+"/genomic_data/pacbio_hifi/"+i+"\n    filetype: fastqsanger.gz"
		with open(path_script+"/templates/wf1_run.sample.yaml", 'r') as sample_file:
			filedata = sample_file.read()
		filedata = filedata.replace('["Pacbio"]', str_elements )
		filedata = filedata.replace('["species_name"]', spec_name )
		filedata = filedata.replace('["assembly_name"]', spec_id )
		with open(yml_file, 'w') as yaml_wf1:
			yaml_wf1.write(filedata)
		cmd_line="planemo run "+worfklow_path+" "+yml_file+" --engine external_galaxy --galaxy_url "+galaxy_instance+" --galaxy_user_key $MAINKEY --simultaneous_uploads --check_uploads_ok --history_name "+spec_id+suffix_run+" --no_wait --test_output_json "+res_file+" > "+log_file+" 2>&1 &"
		commands.append(cmd_line)
		print(cmd_line)
	infos["Job_File_wf1"]=list_yml
	infos["Version_wf1"]=release_number
	infos["Results_wf1"]=list_res
	infos["Command_wf1"]=commands
	infos["Invocation_wf1"]='NA'
	infos.to_csv(args.species, sep='\t', header=True, index=False)

if __name__ == "__main__":
	main()
		


	   