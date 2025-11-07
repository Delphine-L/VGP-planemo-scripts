#!/usr/bin/env python3

import pandas
import argparse
import re
import pathlib
import function
import textwrap
import os
from bioblend.galaxy import GalaxyInstance


def main():

    parser = argparse.ArgumentParser(
                        prog='prepare_wf1',
                        description='After running wf1, download the qc and prepare the job files and command line to run wf4',
                        usage='prepare_wf1.py -t <Table with file paths> -g <Galaxy url> -k <API Key> [OPTIONS]',
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
    parser.add_argument('-k', '--apikey', dest="apikey",required=True, help="Your Galaxy API Key")  
    parser.add_argument('-s', '--suffix', dest="suffix",  required=False,  default="", help="Optional: Specify a suffix for your run (e.g. 'v2.0' to name the job file wf1_mCteGun2_v2.0.yaml)") 
    
    use_file = parser.add_argument_group("Workflow File","Use the following options to use a workflow file.")
    use_file.add_argument('--from_file', action='store_true', required=False, help='Use a workflow file.')
    use_file.add_argument('-v', '--wfl_version', dest="wfl_version",  required=False,  default="0.5", help="Optional: Specify which version of the workflow to run. Must be compatible with the sample yaml files (default: 0.5)")    
    use_file.add_argument('-w', '--wfl_dir', dest="wfl_dir",  required=False,  default="", help="Directory containing the workflows. If the directory doesn't exist, it will be created and the workflow downloaded.") 

    use_id = parser.add_argument_group("Workflow ID","If you already have the workflow in your Galaxy instance, use the following options to use the workflow ID.")
    use_id.add_argument('--from_id', action='store_true', required=False, help='Use a workflow ID.')
    use_id.add_argument('-i', '--wfl_id', dest="wfl_id",  default="",  required=False, help="Workflow ID.")
    args = parser.parse_args()

    
    path_script=str(pathlib.Path(__file__).parent.resolve())

    suffix_run,galaxy_instance=function.fix_parameters(args.suffix, args.instance)
 
    gi = GalaxyInstance(galaxy_instance, args.apikey)



    Compatible_version=args.wfl_version
    workflow_name="kmer-profiling-hifi-VGP1"

    if args.from_id and args.from_file:
        raise SystemExit("Error: Please select only one of the two options: --from_id or --from_file.")
    elif not args.from_id and not args.from_file:
        raise SystemExit("Error: Please select one of the two options: --from_id or --from_file.")
    elif args.from_file:
        if args.wfl_version=="":
            raise SystemExit("Missing option: -v. If you select the --from_file option, you need to provide a workflow version.") 
        elif args.wfl_dir=="":
            raise SystemExit("Missing option: -w. If you select the --from_file option, you need to provide a workflow directory.")
        wfl_dir=function.fix_directory(args.wfl_dir)
        worfklow_path, release_number = function.get_worfklow(Compatible_version, workflow_name, wfl_dir)
    elif args.from_id:
        if args.wfl_id=="":
            raise SystemExit("Missing option: -i. If you select the --from_id option, you need to provide a workflow ID.")
        worfklow_path = args.wfl_id
        release_number = 'NA'
        wfl_info=gi.workflows.show_workflow(worfklow_path)
        wfl_name=wfl_info['name']
        if 'VGP1' not in wfl_name:
            raise SystemExit("Error: The workflow ID provided does not correspond to the kmer-profiling-hifi-VGP1 workflow. Please check the ID.")

    infos=pandas.read_csv(args.species, header=0, sep="\t")
    list_yml=[]
    list_res=[]
    commands=[]
    for i,row in infos.iterrows():
        spec_name=infos.iloc[i]['Species']
        assembly_id=infos.iloc[i]['Assembly']

        # Use Working_Assembly if it exists (for multiple assemblies from same species)
        # Otherwise use Assembly for backward compatibility
        if 'Working_Assembly' in infos.columns and pandas.notna(infos.iloc[i]['Working_Assembly']) and str(infos.iloc[i]['Working_Assembly']).strip() != '':
            spec_id = infos.iloc[i]['Working_Assembly']
        else:
            spec_id = assembly_id

        hifi_col=infos.iloc[i]['Hifi_reads']
        if hifi_col=='NA':
            print('Warning: '+spec_id+' has been skipped because it has no PacBio reads.')
            continue
        list_pacbio=hifi_col.split(',')

        # Get custom path if it exists (for non-standard GenomeArk directory structure)
        custom_path = ''
        if 'Custom_Path' in infos.columns and pandas.notna(infos.iloc[i]['Custom_Path']) and str(infos.iloc[i]['Custom_Path']).strip() != '':
            custom_path = '/' + str(infos.iloc[i]['Custom_Path']).strip()

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
            # Use assembly_id (not spec_id) in GenomeArk URL, with optional custom_path
            str_elements=str_elements+"\n  - class: File\n    identifier: "+name+"\n    path: gxfiles://genomeark/species/"+spec_name+"/"+assembly_id+custom_path+"/genomic_data/pacbio_hifi/"+i+"\n    filetype: fastqsanger.gz"
        with open(path_script+"/templates/wf1_run.sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()
        filedata = filedata.replace('["Pacbio"]', str_elements )
        filedata = filedata.replace('["species_name"]', spec_name )
        filedata = filedata.replace('["assembly_name"]', spec_id )
        with open(yml_file, 'w') as yaml_wf1:
            yaml_wf1.write(filedata)
        cmd_line="planemo run "+worfklow_path+" "+yml_file+" --engine external_galaxy --galaxy_url "+galaxy_instance+" --simultaneous_uploads --check_uploads_ok --galaxy_user_key $MAINKEY --simultaneous_uploads --check_uploads_ok --history_name "+spec_id+suffix_run+" --no_wait --test_output_json "+res_file+" > "+log_file+" 2>&1 &"
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
        


       