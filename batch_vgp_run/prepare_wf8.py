#!/usr/bin/env python3


import json 
import sys
import argparse
import pandas
import re
import pathlib
import function
from bioblend.galaxy import GalaxyInstance
from io import StringIO
import textwrap
import os


def main():

    parser = argparse.ArgumentParser(
                        prog='prepare_wf8',
                        description='After running wf4, download the qc and prepare the job files and command line to run wf8',
                        usage='prepare_wf8.py  -t  <Tracking table>  -k <API Key> -g <Galaxy Instance> [OPTIONS]',
                        formatter_class=argparse.RawTextHelpFormatter,
                        epilog=textwrap.dedent('''
                                            General outputs: 
                                            - {Tracking_table}: The tracking table updated with wf8 runs. 
                                            For each species in {table}:
                                            - {assembly_id}/job_files/wf8_{assembly_id}_{suffix}_{haplotype}.yaml: The yaml file with the job inputs and parameters.
                                            - {assembly_id}/invocations_json/wf8_{assembly_id}_{suffix}_{haplotype.json:  The json file with the invocation details.
                                            '''))
    parser.add_argument('-t', '--table', dest="track_table",required=True, help='File containing the species and input files (Produced by prepare_wf4.py) ')  
    parser.add_argument('-k', '--apikey', dest="apikey",required=True, help="Your Galaxy API Key")  
    parser.add_argument('-g', '--galaxy_instance', dest="instance", required=True, help='The URL of your prefered Galaxy instance. E.g https://vgp.usegalaxy.org/ ')  
    parser.add_argument('-s', '--suffix', dest="suffix",  required=False,  default="", help="Optional: Specify a suffix for your run (e.g. 'v2.0' to name the job file wf8_mCteGun2_v2.0.yaml)") 

    group = parser.add_argument_group("Haplotype","Select one and only one of these options to specify what haplotype you are scaffolding")
    haps = group.add_mutually_exclusive_group() 
    haps.add_argument('-1', '--hap1', action='store_true', required=False, help='Scaffold Haplotype 1')  
    haps.add_argument('-2', '--hap2', action='store_true', required=False, help='Scaffold Haplotype 2')  
    haps.add_argument('-p', '--pat', action='store_true', required=False, help='Scaffold paternal haplotype')       
    haps.add_argument('-m', '--mat', action='store_true', required=False, help='Scaffold maternal haplotype')  
 
    use_file = parser.add_argument_group("Workflow File","Use the following options to use a workflow file.")
    use_file.add_argument('--from_file', action='store_true', required=False, help='Use a workflow file.')
    use_file.add_argument('-v', '--wfl_version', dest="wfl_version",  required=False,  default="3.0", help="Optional: Specify which version of the workflow to run. Must be compatible with the sample yaml files (default: 3.0)")    
    use_file.add_argument('-w', '--wfl_dir', dest="wfl_dir",  required=False,  default="", help="Directory containing the workflows. If the directory doesn't exist, it will be created and the workflow downloaded.") 

    use_id = parser.add_argument_group("Workflow ID","If you already have the workflow in your Galaxy instance, use the following options to use the workflow ID.")
    use_id.add_argument('--from_id', action='store_true', required=False, help='Use a workflow ID.')
    use_id.add_argument('-i', '--wfl_id', dest="wfl_id",  required=False, help="Workflow ID.")

    args = parser.parse_args()


    path_script=str(pathlib.Path(__file__).parent.resolve())

    suffix_run,galaxy_instance=function.fix_parameters(args.suffix, args.instance)
    gi = GalaxyInstance(galaxy_instance, args.apikey)

    Compatible_version=args.wfl_version
    workflow_name="Scaffolding-HiC-VGP8"

    if args.from_id and args.from_file:
        raise SystemExit("Error: Please select only one of the two options: --from_id or --from_file.")
    elif not args.from_id and not args.from_file:
        raise SystemExit("Error: Please select one of the two options: --from_id or --from_file.")
    elif args.from_file:
        if args.wfl_version==False:
            raise SystemExit("Missing option: -v. If you select the --from_file option, you need to provide a workflow version.") 
        elif args.wfl_dir==False:
            raise SystemExit("Missing option: -w. If you select the --from_file option, you need to provide a workflow directory.")
        wfl_dir=function.fix_directory(args.wfl_dir)
        worfklow_path, release_number = function.get_worfklow(Compatible_version, workflow_name, wfl_dir)
    elif args.from_id:
        if args.wfl_id==False:
            raise SystemExit("Missing option: -i. If you select the --from_id option, you need to provide a workflow ID.")
        worfklow_path = args.wfl_id
        release_number = 'NA'
        wfl_info=gi.workflows.show_workflow(worfklow_path)
        wfl_name=wfl_info['name']
        if 'VGP8' not in wfl_name:
            raise SystemExit("Error: The workflow ID provided does not correspond to the Scaffolding-HiC-VGP8 workflow. Please check the ID.")


    if args.hap1:
        haplotype="Haplotype 1"
        gfa_input='usable hap1 gfa'
        hap_for_path="hap1"
    elif args.hap2:
        haplotype="Haplotype 2"
        gfa_input='usable hap2 gfa'
        hap_for_path="hap2"
    elif args.pat:
        haplotype="Paternal"
        gfa_input='usable hap1 gfa'
        hap_for_path="pat"
    elif args.mat:
        haplotype="Maternal"
        gfa_input='usable hap2 gfa'
        hap_for_path="mat"
    else:
        raise SystemExit("Please specify the haplotype being scaffolded")


    infos=pandas.read_csv(args.track_table, header=0, sep="\t" )
    infos = infos.fillna(value={'Invocation_wf4':'NA'}) 
    infos = infos.fillna(value={'WF4_result_json':'NA'}) 

    list_yml=[]
    list_res=[]
    commands=[]
    list_reports=[]
    list_invocation=[]

    for i,row in infos.iterrows():
        spec_name=row['Species']
        spec_id=row['Assembly']
        species_path="./"+spec_id+"/"
        if row['Invocation_wf4']=='NA':
            json_wf4=infos.iloc[i]['WF4_result_json']
            if os.path.exists(json_wf4):
                wf4json=open(json_wf4)
                reswf4=json.load(wf4json)
                invocation_number=reswf4["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                row.loc['Invocation_wf4']=invocation_number
            else:
                print("Skipped "+spec_id+": No Json or invocation number found")
                list_reports.append("NA")
                commands.append("NA")
                list_res.append("NA")
                list_yml.append("NA")
                continue
        else:
            invocation_number=row['Invocation_wf4']

        res_file=species_path+"invocations_json/wf8_"+spec_id+suffix_run+"_"+hap_for_path+".json"
        yml_file=species_path+"job_files/wf8_"+spec_id+suffix_run+"_"+hap_for_path+".yml"
        log_file=species_path+"planemo_log/"+spec_id+suffix_run+"_wf8.log"

        if os.path.exists(yml_file):
            print("Skipped "+spec_id+"_"+hap_for_path+": Files and command already generated")
            list_reports.append(row['Wf4_Report'])
            commands.append(row['Wf8_Commands_'+hap_for_path])
            list_res.append(row['WF8_result_json_'+hap_for_path])
            list_yml.append(row['WF8_job_yml_'+hap_for_path])
            continue

        wf4_inv=gi.invocations.show_invocation(str(invocation_number))
        invocation_state=gi.invocations.get_invocation_summary(str(invocation_number))['populated_state']

        if invocation_state!='ok':
            print("Skipped "+spec_id+"_"+hap_for_path+": Invocation incomplete, Status: "+invocation_state+", url: "+args.instance+"/workflows/invocations/"+invocation_number)
            list_reports.append("NA")
            commands.append("NA")
            list_res.append("NA")
            list_yml.append("NA")
            continue

                    
        dic_data_ids=function.get_datasets_ids(wf4_inv)
        if dic_data_ids['Species Name']!=spec_name:
            raise SystemExit("Error: The species name for the invocation does no fit the name in the table: "+spec_name+". Please check the invocation number.") 

        dic_data_ids['haplotype']=haplotype
        dic_data_ids['gfa_assembly']=dic_data_ids[gfa_input]

        gi.invocations.get_invocation_report_pdf(str(invocation_number),file_path=species_path+'reports/report_wf4_'+spec_id+suffix_run+'_'+invocation_number+'.pdf')

        list_reports.append(species_path+'reports/report_wf4_'+spec_id+suffix_run+'_'+invocation_number+'.pdf')

        history_id=wf4_inv['history_id']
        list_yml.append(yml_file)
        list_res.append(res_file)
        cmd_line="planemo run "+worfklow_path+" "+yml_file+" --engine external_galaxy --galaxy_url "+galaxy_instance+"  --simultaneous_uploads --check_uploads_ok --galaxy_user_key $MAINKEY --history_id "+history_id+" --no_wait --test_output_json "+res_file+" > "+log_file+" 2>&1  &"
        commands.append(cmd_line)
        print(cmd_line)
        with open(path_script+"/templates/wf8_run_sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()

        pattern = r'\["(.*)"\]'  # Matches the fields to replace
        to_fill = re.findall(pattern, filedata)

        for i in to_fill:
            filedata = filedata.replace('["'+i+'"]', dic_data_ids[i] )

        with open(yml_file, 'w') as yaml_wf:
            yaml_wf.write(filedata)

    infos['Wf4_Report']=list_reports
    infos['WF8_job_yml_'+hap_for_path]=list_yml
    infos['WF8_result_json_'+hap_for_path]=list_res
    infos['Wf8_Commands_'+hap_for_path]=commands
    infos['Invocation_wf8_'+hap_for_path]='NA'
    infos.to_csv(args.track_table, sep='\t', header=True, index=False)

if __name__ == "__main__":
    main()
        