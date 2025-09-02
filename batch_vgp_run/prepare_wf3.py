#!/usr/bin/env python3

import json 
import sys
import argparse
import pandas
import re
import pathlib
import function
from bioblend.galaxy.objects import GalaxyInstance
from io import StringIO
import textwrap
import os


def main():

    parser = argparse.ArgumentParser(
                        prog='prepare_wf3',
                        description='After running wf1, download the qc and prepare the job files and command line to run wf4',
                        usage='prepare_wf3.py  -t  <Tracking table> -g <Galaxy Instance> -k <API Key>  -w <workflow directory> -v <workflow version> -s <Optional suffix>',
                        formatter_class=argparse.RawTextHelpFormatter,
                        epilog=textwrap.dedent('''
                                            General outputs: 
                                            - {Tracking_table}: The tracking table updated with wf4 runs. 
                                            For each species in {table}:
                                            - {assembly_id}/job_files/wf4_{assembly_id}_{suffix}.yaml: The yaml file with the job inputs and parameters.
                                            - {assembly_id}/invocations_json/wf4_invocation_{assembly_id}_{suffix}.json:  The json file with the invocation details.
                                            '''))
    
    parser.add_argument('-t', '--table', dest="track_table",required=True, help='File containing the species and input files (Produced by prepare_wf1.py) ')  
    parser.add_argument('-g', '--galaxy_instance', dest="instance", required=True, help='The URL of your prefered Galaxy instance. E.g https://vgp.usegalaxy.org/ ')  
    parser.add_argument('-k', '--apikey', dest="apikey",required=True, help="Your Galaxy API Key")  
    parser.add_argument('-w', '--wfl_dir', dest="wfl_dir",  required=True,  default="", help="Directory containing the workflows. If the directory doesn't exist, it will be created and the workflow downloaded.") 
    parser.add_argument('-v', '--wfl_version', dest="wfl_version",  required=False,  default="0.3.2", help="Optional: Specify which version of the workflow to run. Must be compatible with the sample yaml files (default: 0.3.2)")    
    parser.add_argument('-s', '--suffix', dest="suffix",  required=False,  default="", help="Optional: Specify a suffix for your run (e.g. 'v2.0' to name the job file wf4_mCteGun2_v2.0.yaml)") 
 
    args = parser.parse_args()


    if args.wfl_dir[-1]=="/":
        wfl_dir=args.wfl_dir
    else: 
         wfl_dir=args.wfl_dir+"/"

    if args.suffix!='':
        suffix_run='_'+suffix
    else:
        suffix_run=''

    path_script=str(pathlib.Path(__file__).parent.resolve())

    gi = GalaxyInstance(args.instance, args.apikey)


    Compatible_version=args.wfl_version
    workflow_name="Assembly-Hifi-only-VGP3"

    worfklow_path,release_number=function.get_worfklow(Compatible_version, workflow_name, wfl_dir)



    infos=pandas.read_csv(args.track_table, header=0, sep="\t" )
    infos = infos.fillna(value={'Invocation_wf1':'NA'}) 


    list_yml=[]
    list_res=[]
    commands=[]
    list_reports=[]
    list_invocation=[]


    for i,row in infos.iterrows():
        spec_name=row['Species']
        spec_id=row['Assembly']
        species_path="./"+spec_id+"/"
        if row['Invocation_wf1']=='NA':
            json_wf1=infos.iloc[i]['Results_wf1']
            if os.path.exists(json_wf1):
                wf1json=open(json_wf1)
                reswf1=json.load(wf1json)
                invocation_number=reswf1["tests"][0]["data"]['invocation_details']['details']['invocation_id']
                row.loc['Invocation_wf1']=invocation_number
            else:
                print("Skipped "+spec_id+": No Json or invocation number found")
                list_reports.append("NA")
                commands.append("NA")
                list_res.append("NA")
                list_yml.append("NA")
                continue
        else:
            invocation_number=row['Invocation_wf1']
        res_file=species_path+"invocations_json/wf3_"+spec_id+suffix_run+".json"
        yml_file=species_path+"job_files/wf3_"+spec_id+suffix_run+".yml"
        if os.path.exists(yml_file):
            print("Skipped "+spec_id+": Files and command already generated")
            list_reports.append(row['Wf1_Report'])
            commands.append(row['Wf3_Commands'])
            list_res.append(row['WF3_result_json'])
            list_yml.append(row['WF3_job_yml'])
            continue
        wf1_inv=gi.invocations.get(str(invocation_number))
        invocation_state=wf1_inv.summary()['populated_state']
        if invocation_state!='ok':
            print("Skipped "+spec_id+": Invocation incomplete, Status: "+invocation_state+", url: "+args.instance+"/workflows/invocations/"+invocation_number)
            list_reports.append("NA")
            commands.append("NA")
            list_res.append("NA")
            list_yml.append("NA")
            continue
            
        wf1_inv.__dict__['wrapped']['input_step_parameters']
        pacbio_collection=wf1_inv.__dict__['wrapped']['inputs'][0]['id']
        merylres_id=wf1_inv.__dict__['wrapped']['outputs']['Merged Meryl Database']['id']
        genomescope_linear_plot=wf1_inv.__dict__['wrapped']['outputs']['GenomeScope linear plot']['id']
        genomescope_summary=wf1_inv.__dict__['wrapped']['outputs']['GenomeScope summary']['id']
        genomescope_parameters=wf1_inv.__dict__['wrapped']['outputs']['GenomeScope Model Parameters']['id']
        wf1_inv.save_report_pdf(species_path+'reports/report_wf1_'+spec_id+suffix_run+'_'+invocation_number+'.pdf')
        list_reports.append(species_path+'reports/report_wf1_'+spec_id+suffix_run+'_'+invocation_number+'.pdf')
        str_elements=""


        list_yml.append(yml_file)
        list_res.append(res_file)
        history_id=wf1_inv.__dict__['history_id']
        cmd_line="planemo run  "+worfklow_path+" "+yml_file+" --engine external_galaxy --galaxy_url "+args.instance+" --galaxy_user_key $MAINKEY --simultaneous_uploads --history_id "+history_id+" --no_wait --test_output_json "+res_file+" &"
        commands.append(cmd_line)
        print(cmd_line)
        with open(path_script+"/templates/wf3_run.sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()
        filedata = filedata.replace('["species_name"]', spec_name )
        filedata = filedata.replace('["assembly_name"]', spec_id )
        filedata = filedata.replace('["Pacbio"]', pacbio_collection )
        filedata = filedata.replace('["read_db"]', merylres_id)
        filedata = filedata.replace('["summary"]', genomescope_summary)
        filedata = filedata.replace('["model_params"]', genomescope_parameters)
        with open(yml_file, 'w') as yaml_wf3:
            yaml_wf3.write(filedata)

    infos['Wf1_Report']=list_reports
    infos['WF3_job_yml']=list_yml
    infos['WF3_result_json']=list_res
    infos['Wf3_Commands']=commands
    infos['Invocation_wf3']='NA'

    infos.to_csv(args.track_table, sep='\t', header=False, index=False)


if __name__ == "__main__":
    main()
        