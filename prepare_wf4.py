#!/usr/bin/env python3


import json 
import sys
import argparse
import pandas
import re
import pathlib
import function


path_script=str(pathlib.Path(__file__).parent.resolve())

parser = argparse.ArgumentParser("prepare_wf4")
parser.add_argument("track_table", help="File containing the species and input files (Produced by prepare_wf1.sh)", type=str)
parser.add_argument("yaml", help="Prefix of the yaml file used to run WF4", type=str)

args = parser.parse_args()


### Get compatible workflow versions

Compatible_workflow="https://github.com/iwc-workflows/Assembly-Hifi-HiC-phasing-VGP4/archive/refs/tags/v0.3.8.zip"
path_compatible="Assembly-Hifi-HiC-phasing-VGP4-0.3.8/Assembly-Hifi-HiC-phasing-VGP4.ga"
archive_name="Assembly-Hifi-HiC-phasing-VGP4"
 
worfklow_name=function.get_worfklow(Compatible_workflow, path_compatible, archive_name)



infos=pandas.read_csv(args.track_table, header=None, sep="\t")

list_yml=[]
list_res=[]
commands=[]
list_histories=[]
list_genomescope=[]
list_invocation=[]
for i,row in infos.iterrows():
    json_wf1=infos.iloc[i][7]
    spec_name=infos.iloc[i][0]
    spec_id=infos.iloc[i][1]
    str_elements=""
    str_hic=""
    hic_f=infos.iloc[i][3].split(' ')
    hic_r=infos.iloc[i][4].split(' ')
    yml_file=args.yaml+"_wf4_"+spec_id+".yml"
    list_yml.append(yml_file)
    res_file="wf4_invocation_"+spec_id+".json"
    list_res.append(res_file)
    wf1json=open(json_wf1)
    reswf1=json.load(wf1json)
    hifi_upload= {key: value for key, value in reswf1["tests"][0]["data"]['invocation_details']['steps'].items() if 'Collection of Pacbio Data' in key}
    pacbio_collection=hifi_upload[list(hifi_upload.keys())[0]]['output_collections']['output']['id']
    jobs_list={key: value for key, value in reswf1["tests"][0]["data"]['invocation_details']['steps'].items() if  len(value['jobs'])>0}
    merylres_run={key: value['jobs'][0]['outputs'] for key, value in jobs_list.items() if  isinstance(value['jobs'][0]['command_version'], str) and 'groups_operations' in value['jobs'][0]['params'].keys()}
    merylres_id=merylres_run[list(merylres_run.keys())[0]]['read_db']['id']
    genomescope_run={key: value['jobs'][0]['outputs'] for key, value in jobs_list.items() if  isinstance(value['jobs'][0]['command_version'], str) and 'GenomeScope' in value['jobs'][0]['command_version'] }
    summary_id=genomescope_run[list(genomescope_run.keys())[0]]['summary']['id']
    model_id=genomescope_run[list(genomescope_run.keys())[0]]['model_params']['id']
    history_id=reswf1["tests"][0]["data"]['invocation_details']['details']['history_id']
    history_path="https://usegalaxy.org/histories/view?id="+history_id
    list_histories.append(history_path)
    invocation_path="https://usegalaxy.org/workflows/invocations/"+reswf1["tests"][0]["data"]['invocation_details']['details']['invocation_id']
    list_invocation.append(invocation_path)
    genomescope_view="https://usegalaxy.org/datasets/"+reswf1["tests"][0]["data"]['invocation_details']['steps']['12. Unnamed step']['outputs']['linear_plot']['id']+"/preview"
    list_genomescope.append(genomescope_view)
    if len(hic_f)!=len(hic_r):
        raise SystemExit("Number of Hi-C forward reads does not match number of Hi-C reverse reads. Check the table and correct if necessary.")
    for i in range(0,len(hic_f)):
        namef=re.sub(r"\.f(ast)?q(sanger)?\.gz","",hic_f[i])
        namer=re.sub(r"\.f(ast)?q(sanger)?\.gz","",hic_r[i])
        str_hic=str_hic+"\n  - class: Collection\n    type: paired\n    identifier: "+namef+"\n    elements:\n    - identifier: forward\n      class: File\n      path: gxfiles://genomeark/species/"+spec_name+"/"+spec_id+"/genomic_data/arima/"+hic_f[i]+"\n      filetype: fastqsanger.gz\n    - identifier: reverse\n      class: File\n      path: gxfiles://genomeark/species/"+spec_name+"/"+spec_id+"/genomic_data/arima/"+hic_r[i]+"\n      filetype: fastqsanger.gz"
    cmd_line="planemo run "+worfklow_name+" "+yml_file+" --engine external_galaxy --galaxy_url https://vgp.usegalaxy.org/ --galaxy_user_key $MAINKEY --history_id "+history_id+" --no_wait --test_output_json "+res_file+" &"
    commands.append(cmd_line)
    print(cmd_line)
    with open(path_script+"/wf4_run.sample.yaml", 'r') as sample_file:
        filedata = sample_file.read()
    filedata = filedata.replace('["Pacbio"]', pacbio_collection )
    filedata = filedata.replace('["hic"]', str_hic)
    filedata = filedata.replace('["species_name"]', spec_name )
    filedata = filedata.replace('["assembly_name"]', spec_id )
    filedata = filedata.replace('["read_db"]', merylres_id )
    filedata = filedata.replace('["summary"]', summary_id)
    filedata = filedata.replace('["model_params"]', model_id)
    with open(yml_file, 'w') as yaml_wf3:
        yaml_wf3.write(filedata)

infos[9]=list_yml
infos[10]=list_res
infos[11]=commands
infos.to_csv(args.track_table, sep='\t', header=False, index=False)

QC_frame=pandas.concat([infos[0],infos[1]],axis=1, keys=['Species', 'ID'])
QC_frame["History"]=list_histories
QC_frame["Invocation_WF2"]=list_invocation
QC_frame["Genomescope"]=list_genomescope
QC_frame.to_csv("QC_"+args.track_table, sep='\t', header=True, index=False)
