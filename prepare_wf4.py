import json 
import sys
import argparse
import pandas
import re
import pathlib

path_script=str(pathlib.Path(__file__).parent.resolve())

parser = argparse.ArgumentParser("prepare_wf4")
parser.add_argument("species", help="File containing the species and input files (Produced by prepare_wf1.sh)", type=str)
parser.add_argument("yaml", help="Prefix of the yaml file used to run WF4", type=str)

args = parser.parse_args()

infos=pandas.read_csv(args.species, header=None, sep="\t")

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
    str_hic_f=""
    str_hic_r=""
    list_pacbio=infos.iloc[i][2].split(' ')
    hic_f=infos.iloc[i][3].split(' ')
    hic_r=infos.iloc[i][4].split(' ')
    yml_file=args.yaml+"_wf4_"+spec_id+".yml"
    list_yml.append(yml_file)
    res_file="wf4_invocation_"+spec_id+".json"
    list_res.append(res_file)
    print(json_wf1)
    wf1json=open(json_wf1)
    reswf1=json.load(wf1json)
    history_id=reswf1["tests"][0]["data"]['invocation_details']['details']['history_id']
    history_path="https://usegalaxy.org/histories/view?id="+history_id
    list_histories.append(history_path)
    invocation_path="https://usegalaxy.org/workflows/invocations/"+reswf1["tests"][0]["data"]['invocation_details']['details']['invocation_id']
    list_invocation.append(invocation_path)
    genomescope_view="https://usegalaxy.org/datasets/"+reswf1["tests"][0]["data"]['invocation_details']['steps']['6. Unnamed step']['outputs']['linear_plot']['id']+"/preview"
    list_genomescope.append(genomescope_view)
    for i in list_pacbio:
        name=re.sub(r"\.f(ast)?q(sanger)?\.gz","",i)
        str_elements=str_elements+"\n  - class: File\n    identifier: "+name+"\n    path: gxfiles://genomeark/species/"+spec_name+"/"+spec_id+"/genomic_data/pacbio_hifi/"+i+"\n    filetype: fastqsanger.gz"
    for i in hic_f:
        name=re.sub(r"\.f(ast)?q(sanger)?\.gz","",i)
        str_hic_f=str_hic_f+"\n  - class: File\n    identifier: "+name+"\n    path: gxfiles://genomeark/species/"+spec_name+"/"+spec_id+"/genomic_data/arima/"+i+"\n    filetype: fastqsanger.gz"
    for i in hic_r:
        name=re.sub(r"\.f(ast)?q(sanger)?\.gz","",i)
        str_hic_r=str_hic_r+"\n  - class: File\n    identifier: "+name+"\n    path: gxfiles://genomeark/species/"+spec_name+"/"+spec_id+"/genomic_data/arima/"+i+"\n    filetype: fastqsanger.gz"
    cmd_line="planemo run Assembly-Hifi-HiC-phasing-VGP4.ga "+yml_file+" --engine external_galaxy --galaxy_url https://usegalaxy.org/ --galaxy_user_key $MAINKEY --history_id "+history_id+" --no_wait --test_output_json "+res_file+" &"
    commands.append(cmd_line)
    print(cmd_line)
    with open(path_script+"/wf4_run.sample.yaml", 'r') as sample_file:
        filedata = sample_file.read()
    filedata = filedata.replace('["Pacbio"]', str_elements )
    filedata = filedata.replace('["hic_f"]', str_hic_f )
    filedata = filedata.replace('["hic_r"]', str_hic_r )
    filedata = filedata.replace('["read_db"]', reswf1["tests"][0]["data"]['invocation_details']['steps']['4. Unnamed step']['outputs']['read_db']['id'])
    filedata = filedata.replace('["summary"]', reswf1["tests"][0]["data"]['invocation_details']['steps']['6. Unnamed step']['outputs']['summary']['id'])
    filedata = filedata.replace('["model_params"]', reswf1["tests"][0]["data"]['invocation_details']['steps']['6. Unnamed step']['outputs']['model_params']['id'])
    with open(yml_file, 'w') as yaml_wf3:
        yaml_wf3.write(filedata)

infos[9]=list_yml
infos[10]=list_res
infos[11]=commands
infos.to_csv(args.species, sep='\t', header=False, index=False)

QC_frame=pandas.concat([infos[0],infos[1]],axis=1, keys=['Species', 'ID'])
QC_frame["History"]=list_histories
QC_frame["Invocation_WF2"]=list_invocation
QC_frame["Genomescope"]=list_genomescope
QC_frame.to_csv("QC_"+args.species, sep='\t', header=True, index=False)
