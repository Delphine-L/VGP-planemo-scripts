import pandas
import argparse
import re
import pathlib

path_script=str(pathlib.Path(__file__).parent.resolve())
print(path_script)
parser = argparse.ArgumentParser("prepare_wf3")
parser.add_argument("species", help="File containing the species and input files (Produced with get_files_names.sh)", type=str)
parser.add_argument("yaml", help="Prefix of the yaml file used to run WF1", type=str)

args = parser.parse_args()

infos=pandas.read_csv(args.species, header=None, sep="\t")
list_yml=[]
list_res=[]
commands=[]
for i,row in infos.iterrows():
    list_pacbio=infos.iloc[i][2].split(' ')
    spec_name=infos.iloc[i][0]
    spec_id=infos.iloc[i][1]
    str_elements=""
    yml_file=args.yaml+"_wf1_"+spec_id+".yml"
    list_yml.append(yml_file)
    res_file="wf1_invocation_"+spec_id+".json"
    list_res.append(res_file)
    for i in list_pacbio:
        name=re.sub(r"\.f(ast)?q(sanger)?\.gz","",i)
        str_elements=str_elements+"\n  - class: File\n    identifier: "+name+"\n    path: gxfiles://genomeark/species/"+spec_name+"/"+spec_id+"/genomic_data/pacbio_hifi/"+i+"\n    filetype: fastqsanger.gz"
    with open(path_script+"/wf1_run.sample.yaml", 'r') as sample_file:
        filedata = sample_file.read()
    filedata = filedata.replace('["Pacbio"]', str_elements )
    with open(yml_file, 'w') as yaml_wf1:
        yaml_wf1.write(filedata)
    cmd_line="planemo run kmer-profiling-hifi-VGP1.ga "+yml_file+" --engine external_galaxy --galaxy_url https://usegalaxy.org/ --galaxy_user_key $MAINKEY --history_name "+spec_id+" --no_wait --test_output_json "+res_file+" &"
    commands.append(cmd_line)
    print(cmd_line)
infos[6]=list_yml
infos[7]=list_res
infos[8]=commands
infos.to_csv("wf_run_"+args.species, sep='\t', header=False, index=False)