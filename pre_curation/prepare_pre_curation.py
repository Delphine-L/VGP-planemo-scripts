import pandas
import argparse
import re
import pathlib
import sys

path_script=str(pathlib.Path(__file__).parent.resolve())
parser = argparse.ArgumentParser("prepare_pre_curation")
parser.add_argument("-s","--species", nargs=1,  help="File containing the species and input files (Produced with get_files_pre_curation.sh)", type=str)
parser.add_argument("-g","--galaxy_url", nargs=1, help="Url for the target Galaxy instance. E.g. https://vgp.usegalaxy.org/", type=str)
parser.add_argument("-d","--destination", nargs="?", help="Target folder for outputs. Must exist. If not specified, uses the current directory.",default='./', type=str)
parser.add_argument("-a","--api_key", nargs='?', help="Name of the shell variable containing your Galaxy API key. Default: MAINKEY", default='MAINKEY', type=str)



args = parser.parse_args()
infos=pandas.read_csv(args.species[0], header=None, sep="\t")
destination=args.destination
instance=args.galaxy_url[0]
api_key=args.api_key
list_yml=[]
list_res=[]
commands=[]
for i,row in infos.iterrows():
    ass_tech=infos.iloc[i][2]
    hic_tech=infos.iloc[i][3]
    list_pacbio=str(infos.iloc[i][4]).split(' ')
    list_hicfor=str(infos.iloc[i][5]).split(' ')
    list_hicrev=str(infos.iloc[i][6]).split(' ')
    hap1=infos.iloc[i][7]
    hap2=infos.iloc[i][8]
    repo_pattern=r"(.+)assembly"
    match = re.search(repo_pattern, hap2)
    aws_repo=match.group(1)
    spec_id=infos.iloc[i][1]
    if len(list_hicfor)!=len(list_hicrev):
        print("Please verify the hi-C files for species"+spec_id+": the number of reverse and forward files are different")
        sys.exit()
    str_elements=""
    hic_elements=""
    res_file="pre_curation_invocation_"+spec_id+".json"
    yml_file="pre_curation_"+spec_id+".yaml"
    list_res.append(res_file)
    list_yml.append(yml_file)
    if ass_tech=="trio":
        hap1_suf="pat_H1"
        hap2_suf="mat_H2"
        sechap="true"     
    elif ass_tech=="HiC":
        hap1_suf="H1"
        hap2_suf="H2"
        sechap="true"    
    elif ass_tech=="standard":
        hap1_suf="H1"
        hap2_suf="H2"
        sechap="false"   
    if hic_tech=="arima":
        trimming="true"
    else:
        trimming="false"
    for i in list_pacbio:
        name=re.sub(r"\.f(ast)?q(sanger)?\.gz","",i)
        str_elements=str_elements+"\n  - class: File\n    identifier: \""+name+"\"\n    path: gxfiles://"+aws_repo+"genomic_data/pacbio_hifi/"+i+"\n    filetype: fastqsanger.gz"
    for j in range(0,len(list_hicrev)):
        namehic=re.sub(r"\.f(ast)?q(sanger)?\.gz","",list_hicrev[j])
        hic_elements=hic_elements+"\n    - class: Collection\n      type: paired\n      identifier: \""+namehic+"\"\n      elements:\n      - identifier: forward\n        class: File\n        path: gxfiles://"+aws_repo+"genomic_data/"+hic_tech+"/"+list_hicfor[j]+"\n      - identifier: reverse\n        class: File\n        path: gxfiles://"+aws_repo+"genomic_data/"+hic_tech+"/"+list_hicrev[j]
    with open(path_script+"/pre_curation_run.sample.yaml", 'r') as sample_file:
        filedata = sample_file.read()
    filedata = filedata.replace('["hifi"]', str_elements )
    filedata = filedata.replace('["hic"]', hic_elements )
    filedata = filedata.replace('["hap_1"]', "gxfiles://"+hap1 )
    filedata = filedata.replace('["hap_2"]', "gxfiles://"+hap2 )    
    filedata = filedata.replace('["sechap"]', sechap )   
    filedata = filedata.replace('["trimhic"]', trimming )  
    filedata = filedata.replace('["h1suf"]', hap1_suf )  
    filedata = filedata.replace('["h2suf"]', hap2_suf )    
    with open(destination+"/"+yml_file, 'w') as yaml_wf1:
        yaml_wf1.write(filedata)
    cmd_line="planemo run "+path_script+"/PretextMap_Generation.ga "+destination+"/"+yml_file+" --engine external_galaxy --galaxy_url "+instance+" --galaxy_user_key $"+api_key+" --history_name "+spec_id+" --no_wait --test_output_json "+destination+"/"+res_file+" &"
    commands.append(cmd_line)
    print(cmd_line)
infos[9]=list_yml
infos[10]=list_res
infos[11]=commands
infos.to_csv(destination+"/"+"wf_run_"+args.species, sep='\t', header=False, index=False)
