
import os
import requests
import zipfile
import json
import shutil
import argparse
import textwrap
from bioblend.galaxy import GalaxyInstance
import re
import pandas
import function


def main():

    parser = argparse.ArgumentParser(
                        prog='create_sample_from_worklow.py',
                        description='Create an empty job file from a workflow file',
                        usage='create_sample_from_worklow.py -w <Workflow file> -o <Output yaml file>  ',
                        formatter_class=argparse.RawTextHelpFormatter,
                        epilog=textwrap.dedent('''
                                            General outputs: 
                                            - <Output yaml file>: Template Yaml file for the workflow
                                            '''))

    parser.add_argument('-t', '--table', dest="track_table",required=True, help='File containing the species and input files')  
    parser.add_argument('-g','--galaxy_url',  required=True, help='Galaxy Url')
    parser.add_argument('-k','--APIkey', required=True, help='API key for this Galaxy instance')
    args = parser.parse_args()
 
    regexurl=r'(https?:\/\/)'
    if re.search(regexurl,args.galaxy_url):
        validurl=args.galaxy_url
    else:
        validurl='https://'+args.galaxy_url
        
    
    gi = GalaxyInstance(validurl, args.APIkey)
        
    infos=pandas.read_csv(args.track_table, header=0, sep="\t" )

    invocation_columns=[col for col  in list(infos.columns) if 'nvocation_' in col]

    dictionary_column_content={key: [] for key in invocation_columns}

    dict_na=dict.fromkeys(invocation_columns, 'NA')
    infos = infos.fillna(value=dict_na) 
    history_id_present=False
    if 'History_id' in list(infos.columns):
        history_id_present=True
    list_histories=[]

    for i,row in infos.iterrows():
        spec_id=row['Assembly']
        if history_id_present and not pandas.isna(row['History_id']):
            history_id=row['History_id']
            print("Using history id "+str(history_id)+" for assembly "+spec_id)
        else:
            result_wf1=row['Results_wf1'].split('/')[-1]
            history_name=result_wf1.replace('wf1_','').replace('.json','')
            print("Searching history for assembly "+spec_id+" with name "+history_name)
            history_list = gi.histories._get_histories(name=history_name)
            if len(history_list)>1:
                print("Warning: Multiple histories called "+history_name+". Most recent selected. If this is incorrect, replace the value in the column 'History_id' and rerun.")
                hist_times={ hist['id']: hist['update_time'] for hist in history}
                sorted_hists = sorted(hist_times.items(), key=lambda item: item[1],reverse=True)
                history_id=sorted_hists[0][0]
            else:
                history_id=history_list[0]['id']
        
        list_histories.append(history_id)
        invocations=gi.invocations.get_invocations(history_id=history_id)
        history_invocations={ invoc['id'] : gi.workflows.show_workflow(workflow_id=invoc["workflow_id"], instance=True)['name'] for invoc in invocations}

        subworkflows=[]
        for key in history_invocations.keys():
            invocation_steps=gi.invocations.show_invocation(key)['steps']
            #subworkflows.append([step['subworkflow_invocation_id'] for stp in invocation_steps if step['subworkflow_invocation_id'] ])
            for step in invocation_steps:
                subworkflows=subworkflows+[step['subworkflow_invocation_id'] for step in invocation_steps if step['subworkflow_invocation_id']]

        subworkflows=list(set(subworkflows))
        
        workflow_invocations={key:value for key,value in history_invocations.items() if key not in subworkflows}
        
        duplicates=function.find_duplicate_values(workflow_invocations)
        invocation_states={value: {key: gi.invocations.get_invocation_summary(str(key))['populated_state']} for key,value in workflow_invocations.items() if value not in duplicates.keys() }

        for i in duplicates.keys():
            list_invocations=duplicates[i]
            invocation_states[i]={invoc : gi.invocations.get_invocation_summary(str(invoc))['populated_state'] for invoc in list_invocations}

        failed_states=['failed','canceled']
        good_invocations={}
        for i in invocation_states.keys():
                good_invocations[i]={key:gi.invocations.show_invocation(key)['create_time']  for key,value in invocation_states[i].items() if value not in failed_states}

        clean_good_invocations={key: value for key, value in good_invocations.items() if value}
        failed_runs=[workflow for workflow in good_invocations.keys() if workflow not in clean_good_invocations.keys()]
        
        invocations_ids={}
        for wkfl in invocation_columns:
            if row[wkfl]=='NA':
                wkfl_name=wkfl.replace('Invocation_wf','VGP')
                print("Searching invocation for workflow "+wkfl_name+" for assembly "+spec_id)
                if "VGP8" in wkfl_name:
                    wkfl_name="VGP8"
                    haplotype=wkfl.split('_')[-1]
                run_names=[name for name in list(good_invocations.keys()) if wkfl_name in name]
                if len(run_names)>0:
                    list_invocs= [good_invocations[i] for i in  run_names]
                    merged_dict = {}
                    if  wkfl_name=="VGP8":
                        for d in list_invocs:
                            for key in d.keys():
                                if key not in merged_dict:
                                    invoc_hap=gi.invocations.show_invocation(key)['input_step_parameters']['Haplotype']['parameter_value'].replace('Haplotype ','hap')
                                    if haplotype==invoc_hap:
                                        merged_dict[key] = d[key]
                    else:
                        for d in list_invocs:
                            for key in d.keys():
                                if key not in merged_dict:
                                    merged_dict[key] = d[key]
                    invocations_ids[wkfl]=merged_dict

                    if len(invocations_ids[wkfl])>1:
                        print("Warning: Multiple valid invocations for workflow "+wkfl.replace('Invocation_wf','VGP')+" for assembly "+spec_id+". Most recent selected. If this is incorrect, replace the value in the column '"+wkfl+"'.")
                        sorted_invocations= sorted(invocations_ids[wkfl].items(), key=lambda item: item[1],reverse=True)
                        invocation_latests=sorted_invocations[0][0]
                    elif len(invocations_ids[wkfl])==1: 
                        invocation_latests=list(invocations_ids[wkfl].keys())[0]
                    else:
                        print("Warning: All invocations for workflow "+wkfl.replace('Invocation_wf','VGP')+" are in a failed state for assembly "+spec_id+".")
                        invocation_latests='NA'
                    dictionary_column_content[wkfl].append(invocation_latests)
                else:
                    dictionary_column_content[wkfl].append('NA')
            else:
                dictionary_column_content[wkfl].append(row[wkfl])

    for col in invocation_columns:
        infos[col]=dictionary_column_content[col]

    infos['History_id']=list_histories
    infos.to_csv(args.track_table, sep='\t', header=True, index=False)


if __name__ == "__main__":
    main()
        