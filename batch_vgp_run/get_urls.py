import subprocess
import sys
from io import StringIO
import pandas as pd
import re
import argparse
import re
import pathlib
import textwrap
import os

        

def add_species(species_name,species_id,table):
    hifi_reads,hic_forward,hic_reverse=get_urls(species_name,species_id)
    new_row=pd.DataFrame({'Species': [species_name], 'Assembly': [species_id], 'Hifi_reads': [hifi_reads], 'HiC_forward_reads': [hic_forward], 'HiC_reverse_reads': [hic_reverse]})
    result=pd.concat([table, new_row], axis=0)
    return result


def get_urls(species_name,species_id):
    command_genomic_data="aws --no-sign-request s3 ls genomeark/species/"+species_name+"/"+species_id+"/genomic_data/ "
    data_type=subprocess.run(command_genomic_data.split(), capture_output=True, text=True, check=True)
    if 'arima' in data_type.stdout:
        command_hic="aws --no-sign-request s3 ls genomeark/species/"+species_name+"/"+species_id+"/genomic_data/arima/ "
    elif 'dovetail' in data_type.stdout:
        command_hic="aws --no-sign-request s3 ls genomeark/species/"+species_name+"/"+species_id+"/genomic_data/dovetail/ "
    else:
        raise SystemExit("No Hi-C folder (arima or dovetail) found in genomeark/species/"+species_name+"/"+species_id+"/genomic_data/")
    command_hifi="aws --no-sign-request s3 ls genomeark/species/"+species_name+"/"+species_id+"/genomic_data/pacbio_hifi/ "
    res_cmd_hifi=subprocess.run(command_hifi.split(), capture_output=True, text=True, check=True)
    res_cmd_hic=subprocess.run(command_hic.split(), capture_output=True, text=True, check=True)
    list_hifi=res_cmd_hifi.stdout.split('\n')
    list_hic=res_cmd_hic.stdout.split('\n')
    list_hifi= [ i for i in res_cmd_hifi.stdout.split('\n') if re.search(r'fastq.gz$',i) ]
    list_hic= [ i for i in res_cmd_hic.stdout.split('\n') if re.search(r'fastq.gz$',i) ]
    list_hic_f= [ i for i in list_hic if re.search(r'R1',i) ]
    list_hic_r= [ i for i in list_hic if re.search(r'R2',i) ]
    if len(list_hifi)==0:
        print('Warning: No Hifi reads found for '+species_id+'. Please verify the species name and assembly ID.')
        hifi_reads="NA"
    else:
        res_table_hifi=pd.read_table(StringIO("\n".join(list_hifi)),sep=r'\s+',header=None)
        hifi_reads = ",".join(res_table_hifi[3])
    if len(list_hic_f)==0 or len(list_hic_f)==0:
        hic_reverse="NA"
        hic_forward="NA"
    else:
        res_table_hic_f=pd.read_table(StringIO("\n".join(list_hic_f)),sep=r'\s+',header=None)
        res_table_hic_f=res_table_hic_f.sort_values(by=3)
        res_table_hic_r=pd.read_table(StringIO("\n".join(list_hic_r)),sep=r'\s+',header=None)
        res_table_hic_r=res_table_hic_r.sort_values(by=3)
        hic_forward= ",".join(res_table_hic_f[3])
        hic_reverse = ",".join(res_table_hic_r[3])
    return hifi_reads,hic_forward,hic_reverse


def main():

    parser = argparse.ArgumentParser(
                        prog='get_urls.',
                        description='After running wf1, download the qc and prepare the job files and command line to run wf4',
                        usage='get_urls.py -t <Table with Species and Assembly ID> --add -s <Species Name> -a <Species ID>',
                        formatter_class=argparse.RawTextHelpFormatter,
                        epilog=textwrap.dedent('''
                                            New table: 
                                            - tracking_runs_{table}: A table with the paths to genomic data for each species. 
                                            Add a species: 
                                            - {table}: The input table with the added species.
                                            '''))
    parser.add_argument('-t', '--table', required=True, help='Tabulated file containing the species name (column 1) and assembly id (column 2) with no space')
    group = parser.add_argument_group("Add a species to the table","Use the following options to add species to a tracking table. The table must be a table generated previously by this tool.")
    group.add_argument('--add', action='store_true', required=False, help='Add new species to the table')
    group.add_argument('-s','--species',  required=False, help='Species Name')
    group.add_argument('-a','--assembly', required=False, help='Assembly ID')
    args = parser.parse_args()

    
    path_script=str(pathlib.Path(__file__).parent.resolve())

    


    if args.add:
        infos=pd.read_csv(args.table, header=0, sep="\t")
        if args.species and args.assembly :
            print("Add: "+args.species)
            infos=add_species(args.species,args.assembly,infos)
            infos.to_csv(args.table, sep='\t', header=True, index=False)
        elif args.species:
            raise SystemExit("Missing option: -a. If you select the --add option, you need to provide an assembly id.")
        elif args.species:
            raise SystemExit("Missing option: -s. If you select the --add option, you need to provide a species name.")

    else:
        infos=pd.read_csv(args.table, header=None, sep="\t")
        list_hifi_urls=[]
        list_hic_f_urls=[]
        list_hic_r_urls=[]
        infos.rename(columns={0: 'Species', 1: 'Assembly'}, inplace=True)

        for i,row in infos.iterrows():
            species_name=row['Species']
            species_id=row['Assembly']
            hifi_reads,hic_forward,hic_reverse=get_urls(species_name,species_id)
            list_hifi_urls.append(hifi_reads)
            list_hic_f_urls.append(hic_forward)
            list_hic_r_urls.append(hic_reverse)
        infos['Hifi_reads']=list_hifi_urls
        infos['HiC_forward_reads']=list_hic_f_urls
        infos['HiC_reverse_reads']=list_hic_r_urls

        infos.to_csv("tracking_runs_"+args.table, sep='\t', header=True, index=False)


if __name__ == "__main__":
    main()
