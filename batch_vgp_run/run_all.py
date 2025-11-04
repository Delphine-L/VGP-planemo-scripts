import json
import argparse
import pandas
import re
import pathlib
import batch_vgp_run.function as function
from bioblend.galaxy import GalaxyInstance
import textwrap
import os
import yaml
from concurrent.futures import ThreadPoolExecutor
import threading
from batch_vgp_run.get_urls import get_urls



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
    parser.add_argument('-s', '--suffix', dest="suffix",  required=False,  default="", help="Optional: Specify a suffix for your run (e.g. 'v2.0' to name the job file wf1_mCteGun2_v2.0.yaml)") 
    parser.add_argument('-c', '--concurrent', required=False,  default="3", help="Number of concurrent processes to use (default: 3)") 
    parser.add_argument('-p', '--profile', dest="profile",  required=False,  default="", help="Path to the profile file. (See profile.sample.yaml for an example)") 
    parser.add_argument('-m', '--metadata_directory', required=False,  default="./", help="Path to the directory for run metadata.") 
    parser.add_argument('-i', '--id', action='store_true', required=False, help='The Profile contains the workflow IDs. This option is mutually exclusive with the --version option and will use the workflow in your Galaxy instance.')
    parser.add_argument('-v', '--version', action='store_true', required=False, help='The Profile contains the workflow versions. This option is mutually exclusive with the --id option and will download the workflows.')
    parser.add_argument('-r', '--resume', required=False, action='store_true',  help='Resume a previous run using the metadata json file produced at the end of the run_all.py script and found in the metadata directory.')
    parser.add_argument('--retry-failed', required=False, action='store_true',  help='When used with --resume, automatically retry any failed or cancelled invocations by launching them again.')
    parser.add_argument('--fetch-urls', required=False, action='store_true',  help='Fetch GenomeArk file URLs before running workflows. Use this when the input table only contains Species and Assembly columns (no file paths).')
    args = parser.parse_args()

    # Validate that --retry-failed is only used with --resume
    if args.retry_failed and not args.resume:
        raise SystemExit("Error: --retry-failed can only be used with --resume option.")

    # Validate that --fetch-urls is not used with --resume
    if args.fetch_urls and args.resume:
        raise SystemExit("Error: --fetch-urls cannot be used with --resume option.")

    # Fetch GenomeArk URLs if requested
    if args.fetch_urls:
        print("=" * 60)
        print("Fetching GenomeArk file URLs...")
        print("=" * 60)

        # Read input table (should have only Species and Assembly columns)
        infos = pandas.read_csv(args.species, header=None, sep="\t")
        infos.rename(columns={0: 'Species', 1: 'Assembly'}, inplace=True)

        # Check that table has exactly 2 columns
        if len(infos.columns) != 2:
            raise SystemExit("Error: When using --fetch-urls, the input table must have exactly 2 columns (Species, Assembly).")

        # Fetch URLs for each species
        list_hifi_urls = []
        list_hic_type = []
        list_hic_f_urls = []
        list_hic_r_urls = []

        for i, row in infos.iterrows():
            species_name = row['Species']
            species_id = row['Assembly']
            print(f"Fetching URLs for {species_id} ({species_name})...")

            try:
                hifi_reads, hic_type, hic_forward, hic_reverse = get_urls(species_name, species_id)
                list_hifi_urls.append(hifi_reads)
                list_hic_type.append(hic_type)
                list_hic_f_urls.append(hic_forward)
                list_hic_r_urls.append(hic_reverse)
                print(f"  ✓ Found {hic_type} Hi-C data")
            except Exception as e:
                print(f"  ✗ Error fetching URLs for {species_id}: {e}")
                raise SystemExit(f"Failed to fetch URLs for {species_id}. Please check species name and assembly ID.")

        # Add URLs to dataframe
        infos['Hifi_reads'] = list_hifi_urls
        infos['HiC_Type'] = list_hic_type
        infos['HiC_forward_reads'] = list_hic_f_urls
        infos['HiC_reverse_reads'] = list_hic_r_urls

        # Save tracking table
        output_table = "tracking_runs_" + os.path.basename(args.species)
        infos.to_csv(output_table, sep='\t', header=True, index=False)
        print(f"\n✓ GenomeArk URLs saved to: {output_table}")
        print("=" * 60)
        print()

        # Update args to use the new tracking table
        args.species = output_table

    path_script=str(pathlib.Path(__file__).parent.resolve())

    with open(args.profile, "r") as file:
        profile_data = yaml.safe_load(file)

    profile_data['Metadata_directory']=function.fix_directory(args.metadata_directory)
    suffix_run,galaxy_instance=function.fix_parameters(args.suffix, profile_data['Galaxy_instance'])
    profile_data['Galaxy_instance']=galaxy_instance
    profile_data['Suffix']=suffix_run
    gi = GalaxyInstance(galaxy_instance, profile_data['Galaxy_key'])

    profile_data['path_script']=path_script

    if args.resume:
        metadata_file=profile_data['Metadata_directory']+'metadata_run'+suffix_run+'.json'
        if not os.path.isfile(metadata_file):
            raise SystemExit("Error: The metadata file "+metadata_file+" does not exist. Please check the path and filename.")
        with open(metadata_file, "r") as json_file:
            list_metadata = json.load(json_file)

        # Check for per-species metadata files (more recent than main file)
        for species_id in list(list_metadata.keys()):
            species_metadata_file = f"{profile_data['Metadata_directory']}metadata_{species_id}_run{suffix_run}.json"
            if os.path.isfile(species_metadata_file):
                try:
                    with open(species_metadata_file, "r") as json_file:
                        species_metadata = json.load(json_file)
                    # Update with per-species data (more recent)
                    list_metadata[species_id] = species_metadata
                    print(f"Loaded per-species metadata for {species_id}")
                except Exception as e:
                    print(f"Warning: Could not load per-species metadata for {species_id}: {e}")

        # Load workflow metadata
        workflow_file=profile_data['Metadata_directory']+'metadata_workflow'+suffix_run+'.json'
        if not os.path.isfile(workflow_file):
            raise SystemExit("Error: The workflow metadata file "+workflow_file+" does not exist. Please check the path and filename.")
        with open(workflow_file, "r") as json_file:
            dico_workflows = json.load(json_file)

        # Check for failed invocations and warn user
        print("\nChecking status of existing invocations...")
        failed_invocations = []
        for species_id in list_metadata.keys():
            if 'invocations' in list_metadata[species_id]:
                for workflow_key, invocation_id in list_metadata[species_id]['invocations'].items():
                    if invocation_id and invocation_id != 'NA':
                        try:
                            inv_details = gi.invocations.show_invocation(invocation_id)
                            state = inv_details.get('state', 'unknown')
                            if state in ['failed', 'cancelled']:
                                failed_invocations.append({
                                    'species': species_id,
                                    'workflow': workflow_key,
                                    'invocation': invocation_id,
                                    'state': state
                                })
                        except Exception as e:
                            print(f"Warning: Could not check invocation {invocation_id} for {species_id} {workflow_key}: {e}")

        if failed_invocations:
            print(f"\n{'='*60}")
            if args.retry_failed:
                print("🔄 Found failed/cancelled invocations - will retry:")
            else:
                print("⚠  WARNING: Found failed/cancelled invocations:")
            print(f"{'='*60}")
            for failed in failed_invocations:
                print(f"  - {failed['species']} {failed['workflow']}: {failed['state']} (invocation: {failed['invocation']})")
            print(f"{'='*60}")

            if args.retry_failed:
                print("Resetting failed invocations to allow retry...\n")
                for failed in failed_invocations:
                    list_metadata[failed['species']]['invocations'][failed['workflow']] = 'NA'
                    print(f"  Reset {failed['species']} {failed['workflow']}")
                print("\nFailed workflows will be re-launched during this run.\n")
            else:
                print("These workflows will be skipped unless you re-run them manually or remove their invocation IDs from metadata.")
                print("Use --retry-failed flag to automatically retry failed invocations.\n")
        else:
            print("✓ No failed invocations found.\n")

    else:
        infos=pandas.read_csv(args.species, header=0, sep="\t")
        list_metadata={}
        dico_workflows={}
        dico_workflows["Workflow_1"]={}
        dico_workflows["Workflow_0"]={}
        dico_workflows["Workflow_4"]={}
        dico_workflows["Workflow_8_hap1"]={}
        dico_workflows["Workflow_8_hap2"]={}
        dico_workflows["Workflow_9_hap1"]={}
        dico_workflows["Workflow_9_hap2"]={}
        dico_workflows["Workflow_PreCuration"]={}
        dico_workflows["Workflow_1"]['Name']="kmer-profiling-hifi-VGP1"
        dico_workflows["Workflow_0"]['Name']="Mitogenome-assembly-VGP0"
        dico_workflows["Workflow_4"]['Name']="Assembly-Hifi-HiC-phasing-VGP4"
        dico_workflows["Workflow_8_hap1"]['Name']="Scaffolding-HiC-VGP8"
        dico_workflows["Workflow_8_hap2"]['Name']="Scaffolding-HiC-VGP8"
        dico_workflows["Workflow_9_hap1"]['Name']="Assembly-decontamination-VGP9"
        dico_workflows["Workflow_9_hap2"]['Name']="Assembly-decontamination-VGP9"
        dico_workflows["Workflow_PreCuration"]['Name']="PretextMap-Generation"
        if args.id and args.version:
            raise SystemExit("Error: Please select only one of the two options: --id or --version.")
        elif not args.version and not args.id:
            raise SystemExit("Error: Please select one of the two options: --version or --id.")
        elif args.version:
            # Map base workflow names to their keys (handling haplotypes)
            workflow_base_keys = {
                "Workflow_1": "Workflow_1",
                "Workflow_0": "Workflow_0",
                "Workflow_4": "Workflow_4",
                "Workflow_8": "Workflow_8_hap1",  # Use hap1 as reference
                "Workflow_9": "Workflow_9_hap1"   # Use hap1 as reference
            }

            for base_key, ref_key in workflow_base_keys.items():
                if base_key in profile_data:
                    wfl_dir=function.fix_directory(path_script+"/workflows/")
                    worfklow_path, release_number = function.get_worfklow(profile_data[base_key], dico_workflows[ref_key]['Name'], wfl_dir)

                    # Assign to all related keys
                    if base_key == "Workflow_8":
                        dico_workflows["Workflow_8_hap1"]['Path']=worfklow_path
                        dico_workflows["Workflow_8_hap1"]['version']=release_number
                        dico_workflows["Workflow_8_hap2"]['Path']=worfklow_path
                        dico_workflows["Workflow_8_hap2"]['version']=release_number
                    elif base_key == "Workflow_9":
                        dico_workflows["Workflow_9_hap1"]['Path']=worfklow_path
                        dico_workflows["Workflow_9_hap1"]['version']=release_number
                        dico_workflows["Workflow_9_hap2"]['Path']=worfklow_path
                        dico_workflows["Workflow_9_hap2"]['version']=release_number
                    else:
                        dico_workflows[base_key]['Path']=worfklow_path
                        dico_workflows[base_key]['version']=release_number
                else:
                    raise SystemExit("Missing option: "+base_key+" in profile. If you select the --version option, you need to provide a workflow version for "+dico_workflows[ref_key]['Name']+".")

            # Handle optional PreCuration workflow
            if "Workflow_PreCuration" in profile_data:
                wfl_dir=function.fix_directory(path_script+"/workflows/")
                worfklow_path, release_number = function.get_worfklow(profile_data["Workflow_PreCuration"], dico_workflows["Workflow_PreCuration"]['Name'], wfl_dir)
                dico_workflows["Workflow_PreCuration"]['Path']=worfklow_path
                dico_workflows["Workflow_PreCuration"]['version']=release_number
                print("Pre-curation workflow enabled")
            else:
                print("Pre-curation workflow not specified - skipping")
        elif args.id:
            # Map base workflow names to their keys (handling haplotypes)
            workflow_base_keys = {
                "Workflow_1": "Workflow_1",
                "Workflow_0": "Workflow_0",
                "Workflow_4": "Workflow_4",
                "Workflow_8": "Workflow_8_hap1",
                "Workflow_9": "Workflow_9_hap1"
            }

            for base_key, ref_key in workflow_base_keys.items():
                if base_key in profile_data:
                    worfklow_path=profile_data[base_key]
                    wfl_info=gi.workflows.show_workflow(worfklow_path)
                    wfl_name=wfl_info['name']
                    short_hand=base_key.split('_')[-1]
                    if short_hand not in wfl_name:
                        raise SystemExit("Error: The workflow ID provided does not correspond to the "+base_key+" workflow. Please check the ID.")

                    # Assign to all related keys
                    if base_key == "Workflow_8":
                        dico_workflows["Workflow_8_hap1"]['Path']=worfklow_path
                        dico_workflows["Workflow_8_hap1"]['version']='NA'
                        dico_workflows["Workflow_8_hap2"]['Path']=worfklow_path
                        dico_workflows["Workflow_8_hap2"]['version']='NA'
                    elif base_key == "Workflow_9":
                        dico_workflows["Workflow_9_hap1"]['Path']=worfklow_path
                        dico_workflows["Workflow_9_hap1"]['version']='NA'
                        dico_workflows["Workflow_9_hap2"]['Path']=worfklow_path
                        dico_workflows["Workflow_9_hap2"]['version']='NA'
                    else:
                        dico_workflows[base_key]['Path']=worfklow_path
                        dico_workflows[base_key]['version']='NA'
                else:
                    raise SystemExit("Missing option: "+base_key+" in profile. If you select the --id option, you need to provide a workflow ID.")

            # Handle optional PreCuration workflow
            if "Workflow_PreCuration" in profile_data:
                worfklow_path = profile_data["Workflow_PreCuration"]
                wfl_info = gi.workflows.show_workflow(worfklow_path)
                dico_workflows["Workflow_PreCuration"]['Path'] = worfklow_path
                dico_workflows["Workflow_PreCuration"]['version'] = 'NA'
                print("Pre-curation workflow enabled")
            else:
                print("Pre-curation workflow not specified - skipping")

        with open(profile_data['Metadata_directory']+'metadata_workflow'+suffix_run+'.json', "w") as json_file:	
            json.dump(dico_workflows,json_file , indent=4)
            
        for i, _ in infos.iterrows():
            spec_name=infos.iloc[i]['Species']
            spec_id=infos.iloc[i]['Assembly']
            list_metadata[spec_id]={}
            list_metadata[spec_id]['History_name']=spec_id+suffix_run
            list_metadata[spec_id]['Name']=spec_name
            hifi_col=infos.iloc[i]['Hifi_reads']
            if hifi_col=='NA':
                print('Warning: '+spec_id+' has been skipped because it has no PacBio reads.')
                continue
            list_pacbio=hifi_col.split(',')
            list_metadata[spec_id]['Hifi_reads']=list_pacbio
            species_path="./"+spec_id+"/"
            list_metadata[spec_id]['Path']=species_path

            hic_f_col=infos.iloc[i]['HiC_forward_reads']
            hic_r_col=infos.iloc[i]['HiC_reverse_reads']
            hic_type=infos.iloc[i]['HiC_Type']
            if type(hic_f_col)==float or type(hic_r_col)==float :
                print('Warning: '+spec_id+' has been skipped because it is missing Hi-C reads.')
                continue
            hic_f=hic_f_col.split(',')
            hic_r=hic_r_col.split(',')
            list_metadata[spec_id]['HiC_Type']=hic_type
            list_metadata[spec_id]['HiC_forward_reads']=hic_f
            list_metadata[spec_id]['HiC_reverse_reads']=hic_r


            os.makedirs(species_path, exist_ok=True)
            os.makedirs(species_path+"job_files/", exist_ok=True)
            os.makedirs(species_path+"invocations_json/", exist_ok=True)
            os.makedirs(species_path+"reports/", exist_ok=True)
            os.makedirs(species_path+"planemo_log/", exist_ok=True)

            list_metadata[spec_id]["job_files"]={}
            list_metadata[spec_id]["invocation_jsons"]={}
            list_metadata[spec_id]["planemo_logs"]={}
            list_metadata[spec_id]["reports"]={}
            list_metadata[spec_id]["invocations"]={}
            list_metadata[spec_id]["dataset_ids"]={}
            list_metadata[spec_id]["history_id"]='NA'
            list_metadata[spec_id]["taxon_id"]='NA'

            for wkfl in dico_workflows.keys():
                list_metadata[spec_id]["job_files"][wkfl]=species_path+'job_files/'+spec_id+suffix_run+'_'+wkfl+'.yml'
                list_metadata[spec_id]["invocation_jsons"][wkfl]=species_path+'invocations_json/'+spec_id+suffix_run+'_'+wkfl+'.json'
                list_metadata[spec_id]["planemo_logs"][wkfl]=species_path+"planemo_log/"+spec_id+suffix_run+'_'+wkfl+'.log'
                list_metadata[spec_id]["reports"][wkfl]=species_path+"reports/"+spec_id+suffix_run+'_'+wkfl+'_report.pdf'
                list_metadata[spec_id]["invocations"][wkfl]='NA'


    
## For Workflow 1
    for species_id in list_metadata.keys():
        str_elements=""
        spec_name=list_metadata[species_id]['Name']
        if os.path.exists(list_metadata[species_id]["job_files"]["Workflow_1"]):
            print("Job file for Workflow 1 already generated for "+species_id)
            continue
        for i in list_metadata[species_id]['Hifi_reads']:
            name=re.sub(r"\.f(ast)?q(sanger)?\.gz","",i)
            str_elements=str_elements+"\n  - class: File\n    identifier: "+name+"\n    path: gxfiles://genomeark/species/"+spec_name+"/"+species_id+"/genomic_data/pacbio_hifi/"+i+"\n    filetype: fastqsanger.gz"
        with open(path_script+"/templates/wf1_run.sample.yaml", 'r') as sample_file:
            filedata = sample_file.read()
        filedata = filedata.replace('["Pacbio"]', str_elements )
        filedata = filedata.replace('["species_name"]', spec_name )
        filedata = filedata.replace('["assembly_name"]', species_id )
        with open(list_metadata[species_id]["job_files"]["Workflow_1"], 'w') as yaml_wf1:
            yaml_wf1.write(filedata)
                
    ## Parallelize species processing using threads
    print("Processing species:", list(list_metadata.keys()))

    # Use a lock to prevent race conditions when updating shared data
    results_lock = threading.Lock()
    results_status = {}

    # Use ThreadPoolExecutor to run species in parallel
    with ThreadPoolExecutor(max_workers=int(args.concurrent)) as executor:
        # Submit all species for processing
        # Note: Each thread creates its own Galaxy instance inside the wrapper
        futures = [
            executor.submit(
                function.process_species_wrapper,
                species_id,
                list_metadata,
                profile_data,
                dico_workflows,
                results_lock,
                results_status,
                args.resume  # Pass resume flag to enable/disable history invocation search
            )
            for species_id in list_metadata.keys()
        ]

        # Wait for all to complete and collect results
        for future in futures:
            species_id, status, error = future.result()
            if status == "error":
                print(f"Species {species_id} failed: {error}")

    print(f"\n{'='*60}")
    print("All species processed")
    print(f"{'='*60}\n")
    print(f"Results summary: {results_status}")

    with open(profile_data['Metadata_directory']+'results_run'+suffix_run+'.json', "w") as json_file:
        json.dump(results_status, json_file, indent=4)

    with open(profile_data['Metadata_directory']+'metadata_run'+suffix_run+'.json', "w") as json_file:
        json.dump(list_metadata, json_file, indent=4)






""" 
        with open(yml_file, 'w') as yaml_wf1:
            yaml_wf1.write(filedata)
        cmd_line="planemo run "+worfklow_path+" "+yml_file+" --engine external_galaxy --galaxy_url "+galaxy_instance+" --simultaneous_uploads --check_uploads_ok --galaxy_user_key $MAINKEY --simultaneous_uploads --check_uploads_ok --history_name "+spec_id+suffix_run+" --no_wait --test_output_json "+res_file+" > "+log_file+" 2>&1 &"
        commands.append(cmd_line)
        print(cmd_line)
        
 """




if __name__ == "__main__":
    main()
        

