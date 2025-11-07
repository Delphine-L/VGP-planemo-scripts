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
    parser.add_argument('-i', '--id', action='store_true', required=False, help='(Optional) Force treating profile values as workflow IDs. By default, the script auto-detects whether values are IDs or versions.')
    parser.add_argument('-v', '--version', action='store_true', required=False, help='(Optional) Force treating profile values as workflow versions. By default, the script auto-detects whether values are IDs or versions.')
    parser.add_argument('-r', '--resume', required=False, action='store_true',  help='Resume a previous run using the metadata json file produced at the end of the run_all.py script and found in the metadata directory.')
    parser.add_argument('--retry-failed', required=False, action='store_true',  help='When used with --resume, automatically retry any failed or cancelled invocations by launching them again.')
    parser.add_argument('--fetch-urls', required=False, action='store_true',  help='Fetch GenomeArk file URLs before running workflows. Use this when the input table only contains Species and Assembly columns (no file paths).')
    parser.add_argument('--sync-metadata', required=False, action='store_true',  help='Sync metadata with Galaxy histories: check all invocations, update metadata with latest status and invocation IDs, but do not launch any workflows. Useful for tidying up metadata after manual interventions or to capture background workflow completions.')
    parser.add_argument('--download-reports', required=False, action='store_true',  help='Download PDF reports for completed invocations when using --resume or --sync-metadata. Only works for invocations in terminal states (ok, failed, cancelled). This feature can be unreliable, so errors are logged but do not stop execution.')
    parser.add_argument('-q', '--quiet', required=False, action='store_true',  help='Quiet mode: only show warnings and errors, suppress informational messages.')
    args = parser.parse_args()

    # Initialize logging with quiet flag
    function.setup_logging(quiet=args.quiet)

    # Validate mutually exclusive options
    if args.resume and args.sync_metadata:
        raise SystemExit("Error: --resume and --sync-metadata are mutually exclusive. Use --resume to launch workflows, or --sync-metadata to only update metadata.")

    # Validate that --retry-failed is only used with --resume
    if args.retry_failed and not args.resume:
        raise SystemExit("Error: --retry-failed can only be used with --resume option.")

    # Validate that --fetch-urls is not used with --resume or --sync-metadata
    if args.fetch_urls and (args.resume or args.sync_metadata):
        raise SystemExit("Error: --fetch-urls cannot be used with --resume or --sync-metadata options.")

    # Validate that --download-reports is only used with --resume or --sync-metadata
    if args.download_reports and not (args.resume or args.sync_metadata):
        raise SystemExit("Error: --download-reports can only be used with --resume or --sync-metadata options.")

    # Fetch GenomeArk URLs if requested
    if args.fetch_urls:
        print("=" * 60)
        print("Fetching GenomeArk file URLs...")
        print("=" * 60)

        # Read input table (can have 2-4 columns: Species, Assembly, [Custom_Path], [Suffix])
        # Try to detect if the file has headers by checking the first line
        try:
            # First, peek at the file to see if it has headers
            with open(args.species, 'r') as f:
                first_line = f.readline().strip().split('\t')
                # Check if first line looks like a header (contains "Species" or "Assembly")
                has_header = any(col in ['Species', 'Assembly', 'Custom_Path', 'Suffix', 'Working_Assembly'] for col in first_line)

            # Read with appropriate header setting
            if has_header:
                infos = pandas.read_csv(args.species, header=0, sep="\t")
                print("Detected existing tracking table with headers - will fetch URLs for missing data")
            else:
                infos = pandas.read_csv(args.species, header=None, sep="\t")
        except Exception as e:
            raise SystemExit(f"Error reading input table {args.species}: {e}\n\nPlease ensure all rows have the same number of tab-separated columns.")

        # Handle optional columns - only rename if we read without headers
        if not has_header:
            if len(infos.columns) == 4:
                infos.rename(columns={0: 'Species', 1: 'Assembly', 2: 'Custom_Path', 3: 'Suffix'}, inplace=True)
                has_custom_path = True
                has_suffix = True
                print("Detected 4 columns (Species, Assembly, Custom_Path, Suffix)")
            elif len(infos.columns) == 3:
                infos.rename(columns={0: 'Species', 1: 'Assembly', 2: 'Custom_Path'}, inplace=True)
                has_custom_path = True
                has_suffix = False
                print("Detected optional third column for custom GenomeArk paths")
            elif len(infos.columns) == 2:
                infos.rename(columns={0: 'Species', 1: 'Assembly'}, inplace=True)
                has_custom_path = False
                has_suffix = False
            else:
                raise SystemExit(f"Error: When using --fetch-urls, the input table must have 2-4 columns (Species, Assembly, [Custom_Path], [Suffix]). Found {len(infos.columns)} columns.")
        else:
            # File already has headers, just detect which columns are present
            has_custom_path = 'Custom_Path' in infos.columns
            has_suffix = 'Suffix' in infos.columns
            print(f"Columns found: {', '.join(infos.columns)}")

        # Fetch URLs for each species
        list_hifi_urls = []
        list_hic_type = []
        list_hic_f_urls = []
        list_hic_r_urls = []

        for i, row in infos.iterrows():
            # Strip whitespace from all string columns
            species_name = str(row['Species']).strip()
            species_id = str(row['Assembly']).strip()

            # Get custom path if available and not empty
            custom_path = None
            if has_custom_path:
                cp_value = row['Custom_Path']
                if not pandas.isna(cp_value):
                    cp_stripped = str(cp_value).strip()
                    if cp_stripped:
                        custom_path = cp_stripped

            # Get suffix if available
            suffix = None
            if has_suffix:
                suffix_value = row['Suffix']
                if not pandas.isna(suffix_value):
                    suffix_stripped = str(suffix_value).strip()
                    if suffix_stripped:
                        suffix = suffix_stripped

            display_id = f"{species_id}_{suffix}" if suffix else species_id

            # Check if URLs already exist (when using existing tracking table)
            if 'Hifi_reads' in infos.columns and pandas.notna(row.get('Hifi_reads')) and str(row.get('Hifi_reads')).strip() != '' and str(row.get('Hifi_reads')) != 'NA':
                print(f"Skipping {display_id} - URLs already present")
                list_hifi_urls.append(row['Hifi_reads'])
                list_hic_type.append(row.get('HiC_Type', 'NA'))
                list_hic_f_urls.append(row.get('HiC_forward_reads', 'NA'))
                list_hic_r_urls.append(row.get('HiC_reverse_reads', 'NA'))
                continue

            print(f"Fetching URLs for {display_id} ({species_name})...")

            try:
                hifi_reads, hic_type, hic_forward, hic_reverse = get_urls(species_name, species_id, custom_path)
                list_hifi_urls.append(hifi_reads)
                list_hic_type.append(hic_type)
                list_hic_f_urls.append(hic_forward)
                list_hic_r_urls.append(hic_reverse)
                print(f"  ✓ Found {hic_type} Hi-C data")
            except Exception as e:
                print(f"  ✗ Error fetching URLs for {display_id}: {e}")
                raise SystemExit(f"Failed to fetch URLs for {display_id}. Please check species name and assembly ID.")

        # Add missing columns if they weren't in the input
        if not has_custom_path:
            infos['Custom_Path'] = ''
        if not has_suffix:
            infos['Suffix'] = ''

        # Create Working_Assembly column if it doesn't exist (used as unique key in metadata)
        if 'Working_Assembly' not in infos.columns:
            def make_working_assembly(row):
                assembly = str(row['Assembly']).strip()
                if 'Suffix' in row and row['Suffix']:
                    suffix = str(row['Suffix']).strip()
                    if suffix:
                        return f"{assembly}_{suffix}"
                return assembly
            infos['Working_Assembly'] = infos.apply(make_working_assembly, axis=1)

        # Update URLs in dataframe (only for rows we fetched)
        if not has_header or 'Hifi_reads' not in infos.columns:
            # New table or no existing URL columns - set all rows
            infos['Hifi_reads'] = list_hifi_urls
            infos['HiC_Type'] = list_hic_type
            infos['HiC_forward_reads'] = list_hic_f_urls
            infos['HiC_reverse_reads'] = list_hic_r_urls
        else:
            # Existing table with URL columns - update only rows we fetched
            for idx, (hifi, hic_t, hic_f, hic_r) in enumerate(zip(list_hifi_urls, list_hic_type, list_hic_f_urls, list_hic_r_urls)):
                infos.at[idx, 'Hifi_reads'] = hifi
                infos.at[idx, 'HiC_Type'] = hic_t
                infos.at[idx, 'HiC_forward_reads'] = hic_f
                infos.at[idx, 'HiC_reverse_reads'] = hic_r

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

    # Create metadata directory if it doesn't exist
    if not os.path.exists(profile_data['Metadata_directory']):
        os.makedirs(profile_data['Metadata_directory'])
        print(f"Created metadata directory: {profile_data['Metadata_directory']}")

    suffix_run,galaxy_instance=function.fix_parameters(args.suffix, profile_data['Galaxy_instance'])
    profile_data['Galaxy_instance']=galaxy_instance
    profile_data['Suffix']=suffix_run
    gi = GalaxyInstance(galaxy_instance, profile_data['Galaxy_key'])

    profile_data['path_script']=path_script

    if args.resume or args.sync_metadata:
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

        # Check currently active invocations
        for species_id in list_metadata.keys():
            if 'invocations' in list_metadata[species_id]:
                for workflow_key, invocation_id in list_metadata[species_id]['invocations'].items():
                    if invocation_id and invocation_id != 'NA':
                        try:
                            inv_details = gi.invocations.show_invocation(invocation_id)
                            state = inv_details.get('state', 'unknown')
                            if state in ['failed', 'cancelled']:
                                # For Workflow 0 failures, check if it's due to no mitochondrial data
                                error_detail = state
                                if workflow_key == 'Workflow_0':
                                    is_no_mito, mito_error_msg = function.check_mitohifi_failure(gi, invocation_id)
                                    error_detail = mito_error_msg

                                failed_invocations.append({
                                    'species': species_id,
                                    'workflow': workflow_key,
                                    'invocation': invocation_id,
                                    'state': state,
                                    'error_detail': error_detail
                                })
                        except Exception as e:
                            print(f"Warning: Could not check invocation {invocation_id} for {species_id} {workflow_key}: {e}")

            # Also check the failed_invocations structure
            if 'failed_invocations' in list_metadata[species_id]:
                for workflow_key, inv_list in list_metadata[species_id]['failed_invocations'].items():
                    for invocation_id in inv_list:
                        # Add to failed list (these are already known to be failed)
                        # For Workflow 0, try to get diagnostic info
                        error_detail = 'failed (marked)'
                        if workflow_key == 'Workflow_0':
                            try:
                                is_no_mito, mito_error_msg = function.check_mitohifi_failure(gi, invocation_id)
                                error_detail = mito_error_msg
                            except:
                                error_detail = 'failed (marked)'

                        failed_invocations.append({
                            'species': species_id,
                            'workflow': workflow_key,
                            'invocation': invocation_id,
                            'state': 'failed (marked)',
                            'error_detail': error_detail
                        })

        if failed_invocations:
            print(f"\n{'='*60}")
            if args.retry_failed:
                print("🔄 Found failed/cancelled invocations - will retry:")
            else:
                print("⚠  WARNING: Found failed/cancelled invocations:")
            print(f"{'='*60}")
            for failed in failed_invocations:
                error_info = failed.get('error_detail', failed['state'])
                print(f"  - {failed['species']} {failed['workflow']}: {error_info}")
                if failed['workflow'] == 'Workflow_0' and 'no mitochondrial' in error_info.lower():
                    print(f"    ℹ️  This is expected if the sample has no mitochondrial sequences")
                print(f"    (invocation: {failed['invocation']})")
            print(f"{'='*60}")

            if args.retry_failed:
                print("Resetting failed invocations to allow retry...\n")
                for failed in failed_invocations:
                    # Reset invocation to NA so it can be retried
                    list_metadata[failed['species']]['invocations'][failed['workflow']] = 'NA'
                    # Remove from failed_invocations list
                    if 'failed_invocations' in list_metadata[failed['species']]:
                        if failed['workflow'] in list_metadata[failed['species']]['failed_invocations']:
                            inv_list = list_metadata[failed['species']]['failed_invocations'][failed['workflow']]
                            if failed['invocation'] in inv_list:
                                inv_list.remove(failed['invocation'])
                            # Clean up empty lists
                            if not inv_list:
                                del list_metadata[failed['species']]['failed_invocations'][failed['workflow']]
                    print(f"  Reset {failed['species']} {failed['workflow']}")
                print("\nFailed workflows will be re-launched during this run.\n")
            else:
                print("These workflows will be skipped unless you re-run them manually or remove their invocation IDs from metadata.")
                print("Use --retry-failed flag to automatically retry failed invocations.\n")
        else:
            print("✓ No failed invocations found.\n")

        # Pre-fetch all invocations from histories to minimize API calls during threading
        suffix_run = profile_data['Suffix']
        if args.download_reports:
            print("\n📄 Report download enabled - will attempt to download PDF reports for completed invocations")
            print("   (This feature can be unreliable; errors are logged but won't stop execution)\n")
        function.batch_update_metadata_from_histories(gi, list_metadata, profile_data, suffix_run, download_reports=args.download_reports)

        # If sync-metadata mode, save and exit without launching workflows
        if args.sync_metadata:
            print("\n" + "="*60)
            print("Sync metadata mode: Metadata has been updated from Galaxy")
            print("="*60)
            print("\nSaving updated metadata...")

            # Save main metadata file
            with open(profile_data['Metadata_directory']+'metadata_run'+suffix_run+'.json', "w") as json_file:
                json.dump(list_metadata, json_file, indent=4)
            print(f"✓ Saved: {profile_data['Metadata_directory']}metadata_run{suffix_run}.json")

            # Clean up per-species metadata files (data now in main file)
            for species_id in list_metadata.keys():
                species_metadata_file = f"{profile_data['Metadata_directory']}metadata_{species_id}_run{suffix_run}.json"
                if os.path.exists(species_metadata_file):
                    os.remove(species_metadata_file)
                    print(f"✓ Cleaned up: {species_metadata_file}")

            print("\n" + "="*60)
            print("Metadata sync complete!")
            print("="*60)
            print("\nSummary of invocations found:")
            for species_id in list_metadata.keys():
                print(f"\n{species_id}:")
                for wf_key, inv_id in list_metadata[species_id].get('invocations', {}).items():
                    if inv_id and inv_id != 'NA':
                        print(f"  {wf_key}: {inv_id}")

            return  # Exit without processing workflows

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
        # Auto-detection mode is now the default behavior
        # The --id and --version flags are kept for backward compatibility but are optional
        if args.id and args.version:
            raise SystemExit("Error: Please select only one of the two options: --id or --version.")

        # Determine mode: if neither flag is specified, use auto-detection
        use_auto_detection = not args.id and not args.version

        if use_auto_detection:
            print("Auto-detecting workflow IDs and versions from profile...")
            print()

        if args.version or use_auto_detection:
            # Map base workflow names to their keys (handling haplotypes)
            workflow_base_keys = {
                "Workflow_1": "Workflow_1",
                "Workflow_0": "Workflow_0",
                "Workflow_4": "Workflow_4",
                "Workflow_8": "Workflow_8_hap1",  # Use hap1 as reference
                "Workflow_9": "Workflow_9_hap1"   # Use hap1 as reference
            }

            # Track if any workflows were uploaded (to update profile)
            profile_updated = False

            for base_key, ref_key in workflow_base_keys.items():
                if base_key in profile_data:
                    wfl_dir=function.fix_directory(path_script+"/workflows/")

                    # Use resolve_workflow to handle both IDs and versions
                    workflow_id, release_number, workflow_path = function.resolve_workflow(
                        gi,
                        profile_data[base_key],
                        dico_workflows[ref_key]['Name'],
                        wfl_dir
                    )

                    # If we got a new ID (from version upload), update the profile
                    if release_number is not None:  # This means we downloaded and uploaded
                        profile_data[base_key] = workflow_id
                        profile_updated = True

                    # Assign to all related keys
                    if base_key == "Workflow_8":
                        dico_workflows["Workflow_8_hap1"]['Path']=workflow_id
                        dico_workflows["Workflow_8_hap1"]['version']=release_number if release_number else 'NA'
                        dico_workflows["Workflow_8_hap2"]['Path']=workflow_id
                        dico_workflows["Workflow_8_hap2"]['version']=release_number if release_number else 'NA'
                    elif base_key == "Workflow_9":
                        dico_workflows["Workflow_9_hap1"]['Path']=workflow_id
                        dico_workflows["Workflow_9_hap1"]['version']=release_number if release_number else 'NA'
                        dico_workflows["Workflow_9_hap2"]['Path']=workflow_id
                        dico_workflows["Workflow_9_hap2"]['version']=release_number if release_number else 'NA'
                    else:
                        dico_workflows[base_key]['Path']=workflow_id
                        dico_workflows[base_key]['version']=release_number if release_number else 'NA'
                else:
                    mode_msg = "--version option" if args.version else "profile"
                    raise SystemExit("Missing option: "+base_key+" in profile. You need to provide a workflow version or ID for "+dico_workflows[ref_key]['Name']+".")

            # Handle optional PreCuration workflow
            if "Workflow_PreCuration" in profile_data:
                wfl_dir=function.fix_directory(path_script+"/workflows/")
                workflow_id, release_number, workflow_path = function.resolve_workflow(
                    gi,
                    profile_data["Workflow_PreCuration"],
                    dico_workflows["Workflow_PreCuration"]['Name'],
                    wfl_dir
                )

                if release_number is not None:
                    profile_data["Workflow_PreCuration"] = workflow_id
                    profile_updated = True

                dico_workflows["Workflow_PreCuration"]['Path']=workflow_id
                dico_workflows["Workflow_PreCuration"]['version']=release_number if release_number else 'NA'
                print("Pre-curation workflow enabled")
            else:
                print("Pre-curation workflow not specified - skipping")

            # Save updated profile if any workflows were uploaded
            if profile_updated and use_auto_detection:
                print("\n" + "="*60)
                print("Saving updated profile with workflow IDs...")
                # Create a backup of the original profile
                profile_backup = args.profile + ".bak"
                import shutil
                shutil.copy2(args.profile, profile_backup)
                print(f"✓ Backup created: {profile_backup}")

                # Save updated profile
                with open(args.profile, 'w') as f:
                    yaml.dump(profile_data, f, default_flow_style=False, sort_keys=False)
                print(f"✓ Profile updated with workflow IDs: {args.profile}")
                print("="*60 + "\n")
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
            # Strip whitespace from all string columns
            spec_name = str(infos.iloc[i]['Species']).strip()
            assembly_id = str(infos.iloc[i]['Assembly']).strip()

            # Use Working_Assembly as the key if it exists (for species with suffixes)
            # Otherwise fall back to Assembly for backward compatibility
            if 'Working_Assembly' in infos.columns:
                wa_value = infos.iloc[i]['Working_Assembly']
                if not pandas.isna(wa_value) and str(wa_value).strip() != '':
                    working_assembly = str(wa_value).strip()
                else:
                    working_assembly = assembly_id
            else:
                working_assembly = assembly_id

            list_metadata[working_assembly]={}
            list_metadata[working_assembly]['Assembly']=assembly_id  # Store original assembly ID
            list_metadata[working_assembly]['History_name']=working_assembly+suffix_run
            list_metadata[working_assembly]['Name']=spec_name

            # Store custom_path if it exists (for GenomeArk URL construction)
            if 'Custom_Path' in infos.columns and pandas.notna(infos.iloc[i]['Custom_Path']) and str(infos.iloc[i]['Custom_Path']).strip() != '':
                list_metadata[working_assembly]['Custom_Path'] = str(infos.iloc[i]['Custom_Path']).strip()
            else:
                list_metadata[working_assembly]['Custom_Path'] = ''

            hifi_col=infos.iloc[i]['Hifi_reads']
            if hifi_col=='NA':
                print('Warning: '+working_assembly+' has been skipped because it has no PacBio reads.')
                continue
            list_pacbio=hifi_col.split(',')
            list_metadata[working_assembly]['Hifi_reads']=list_pacbio
            species_path="./"+working_assembly+"/"
            list_metadata[working_assembly]['Path']=species_path

            hic_f_col=infos.iloc[i]['HiC_forward_reads']
            hic_r_col=infos.iloc[i]['HiC_reverse_reads']
            hic_type=infos.iloc[i]['HiC_Type']
            if type(hic_f_col)==float or type(hic_r_col)==float :
                print('Warning: '+working_assembly+' has been skipped because it is missing Hi-C reads.')
                continue
            hic_f=hic_f_col.split(',')
            hic_r=hic_r_col.split(',')
            list_metadata[working_assembly]['HiC_Type']=hic_type
            list_metadata[working_assembly]['HiC_forward_reads']=hic_f
            list_metadata[working_assembly]['HiC_reverse_reads']=hic_r


            os.makedirs(species_path, exist_ok=True)
            os.makedirs(species_path+"job_files/", exist_ok=True)
            os.makedirs(species_path+"invocations_json/", exist_ok=True)
            os.makedirs(species_path+"reports/", exist_ok=True)
            os.makedirs(species_path+"planemo_log/", exist_ok=True)

            list_metadata[working_assembly]["job_files"]={}
            list_metadata[working_assembly]["invocation_jsons"]={}
            list_metadata[working_assembly]["planemo_logs"]={}
            list_metadata[working_assembly]["reports"]={}
            list_metadata[working_assembly]["invocations"]={}
            list_metadata[working_assembly]["dataset_ids"]={}
            list_metadata[working_assembly]["history_id"]='NA'
            list_metadata[working_assembly]["taxon_id"]='NA'
            list_metadata[working_assembly]["failed_invocations"]={}

            for wkfl in dico_workflows.keys():
                list_metadata[working_assembly]["job_files"][wkfl]=species_path+'job_files/'+working_assembly+suffix_run+'_'+wkfl+'.yml'
                list_metadata[working_assembly]["invocation_jsons"][wkfl]=species_path+'invocations_json/'+working_assembly+suffix_run+'_'+wkfl+'.json'
                list_metadata[working_assembly]["planemo_logs"][wkfl]=species_path+"planemo_log/"+working_assembly+suffix_run+'_'+wkfl+'.log'
                list_metadata[working_assembly]["reports"][wkfl]=species_path+"reports/"+working_assembly+suffix_run+'_'+wkfl+'_report.pdf'
                list_metadata[working_assembly]["invocations"][wkfl]='NA'


    
## For Workflow 1
    for species_id in list_metadata.keys():
        str_elements=""
        spec_name=list_metadata[species_id]['Name']

        # Get original assembly ID and custom path for GenomeArk URLs
        assembly_id = list_metadata[species_id].get('Assembly', species_id)  # Fallback to species_id for backward compatibility
        custom_path = list_metadata[species_id].get('Custom_Path', '')
        genomeark_path_segment = '/' + custom_path if custom_path else ''

        if os.path.exists(list_metadata[species_id]["job_files"]["Workflow_1"]):
            print("Job file for Workflow 1 already generated for "+species_id)
            continue
        for i in list_metadata[species_id]['Hifi_reads']:
            name=re.sub(r"\.f(ast)?q(sanger)?\.gz","",i)
            # Use assembly_id (not species_id) in GenomeArk URL, with optional custom_path
            str_elements=str_elements+"\n  - class: File\n    identifier: "+name+"\n    path: gxfiles://genomeark/species/"+spec_name+"/"+assembly_id+genomeark_path_segment+"/genomic_data/pacbio_hifi/"+i+"\n    filetype: fastqsanger.gz"
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
        

