#!/usr/bin/env python3

import json
import argparse
import textwrap
from bioblend.galaxy import GalaxyInstance
import os


def main():
    parser = argparse.ArgumentParser(
        prog='download_reports',
        description='Download workflow reports for completed invocations',
        usage='download_reports.py -p <profile.yaml> -m <metadata_directory> [OPTIONS]',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=textwrap.dedent('''
            This script reads metadata from run_all.py and downloads PDF reports
            for all completed workflow invocations.

            Reports are saved to the paths specified in the metadata file.
            Only invocations with state='ok' will have their reports downloaded.
        '''))

    parser.add_argument('-p', '--profile', dest="profile", required=True,
                        help='Path to the profile file (same as used with run_all.py)')
    parser.add_argument('-m', '--metadata_directory', required=False, default="./",
                        help="Path to the directory containing metadata files (default: ./)")
    parser.add_argument('-s', '--suffix', dest="suffix", required=False, default="",
                        help="Optional: Specify the suffix used in your run (e.g. 'v2.0')")
    parser.add_argument('--skip-existing', action='store_true', required=False,
                        help='Skip downloading reports that already exist')
    parser.add_argument('--species', required=False, default=None,
                        help='Optional: Only download reports for a specific species (assembly ID)')

    args = parser.parse_args()

    # Load profile to get Galaxy credentials
    import yaml
    with open(args.profile, "r") as file:
        profile_data = yaml.safe_load(file)

    galaxy_instance = profile_data['Galaxy_instance']
    galaxy_key = profile_data['Galaxy_key']
    gi = GalaxyInstance(galaxy_instance, galaxy_key)

    # Fix suffix format
    if args.suffix and not args.suffix.startswith('_'):
        suffix_run = '_' + args.suffix
    else:
        suffix_run = args.suffix

    # Fix metadata directory
    metadata_dir = args.metadata_directory
    if not metadata_dir.endswith('/'):
        metadata_dir += '/'

    # Load metadata file
    metadata_file = metadata_dir + 'metadata_run' + suffix_run + '.json'
    if not os.path.isfile(metadata_file):
        raise SystemExit(f"Error: The metadata file {metadata_file} does not exist. Please check the path and filename.")

    with open(metadata_file, "r") as json_file:
        list_metadata = json.load(json_file)

    print(f"Loaded metadata for {len(list_metadata)} species")
    print(f"Galaxy instance: {galaxy_instance}\n")

    # Track download statistics
    total_invocations = 0
    downloaded = 0
    skipped_existing = 0
    skipped_incomplete = 0
    skipped_no_invocation = 0
    errors = 0

    # Process each species
    for species_id in list_metadata.keys():
        # Skip if user specified a specific species
        if args.species and species_id != args.species:
            continue

        print(f"\n{'='*60}")
        print(f"Processing {species_id}")
        print(f"{'='*60}")

        species_data = list_metadata[species_id]

        # Check if this species has invocations
        if 'invocations' not in species_data:
            print(f"  No invocations found for {species_id}")
            continue

        # Process each workflow invocation
        for workflow_key, invocation_id in species_data['invocations'].items():
            total_invocations += 1

            # Skip if no invocation exists
            if not invocation_id or invocation_id == 'NA':
                print(f"  {workflow_key}: No invocation ID found - skipping")
                skipped_no_invocation += 1
                continue

            # Get report destination path from metadata
            if 'reports' not in species_data or workflow_key not in species_data['reports']:
                print(f"  {workflow_key}: No report path found in metadata - skipping")
                skipped_no_invocation += 1
                continue

            report_path = species_data['reports'][workflow_key]

            # Skip if report already exists and user requested skip-existing
            if args.skip_existing and os.path.exists(report_path):
                print(f"  {workflow_key}: Report already exists - skipping")
                skipped_existing += 1
                continue

            try:
                # Check invocation state
                invocation_state = gi.invocations.get_invocation_summary(str(invocation_id))['populated_state']

                if invocation_state != 'ok':
                    print(f"  {workflow_key}: Invocation incomplete (state: {invocation_state}) - skipping")
                    skipped_incomplete += 1
                    continue

                # Ensure directory exists
                report_dir = os.path.dirname(report_path)
                if report_dir:
                    os.makedirs(report_dir, exist_ok=True)

                # Download report
                print(f"  {workflow_key}: Downloading report...")
                gi.invocations.get_invocation_report_pdf(str(invocation_id), file_path=report_path)
                print(f"  {workflow_key}: ✓ Report saved to {report_path}")
                downloaded += 1

            except Exception as e:
                print(f"  {workflow_key}: ✗ Error downloading report: {e}")
                errors += 1

    # Print summary
    print(f"\n{'='*60}")
    print("Download Summary")
    print(f"{'='*60}")
    print(f"Total invocations checked: {total_invocations}")
    print(f"Reports downloaded: {downloaded}")
    print(f"Skipped (already exists): {skipped_existing}")
    print(f"Skipped (incomplete): {skipped_incomplete}")
    print(f"Skipped (no invocation): {skipped_no_invocation}")
    print(f"Errors: {errors}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
