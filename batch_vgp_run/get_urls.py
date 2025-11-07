import subprocess
from io import StringIO
import pandas as pd
import re
import argparse
import textwrap


def add_species(species_name, species_id, table, custom_path=None, suffix=None):
	hifi_reads, hic_type, hic_forward, hic_reverse = get_urls(species_name, species_id, custom_path)

	# Create working assembly ID
	working_assembly = f"{species_id}_{suffix}" if suffix and suffix.strip() else species_id

	new_row = pd.DataFrame({
		'Species': [species_name],
		'Assembly': [species_id],
		'Custom_Path': [custom_path if custom_path else ''],
		'Suffix': [suffix if suffix else ''],
		'Working_Assembly': [working_assembly],
		'Hifi_reads': [hifi_reads],
		'HiC_Type': [hic_type],
		'HiC_forward_reads': [hic_forward],
		'HiC_reverse_reads': [hic_reverse]
	})
	result = pd.concat([table, new_row], axis=0)
	return result


def get_urls(species_name, species_id, custom_path=None):
	"""
	Fetch URLs for HiFi and HiC data from GenomeArk.

	Args:
		species_name (str): Species name (e.g., "Homo_sapiens")
		species_id (str): Assembly ID (e.g., "GCA_000001405.15")
		custom_path (str, optional): Custom subdirectory path between assembly_id and genomic_data.
			For example, "somatic" or "gametic" for species with non-standard directory structure.
			If None, uses the standard path: {assembly_id}/genomic_data/
			If provided, uses: {assembly_id}/{custom_path}/genomic_data/

	Returns:
		tuple: (hifi_reads, hic_type, hic_forward, hic_reverse)
	"""
	# Construct base path with optional custom subdirectory
	if custom_path and custom_path.strip():
		# Custom path provided - use it
		base_path = f"genomeark/species/{species_name}/{species_id}/{custom_path.strip()}/genomic_data/"
		print(f"  Using custom path: {base_path}")
	else:
		# Standard path
		base_path = f"genomeark/species/{species_name}/{species_id}/genomic_data/"

	command_genomic_data = f"aws --no-sign-request s3 ls {base_path}"
	data_type = subprocess.run(command_genomic_data.split(), capture_output=True, text=True, check=True)

	if 'arima' in data_type.stdout:
		hic_type = "arima"
		command_hic = f"aws --no-sign-request s3 ls {base_path}arima/ "
	elif 'dovetail' in data_type.stdout:
		hic_type = "dovetail"
		command_hic = f"aws --no-sign-request s3 ls {base_path}dovetail/ "
	else:
		raise SystemExit(f"No Hi-C folder (arima or dovetail) found in {base_path}")

	command_hifi = f"aws --no-sign-request s3 ls {base_path}pacbio_hifi/ "
	res_cmd_hifi=subprocess.run(command_hifi.split(), capture_output=True, text=True, check=True)
	res_cmd_hic=subprocess.run(command_hic.split(), capture_output=True, text=True, check=True)
	list_hifi=res_cmd_hifi.stdout.split('\n')
	list_hic=res_cmd_hic.stdout.split('\n')
	list_hifi= [ i for i in res_cmd_hifi.stdout.split('\n') if re.search(r'\.f(ast)?q(sanger)?\.gz$',i) ]
	list_hic= [ i for i in res_cmd_hic.stdout.split('\n') if re.search(r'\.f(ast)?q(sanger)?\.gz$',i) ]
	list_hic_f= [ i for i in list_hic if re.search(r'R1',i) ]
	list_hic_r= [ i for i in list_hic if re.search(r'R2',i) ]
	if len(list_hifi)==0:
		print('Warning: No Hifi reads found for '+species_id+'. Please verify the species name and assembly ID.')
		hifi_reads="NA"
	else:
		res_table_hifi=pd.read_table(StringIO("\n".join(list_hifi)),sep=r'\s+',header=None)
		hifi_reads = ",".join(res_table_hifi[3])
	if len(list_hic_f)==0 or len(list_hic_r)==0:
		hic_reverse="NA"
		hic_forward="NA"
	else:
		# Check that forward and reverse reads are properly paired
		if len(list_hic_f) != len(list_hic_r):
			raise SystemExit(f"Error: Number of Hi-C forward reads ({len(list_hic_f)}) does not match number of reverse reads ({len(list_hic_r)}) for {species_id}. Found:\n  Forward (R1): {len(list_hic_f)} files\n  Reverse (R2): {len(list_hic_r)} files")

		res_table_hic_f=pd.read_table(StringIO("\n".join(list_hic_f)),sep=r'\s+',header=None)
		res_table_hic_f=res_table_hic_f.sort_values(by=3)
		res_table_hic_r=pd.read_table(StringIO("\n".join(list_hic_r)),sep=r'\s+',header=None)
		res_table_hic_r=res_table_hic_r.sort_values(by=3)
		hic_forward= ",".join(res_table_hic_f[3])
		hic_reverse = ",".join(res_table_hic_r[3])

		# Verify pairing by comparing base filenames (handles R1/R2, _1/_2, .1/.2, etc.)
		# Replace common forward/reverse indicators with a placeholder
		# Pattern captures separators and preserves them to avoid matching digits elsewhere
		forward_bases = [re.sub(r'([_\.])([Rr])?1([_\.])', r'\1PAIR\3', f) for f in res_table_hic_f[3]]
		reverse_bases = [re.sub(r'([_\.])([Rr])?2([_\.])', r'\1PAIR\3', f) for f in res_table_hic_r[3]]
		if forward_bases != reverse_bases:
			print(f"Warning: Hi-C read pairs for {species_id} may not be properly matched. Please verify filenames:")
			for f, r in zip(res_table_hic_f[3], res_table_hic_r[3]):
				print(f"  {f} <-> {r}")

	return hifi_reads,hic_type,hic_forward,hic_reverse


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
	parser.add_argument('-t', '--table', required=True, help='Tabulated file containing: species name (column 1), assembly id (column 2), optional custom path (column 3), optional suffix (column 4)')
	group = parser.add_argument_group("Add a species to the table","Use the following options to add species to a tracking table. The table must be a table generated previously by this tool.")
	group.add_argument('--add', action='store_true', required=False, help='Add new species to the table')
	group.add_argument('-s','--species',  required=False, help='Species Name')
	group.add_argument('-a','--assembly', required=False, help='Assembly ID')
	group.add_argument('-c','--custom-path', required=False, help='Optional: Custom subdirectory path (e.g., "somatic", "gametic") for species with non-standard GenomeArk directory structure')
	group.add_argument('-x','--suffix', required=False, help='Optional: Suffix to distinguish multiple entries with same assembly ID (e.g., "somatic", "gametic"). Creates working ID as {assembly}_{suffix}')
	args = parser.parse_args()

	if args.add:
		infos = pd.read_csv(args.table, header=0, sep="\t")
		if args.species and args.assembly:
			custom_path = getattr(args, 'custom_path', None)
			suffix = getattr(args, 'suffix', None)

			display_id = f"{args.assembly}_{suffix}" if suffix else args.assembly
			print(f"Adding: {args.species} ({display_id})")

			if custom_path:
				print(f"  Using custom path: {custom_path}")
			if suffix:
				print(f"  Using suffix: {suffix}")

			infos = add_species(args.species, args.assembly, infos, custom_path, suffix)
			infos.to_csv(args.table, sep='\t', header=True, index=False)
		elif args.species:
			raise SystemExit("Missing option: -a. If you select the --add option, you need to provide an assembly id.")
		elif args.assembly:
			raise SystemExit("Missing option: -s. If you select the --add option, you need to provide a species name.")

	else:
		infos = pd.read_csv(args.table, header=None, sep="\t")
		list_hifi_urls = []
		list_hic_type = []
		list_hic_f_urls = []
		list_hic_r_urls = []

		# Check columns: Species, Assembly, [Custom_Path], [Suffix]
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
			raise SystemExit(f"Error: Input table must have 2, 3, or 4 columns (Species, Assembly, [Custom_Path], [Suffix]). Found {len(infos.columns)} columns.")

		for i, row in infos.iterrows():
			# Strip whitespace from all string columns
			species_name = str(row['Species']).strip()
			species_id = str(row['Assembly']).strip()

			# Get custom path if available and not empty
			custom_path = None
			if has_custom_path:
				cp_value = row['Custom_Path']
				if not pd.isna(cp_value):
					cp_stripped = str(cp_value).strip()
					if cp_stripped:
						custom_path = cp_stripped

			# Get suffix if available
			suffix = None
			if has_suffix:
				suffix_value = row['Suffix']
				if not pd.isna(suffix_value):
					suffix_stripped = str(suffix_value).strip()
					if suffix_stripped:
						suffix = suffix_stripped

			display_id = f"{species_id}_{suffix}" if suffix else species_id
			print(f"Fetching URLs for {display_id} ({species_name})...")
			hifi_reads, hic_type, hic_forward, hic_reverse = get_urls(species_name, species_id, custom_path)
			list_hifi_urls.append(hifi_reads)
			list_hic_type.append(hic_type)
			list_hic_f_urls.append(hic_forward)
			list_hic_r_urls.append(hic_reverse)

		# Add missing columns if they weren't in the input
		if not has_custom_path:
			infos['Custom_Path'] = ''
		if not has_suffix:
			infos['Suffix'] = ''

		# Create Working_Assembly column (used as unique key in metadata)
		def make_working_assembly(row):
			assembly = str(row['Assembly']).strip()
			if 'Suffix' in row and row['Suffix']:
				suffix = str(row['Suffix']).strip()
				if suffix:
					return f"{assembly}_{suffix}"
			return assembly
		infos['Working_Assembly'] = infos.apply(make_working_assembly, axis=1)

		infos['Hifi_reads'] = list_hifi_urls
		infos['HiC_Type'] = list_hic_type
		infos['HiC_forward_reads'] = list_hic_f_urls
		infos['HiC_reverse_reads'] = list_hic_r_urls

		infos.to_csv("tracking_runs_"+args.table, sep='\t', header=True, index=False)


if __name__ == "__main__":
	main()
