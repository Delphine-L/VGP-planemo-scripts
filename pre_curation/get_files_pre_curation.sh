input_tab=$1
output_tab=$2

touch $output_tab

while IFS=$'\t' read -r -a myArray
do
repo=${myArray[2]}
species_id=${myArray[1]}
species_name=${myArray[0]}
ass_type=${myArray[3]}
hic_tech=${myArray[4]}
hic_tech=$(sed 's/[[:space:]]*$//' <<< "$hic_tech")
assembly_repo="${repo}assembly_vgp_${ass_type_trimmed}_2.0"

if [ "${ass_type}" == "HiC" ]; then
	hap1_name="hap1"
	hap2_name="hap2"
elif [ "${ass_type}" == "trio" ]; then
	hap1_name="pat"
	hap2_name="mat"
elif [ "${ass_type}" == "standard" ]; then
	hap1_name="pri"
	hap2_name="alt"
else
	echo "Incorrect assembly method name. Correct inputs are : standard, trio, HiC "
	exit 1
fi

pacbios=($(aws --no-sign-request s3 ls ${repo}genomic_data/pacbio_hifi/ | grep fastq | awk '{print $4}'))
HiC_forward=($(aws --no-sign-request s3 ls ${repo}genomic_data/${hic_tech}/ | awk '{print $4}'| grep -e R1 -e _1 -e "\.1"))
HiC_reverse=($(aws --no-sign-request s3 ls ${repo}genomic_data/${hic_tech}/ | awk '{print $4}'| grep -e R2 -e _2 -e "\.2"))
hap1=${repo}assembly_vgp_${ass_type}_2.0/$(aws --no-sign-request s3 ls ${repo}assembly_vgp_${ass_type}_2.0/ | grep $hap1_name | grep fasta | awk '{print $4}')
hap2=${repo}assembly_vgp_${ass_type}_2.0/$(aws --no-sign-request s3 ls ${repo}assembly_vgp_${ass_type}_2.0/ | grep $hap2_name | grep fasta | awk '{print $4}')
echo "${species_name}\t${species_id}\t${ass_type}\t${hic_tech}\t${pacbios[*]}\t${HiC_forward[*]}\t${HiC_reverse[*]}\t${hap1}\t${hap2}"  >> $output_tab
done <  $input_tab