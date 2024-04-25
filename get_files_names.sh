
species_name=$1
species_id=$2
output=$3
pacbios=($(aws --no-sign-request s3 ls genomeark/species/${species_name}/${species_id}/genomic_data/pacbio_hifi/ | grep fastq | awk '{print $4}'))
HiC_forward=($(aws --no-sign-request s3 ls genomeark/species/${species_name}/${species_id}/genomic_data/arima/ | grep R1 | awk '{print $4}'))
HiC_reverse=($(aws --no-sign-request s3 ls genomeark/species/${species_name}/${species_id}/genomic_data/arima/ | grep R2 | awk '{print $4}'))
bionano=($(aws --no-sign-request s3 ls genomeark/species/${species_name}/${species_id}/genomic_data/bionano/ | grep cmap | grep DLE | awk '{print $4}'))

echo "${species_name}\t${species_id}\t${pacbios[*]}\t${HiC_forward[*]}\t${HiC_reverse[*]}\t${bionano}" >> $output