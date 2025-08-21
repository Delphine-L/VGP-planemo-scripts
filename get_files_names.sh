#!/bin/sh 

Help()
{
    # Display help

    printf "\nCommand: sh get_files_names.sh -s <Species_name> -a <Species ID> -o <Output table> \n\n"
    echo "-h Prints help."
    echo "-s Species name with no spaces (e.g. Canis_lupus)"
    echo "-a Assembly ID. eg mCanLup2"
    echo "-o Output file containing the path to the data for further processing. This command will create it if it doesn't exit or add at the end of an existing one)"
    printf "\n"
}



id_flag=false
species_flag=false
out_flag=false
missing_dep=0


while getopts "hs:a:o:" option; do
    case $option in 
        h) #display Help
            Help
            exit;;
        s) #Pass Species Name
            species_name=$OPTARG; species_flag=true;;
        a) #Pass assembly ID
            species_id=$OPTARG; id_flag=true;;
        o) #Pass the output name
            output=$OPTARG; out_flag=true;;
        :) # Handle missing arguments for options requiring them
            echo "Error: Option -$OPTARG requires an argument." >&2
            exit 1
            ;;
        \?) # Handle invalid options
            echo "Error: Invalid option -$OPTARG." >&2
            exit 1
            ;;
    esac
done



if ! $species_flag 
then
    echo "Error: Missing Option. A species name must be specified (-s)" >&2
    missing_dep=1
fi

    
if ! $id_flag 
then
    echo "Error: Missing Option. An assembly ID must be specified (-a) " >&2
    missing_dep=1
fi

    
if ! $out_flag 
then
    echo "Error: Missing Option. An output must be specified (-o)" >&2
    missing_dep=1
fi

if [ "$missing_dep" -gt 0 ]; then
    exit 1
fi
    


pacbios=($(aws --no-sign-request s3 ls genomeark/species/${species_name}/${species_id}/genomic_data/pacbio_hifi/ | grep "fastq.gz$" | awk '{print $4}'))
HiC_forward=($(aws --no-sign-request s3 ls genomeark/species/${species_name}/${species_id}/genomic_data/arima/ | grep R1 | awk '{print $4}'))
HiC_reverse=($(aws --no-sign-request s3 ls genomeark/species/${species_name}/${species_id}/genomic_data/arima/ | grep R2 | awk '{print $4}'))
# bionano=($(aws --no-sign-request s3 ls genomeark/species/${species_name}/${species_id}/genomic_data/bionano/ | grep cmap | grep DLE | awk '{print $4}'))

# echo "${species_name}\t${species_id}\t${pacbios[*]}\t${HiC_forward[*]}\t${HiC_reverse[*]}\t${bionano}" >> $output
echo "${species_name}\t${species_id}\t${pacbios[*]}\t${HiC_forward[*]}\t${HiC_reverse[*]}" >> $output