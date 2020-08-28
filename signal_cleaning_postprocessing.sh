#!/bin/bash

signal_path=/projects/covid_test/covid-19-signal
results_path=/projects/covid_test/covid-19-signal/result_dir
pangolin_path=/projects/covid_test/pangolin

# Samples that passed signal pipeline and post_processing step
sample_list=$(awk -F"," '{print $1}' ${signal_path}/sample_table.csv | awk NR!=1 )

# Check the samples, delete unsuccessful (signal) directories, combine summary reports and concat pangolin clades

echo "sample_id,read_pairs,genome_fraction,avgdepth_coverage,pangolin_clade,pangolearn_probability" > ${results_path}/Summarized_report.csv
for i in $results_path/*
do
	if [ -d "$i" ]; then
        	sra_run=$(basename $i)
		if [[ "$sample_list" =~ $sra_run ]]; then
                	summary=${i}/${sra_run}_sample.txt
			read_pairs="$(grep 'Raw Data (read pairs)' $summary | sed 's/.* //')"
			genome_fraction="$(grep 'Genome Fraction (%):' $summary | sed 's/.* //')"
			depth_cov="$(grep 'Average Depth of Coverage:' $summary | sed 's/.* //')"
			pangolin="$(grep ${sra_run} ${pangolin_path}/covid19_lineage_report.csv | cut -d, -f2,3)"
			echo "${sra_run},${read_pairs},${genome_fraction},${depth_cov},${pangolin}" >> ${results_path}/Summarized_report.csv
		else	
			echo "rm -r ${i}"
        	fi
    	fi
done
