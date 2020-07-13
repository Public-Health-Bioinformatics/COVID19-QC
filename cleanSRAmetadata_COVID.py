import pandas as pd

Metadata = pd.read_csv("COVID_Metadata_detailed.tsv", sep='\t',
                       low_memory=False)
# Selecting unique studies to reduce observations for filtering.
Metadata_unique = Metadata.drop_duplicates(subset='study_accession',
                                           keep="last")

# Filtering based on Host = Human & homo sapiens
# AND
# Filtering only for SARS-CoV-2 in organism_name or sample_title
hosts = ['human', 'homo sapiens']
organism = ['coronavirus', 'cov2', 'covid', 'corona', '2019-nCoV',
            'SARS-CoV-2', 'nCov']
Filtered_Metadata = Metadata_unique[((Metadata_unique[
    'host'].str.contains(
    '|'.join(hosts), case=False, na=False)) &
                                     (Metadata_unique[
                                         'organism_name'].str.contains(
                                         '|'.join(organism), case=False,
                                         na=False)) |
                                     (Metadata_unique[
                                         'sample_title'].str.contains(
                                         '|'.join(organism), case=False,
                                         na=False)))]
# Getting the SRA run IDS back
Metadata_Cleaned = Metadata[Metadata['study_accession'].isin(
     Filtered_Metadata.study_accession)]

# Writing the Cleaned DataFrame to csv
Metadata_Cleaned.to_csv("COVID_Metadata_detailed_cleaned.tsv", sep="\t",
                        index=False)
