import sys
import time
import pandas as pd
from pysradb.sraweb import SRAweb
import numpy as np


def filter_previous_run(df):
    """
    This method filters datasets before fetching metadata based on
    last update. Provided a file from previous extract of NCBI
    SARS-CoV-2 resources https://www.ncbi.nlm.nih.gov/sars-cov-2/
    :param dataframe of new accession ids - df:
    :return list of new ids to be fetched - studies:
    """
    # Reading the previous version of COVID-19 (07 July, 2020)
    prev_accs = pd.read_csv("COVID_SRA_07July.csv")

    # Finding the common between both
    common = df.merge(prev_accs, on=["sra_study"])
    studies = [x for x in common['sra_study'].unique().tolist() if x
               not in
               prev_accs['sra_study'].unique().tolist()]
    # Returning the list of new studies to be fetched
    return studies


def filter_metadata(df):
    """
    This method filters datasets based on 3 criteria (host, library
    strategy and sequencing instrument) with defined values below. If
    required, one can change the values of each below.
    :param dataframe with detailed metadata for each SRA study - df:
    :return cleaned metadata dataframe - metadata_cleaned:
    """
    # Moving search space from all ids to unique study ids
    metadata_unique = df.drop_duplicates(subset='study_accession',
                                         keep="last")
    # Defining the filters
    hosts = ['human', 'homo sapiens']
    instrument = ['Illumina MiSeq', 'Illumina iSeq 100',
                  'Illumina NovaSeq 6000', 'Illumina HiSeq 2500',
                  'NextSeq 500', 'NextSeq 550']
    # Applying the filtration
    filtered_metadata = metadata_unique[(((metadata_unique[
        'host scientific name'].str.contains(
        '|'.join(hosts), case=False, na=False))) & (metadata_unique[
        'library_strategy'].str.contains(
        'AMPLICON', case=False, na=False)) & (metadata_unique[
        'instrument'].str.contains(
        '|'.join(instrument), case=False, na=False)))]
    # A new dataframe that contains filtered metadata
    metadata_cleaned = df[df['study_accession'].isin(
        filtered_metadata.study_accession)]
    # Reading the dataframe with filtered metadata
    return metadata_cleaned


# Connecting SRAweb with
db = SRAweb()

# Reading the most recent file of COVID-19 (03 November, 2020)
SRA = pd.read_csv("~/Downloads/Coronaviridae_runs_head.csv")

# Flag for if a previously fetched fie exists. Set 'previous_flag' = 1
# if previous version exists to run the function - filter_previous_run
previous_flag = 0
if previous_flag == 1:
    new_studies = filter_previous_run(df=SRA)
    search_space = new_studies
else:
    search_space = SRA['sra_study'].unique().tolist()

pd_data = []
for srp in search_space:
    try:
        # Fetching Metadata using NCBI API with flag detailed=TRUE to
        # fetch maximum metadata associated
        df = db.sra_metadata(srp, detailed=True)
        df.columns = map(str.lower, df.columns)
        df = df.replace('N/A', np.nan, regex=True)
        df = df.groupby(level=0, axis=1).first()
        pd_data.append(df)
    except:
        sys.stderr.write("Error with {}\n".format(srp))
        time.sleep(0.5)
    time.sleep(0.5)

# Concatenating list of DataFrames into one with sorting=False
full_pd = pd.concat(pd_data, sort=False)

# Calling the filter_metadata function to filter based on set criteria
final_pd = filter_metadata(df=full_pd)

# Writing the final filtered metadata dataframe to TSV file
final_pd.to_csv("COVID_Metadata_detailed_20201103.tsv", sep="\t",
                index=False)
