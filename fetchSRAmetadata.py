import sys
import time
import pandas as pd
from pysradb.sraweb import SRAweb
import numpy as np

# Connecting SRAweb with
db = SRAweb()

# Reading the most recent file of COVID-19 (03 November, 2020)
SRA = pd.read_csv("~/Downloads/Coronaviridae_runs_head.csv")

# Reading the previous version of COVID-19 (07 July, 2020)
# prev_accs = pd.read_csv("COVID_SRA_07July.csv")

# Finding the common between both
# common = SRA.merge(prev_accs, on=["sra_study"])
# studies = [x for x in SRA['sra_study'].unique().tolist() if x not in
#           prev_accs['sra_study'].unique().tolist()]


pd_data = []
for srp in SRA['sra_study'].unique().tolist():
    print(srp)
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
new_pd = pd.concat(pd_data, sort=False)
new_pd.to_csv("COVID_Metadata_detailed_20201103.tsv", sep="\t",
              index=False)
'''
print(new_pd.columns)
print(new_pd.shape)
print(new_pd['country'].unique())

# Reading the old metadata file into dataframe
prev_metadata = pd.read_csv("COVID_Metadata_detailed.tsv", sep='\t',
                            low_memory=False)
prev_metadata.columns = map(str.lower, prev_metadata.columns)
prev_metadata = prev_metadata.replace('N/A', np.nan, regex=True)
prev_metadata = prev_metadata.groupby(level=0, axis=1).first()

print(prev_metadata.shape)
print(prev_metadata.columns)

prev_metadata.to_csv("COVID_Metadata_detailed_20200707.tsv",
                    sep="\t", index=False)

final_pd = pd.concat([prev_metadata, new_pd], sort=False)
print(final_pd.columns)
print(final_pd)
# Final DataFrame written to CSV file
new_pd.to_csv("COVID_Metadata_detailed_20201103.tsv", sep="\t",
              index=False)
'''