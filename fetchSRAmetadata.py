import sys
import time
import pandas as pd
from pysradb.sraweb import SRAweb

# Connecting SRAweb with
db = SRAweb()

# Reading the most recent file of NCBI search based on organism
# taxonomy (07 July, 2020)
SRA = pd.read_csv("Coronaviridae_runs_20201103.csv")

# Extracting distinct studies to reduce the search space
studies = SRA['sra_study'].nunique()
pd_data = []
for srp in sorted(studies.tolist()):
    try:
        # Fetching Metadata using NCBI API with flag detailed=TRUE to
        # fetch maximum metadata associated
        df = db.sra_metadata(srp, detailed=True)
        pd_data.append(df)
    except:
        sys.stderr.write("Error with {}\n".format(srp))
        time.sleep(0.5)
    time.sleep(0.5)
# Concatenating list of DataFrames into one with sorting=False
final_pd = pd.concat(pd_data, sort=False)

# Final DataFrame written to CSV file
final_pd.to_csv("COVID_Metadata_detailed_20201103.tsv", sep="\t", index=False)
