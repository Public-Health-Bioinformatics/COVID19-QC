from joblib import Parallel, delayed
from pysradb.sraweb import SRAweb
import pandas as pd


# Using pysradb to download single DataFrame
def single_download(df_single):
    db.download(df=df_single, skip_confirmation=True)


# Connecting SRA database
db = SRAweb()
Metadata = pd.read_csv("COVID_Metadata_detailed_cleaned.tsv", sep='\t',
                       low_memory=False)

# Instrument of choice to filter Metadata
instrument = ['Illumina MiSeq', 'Illumina iSeq 100', 'Illumina '
                                                     'NovaSeq 6000',
              'Illumina HiSeq 2500', 'NextSeq 500', 'NextSeq 550']

Filtered_Metadata = Metadata[((Metadata[
    'instrument'].str.contains(
    '|'.join(instrument), case=False, na=False)))]

# Dividing DataFrame into chunks for parallel jobs
n = 1000
list_df = [Filtered_Metadata[i:i + n] for i in
           range(0, Filtered_Metadata.shape[0], n)]

# Starting parallel jobs
jobs = 6
Parallel(n_jobs=jobs)(
    delayed(single_download)(df_x) for df_x in list_df)
