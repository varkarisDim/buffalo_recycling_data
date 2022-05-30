import pandas as pd
from sodapy import Socrata

CLIENT_URL = "data.buffalony.gov"
DATASET_IDENTIFIER = "2cjd-uvx7"

def retrieve_as_df():
    # create the client through socrata for the source domain
    client = Socrata(CLIENT_URL, None)

    # fetch the dataset from the API endpoint
    results = client.get(DATASET_IDENTIFIER, limit=1500)

    # convert the dataset to panda data frame for processing
    results_df = pd.DataFrame.from_records(results)

    return results_df