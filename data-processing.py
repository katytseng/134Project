import pandas as pd
import numpy as np

yelp_df = pd.read_json('Data/Yelp JSON/yelp_dataset/yelp_academic_dataset_business.json')

print(yelp_df.head(5), lines=True)