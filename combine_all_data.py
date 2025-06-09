import pandas as pd
import sys
import os
from place_geocoding import clean_city_name

# data files:
# census.csv: census data for locations with results
#   City,State,PlaceCode,StateCode,CensusVars
# 
# places.csv: census place/state codes for cities
#   City,State,StateCode,PlaceCode
#
# places_clean.csv: census place/state codes for cities
#   City,State,StateCode,PlaceCode
# duplicate city/state pairs removed, takes first place/state codes in dataset
# possibly replace with most common of prime/odd N first occurences of that city
#
# embeddings.csv: all-MiniLM-L6-v2 sentence embeddings of categories
#    cat_emb_0,cat_emb_1, ... ,cat_emb_383

if (os.path.exists("./Data/combined.csv")):
    if (input("./Data/combined.csv already found. Overwrite it? y/[n]: ").lower() != "y"):
        sys.exit(0)

data_files = {
    "census": "./Data/census.csv",
    "places": "./Data/places_clean.csv",
    "embeddings": "./Data/embeddings.csv",
    "businesses": "./Data/Yelp JSON/yelp_academic_dataset_business.json"
}
for file in data_files.values():
    if (not os.path.exists(f"{file}")):
        print(f"{file} not found, exiting")
        sys.exit(1)

yelp_data = pd.read_json(data_files["businesses"], lines=True) 
# Chose to ignore hours and attributes, harder to get predicitve features, plus other unrelated stuff
# leaves ['city', 'state', 'stars', 'review_count', 'categories']
yelp_data.drop(["business_id", "address", "postal_code", "latitude", "longitude", "is_open", "attributes", "hours"],
               axis=1, inplace=True)
print(f"Changing old column names {list(yelp_data.columns)}")
yelp_data.columns = ["Name", "City", "State", "Stars", "ReviewCount", "Categories"]
yelp_data["City"] = yelp_data["City"].apply(clean_city_name)

census_data = pd.read_csv(data_files["census"]).drop(["PlaceCode", "StateCode"], axis=1)

category_embeddings = pd.read_csv(data_files["embeddings"])
if (category_embeddings.shape[0] != yelp_data.shape[0]):
    print("Mismatched embedding data!\n"
          f"Embedding length {category_embeddings.shape[0]}\n"
          f"Yelp data length {yelp_data.shape[0]}")
    sys.exit(1)

yelp_data = pd.concat([yelp_data, category_embeddings], axis=1)

yelp_data = yelp_data.merge(census_data, how="inner", on=["City", "State"])
print(yelp_data.head())

yelp_data.to_csv("./Data/combined.csv", index=False)

