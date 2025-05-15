# https://www.evanmiller.org/how-not-to-sort-by-average-rating.html

import pandas as pd
import math
# import numpy as np

yelp_df = pd.read_json('Data/Yelp JSON/business.json', lines=True)

yelp_df = yelp_df[['name', 'city', 'state', 'stars', 'review_count']]

yelp_10 = yelp_df.head(10)

def weighted_sort(score, reviews):
    weight_score = score - (score - 0.5) * (2 ** -math.log10(reviews + 1))
    return weight_score

yelp_10['w_score'] = yelp_10.apply(lambda x: weighted_sort(x.stars, x.review_count), axis=1)


#print(yelp_df.head(10))
#print(yelp_df.info(verbose=True))

# print(yelp_df.loc[:, "city"])
print(yelp_10)