import pandas as pd
import folium
from folium.plugins import HeatMap
import json

import pandas as pd
import folium
from folium.plugins import HeatMap

businesses = pd.read_json("./Data/Yelp JSON/yelp_academic_dataset_business.json", lines=True)

businesses_clean = businesses.dropna(subset=['latitude', 'longitude'])
map_us = folium.Map(location=[37.0902, -95.7129], zoom_start=4)
heat_data = businesses_clean[['latitude', 'longitude']].values.tolist()
HeatMap(heat_data, radius=8, blur=10).add_to(map_us)

map_us.save("us_business_heatmap.html")
import webbrowser
webbrowser.open("us_business_heatmap.html")


# socal_cities = ["Los Angeles", "San Diego", "Santa Barbara", "Irvine", "Anaheim", "Long Beach"]
# socal_data = businesses_clean[businesses_clean['city'].isin(socal_cities)]
# socal_heat_data = socal_data[['latitude', 'longitude']].values.tolist()
# map_socal = folium.Map(location=[34.05, -118.25], zoom_start=8)
# HeatMap(socal_heat_data, radius=8, blur=10).add_to(map_socal)

# map_socal.save("socal_business_heatmap.html")
# webbrowser.open("socal_business_heatmap.html")
