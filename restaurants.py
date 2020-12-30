from yelpapi import YelpAPI
import json
import pandas as pd
import geopandas

# API Parameters
with open('yelp_key.txt', 'r') as file:
    api_key = file.read().replace('\n', '')
yelp_api = YelpAPI(api_key)

# Funtion to map coordinates to Chicago neighborhoods
def find_neighborhood(p):
    neighborhoods = geopandas.read_file("Boundaries - Neighborhoods.geojson")
    for index, row in neighborhoods.iterrows():
        # poly_area = row['geometry'].area
        if row['geometry'].contains(p):
            return(row['pri_neigh'])
    return('Unknown')


# Sample 100 restaurants Yelp pages from list of categories
category_list = ['pizza', 'sandwiches', 'mexican']

bus_list = []
# cnt = 0
for cat in category_list:
    cnt = 0
    response_check = yelp_api.search_query(categories=cat, location='Chicago', limit=1)
    tot_cnt = response_check['total']
    print(cat, tot_cnt)
    while (cnt < tot_cnt) & (cnt < 100):
        resp = yelp_api.search_query(categories=cat, location='Chicago', limit=20, offset = cnt)
        for i in resp['businesses']:
            cnt+=1
            bus_list.append(i)
            print(cat, cnt, i['id'], i['name'])

bus_ids = []
dedupe_list = []
for i in bus_list:
    if i['id'] not in bus_ids: 
        bus_ids.append(i['id'])
        dedupe_list.append(i)
bus_list = dedupe_list

with open('sample_data.json', 'w') as fout:
    json.dump(bus_list, fout)


# Add neighborhood to JSON of sample data
bus_geo = pd.DataFrame()

for i in bus_list:
    to_append = [i['alias'], i['name'], i['coordinates']['latitude'], i['coordinates']['longitude']]
    a_series = pd.Series(to_append)
    bus_geo = bus_geo.append(a_series, ignore_index=True)

bus_geo.columns = ['alias', 'name', 'latitude', 'longitude']
bus_geo = geopandas.GeoDataFrame(bus_geo, geometry=geopandas.points_from_xy(bus_geo.longitude, bus_geo.latitude))

bus_geo['neighborhood'] = bus_geo.geometry.apply(find_neighborhood)

# for index, row in pizza.iterrows():
for i in bus_list:
    i['neighborhood'] = bus_geo.loc[bus_geo.alias == i['alias'], 'neighborhood'].values[0]

with open('sample_data_w_neighborhood.json', 'w') as fout:
    json.dump(bus_list, fout)