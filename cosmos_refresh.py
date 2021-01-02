import requests
import json
import time
from yelpapi import YelpAPI
import pandas as pd
import geopandas
from shapely.geometry import Point
from treelib import Node, Tree
# import re
from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey
import datetime

# https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/cosmos/azure-cosmos
# Cosmos Connection Parameters
cosmos_url = 'https://enca-eats-cosmos.documents.azure.com:443/'
cosmos_key = 'pAu4Vbd3QCBsOy3FdN9Cq5hTAeMMTnITZODR4QCA5j1O3bOtcRTQpV9kbLicNcZXlIBVcVhkHeSt1BqyZE4rJw=='
client = CosmosClient(cosmos_url, credential = cosmos_key)
database_name = 'enca-eats'
database = client.get_database_client(database_name)
container_cat = database.get_container_client('categories')
container_cat_hier = database.get_container_client('category_hierarchy')
container_rest = database.get_container_client('restaurants')

# Yelp API Parameters
with open('yelp_key.txt', 'r') as file:
    api_key = file.read().replace('\n', '')
headers = {'Authorization': f'Bearer {api_key}'}
cat_url = 'https://api.yelp.com/v3/categories'
params = {'locale': 'en_US'}
# Configure yelpapi package for searches
yelp_api = YelpAPI(api_key)

# Funtion to map coordinates to Chicago neighborhoods
def find_neighborhood(p):
    neighborhoods = geopandas.read_file("Boundaries - Neighborhoods.geojson")
    for index, row in neighborhoods.iterrows():
        if row['geometry'].contains(p):
            return(row['pri_neigh'])
    return('Unknown')

# Get list of all categories
cat_req = requests.get(cat_url, params = params, headers=headers)
cat_parsed = json.loads(cat_req.text)
cat_list = cat_parsed['categories']

# Cleanup database and insert refreshed categories
database.delete_container('categories')
database.create_container(id='categories', partition_key=PartitionKey(path='/alias', kind='Hash'))

for i in range(len(cat_list)):
    # id is optional, will be created if not provided
    # cat_list[i]['id'] = str(i)
    container_cat.upsert_item(cat_list[i])
    print(i, cat_list[i])

# Create tree of categories
# Need single 'root' so create dummy node 'root'
cat_copy = cat_list.copy()
tree = Tree()
tree.create_node('root','root')

while len(cat_copy) > 0:
    pop_list = []
    print(len(cat_copy))
    for i in range(len(cat_copy)):
        try:
            if cat_copy[i]['parent_aliases'] == []:
                tree.create_node(cat_copy[i]['title'], cat_copy[i]['alias'], parent = 'root')
            else:
                tree.create_node(cat_copy[i]['title'], cat_copy[i]['alias'], parent = cat_copy[i]['parent_aliases'][0])
            pop_list.append(i)
        except:
            pass
    pop_list.sort(reverse = True)
    for i in pop_list:
        cat_copy.pop(i)

# # Convert tree to json and upload to cosmos
# # Need to figure out right structure
# cat_dict = json.loads(re.sub(',*\s*"data": null', '',tree.to_json(with_data=True)))
# database.delete_container('category_hierarchy')
# database.create_container(id='category_hierarchy', partition_key=PartitionKey(path='/root', kind='Hash'))
# container_cat_hier.upsert_item(cat_dict)
# tree.save2file('tree.txt')


# Start pulling businesses within food and restaurants
bus_list = []
# cnt = 0
for a in ['food', 'restaurants']:
    for i in tree.children(a):
        cat = i.identifier
        cnt = 0
        retry = 0
        # Get count of businesses for category
        while retry < 3:
            try:
                tot_cnt = yelp_api.search_query(categories=cat, location='Chicago', limit=1)['total']
                retry = 3
            except:
                retry += 1
                time.sleep(10)
                tot_cnt = -1
        check_tot = 0
        while (cnt < tot_cnt) & (cnt < 900):
            time.sleep(2)
            retry = 0
            while retry < 3:
                try:
                    resp = yelp_api.search_query(categories=cat, location='Chicago', limit=50, offset = cnt)
                    if len(resp['businesses']) > 0:
                        for i in resp['businesses']:
                            cnt+=1
                            check_tot += 1
                            bus_list.append(i)
                    else:
                        cnt = 1000
                    retry = 3
                except:
                    retry += 1
                    print(cat, retry, cnt)
                    time.sleep(10)
        print(a, cat, tot_cnt, check_tot)

# dedupe businesses
bus_ids = []
dedupe_list = []
for i in bus_list:
    if i['id'] not in bus_ids: 
        bus_ids.append(i['id'])
        dedupe_list.append(i)

print(len(bus_list), len(dedupe_list))

t_0 = datetime.datetime.now()
# load restaurants container
container_rest = database.get_container_client('restaurants')
database.delete_container('restaurants')
database.create_container(id='restaurants', partition_key=PartitionKey(path='/id', kind='Hash'))

for bus in dedupe_list:
    if (not(bus['coordinates']['longitude'] is None)) and (not(bus['coordinates']['latitude'] is None)):
        bus['neighborhood'] = find_neighborhood(Point(bus['coordinates']['longitude'], bus['coordinates']['latitude']))
    else:
        bus['neighborhood'] = 'Unknown'
    container_rest.upsert_item(bus)
print(datetime.datetime.now() - t_0)