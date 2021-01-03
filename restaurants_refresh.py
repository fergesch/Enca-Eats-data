import time
from yelpapi import YelpAPI
from shapely.geometry import Point
from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey
import datetime
import utils

t_0 = datetime.datetime.now()

# https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/cosmos/azure-cosmos
# Cosmos Connection Parameters

client = CosmosClient(utils.cosmos_key['url'], credential = utils.cosmos_key['key'])
database = client.get_database_client(utils.cosmos_key['database'])
container_cat_hier = database.get_container_client('category_hierarchy')
container_rest = database.get_container_client('restaurants')

cat_list = []
for item in container_cat_hier.query_items('select * from category_hierarchy', enable_cross_partition_query = True):
    cat_list.append(item)

# Yelp API Parameters
yelp_api = YelpAPI(utils.yelp_key)


# Start pulling businesses within food and restaurants
bus_list = []
# cnt = 0
for cat_dict in cat_list:
    cat = cat_dict['alias']
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
    print(cat, tot_cnt, check_tot)

# dedupe businesses
bus_ids = []
dedupe_list = []
for i in bus_list:
    if i['id'] not in bus_ids: 
        bus_ids.append(i['id'])
        dedupe_list.append(i)

print(len(bus_list), len(dedupe_list))

t_dedupe = datetime.datetime.now()
# load restaurants container
container_rest = database.get_container_client('restaurants')
database.delete_container('restaurants')
database.create_container(id='restaurants', partition_key=PartitionKey(path='/id', kind='Hash'))

for bus in dedupe_list:
    if (not(bus['coordinates']['longitude'] is None)) and (not(bus['coordinates']['latitude'] is None)):
        bus['neighborhood'] = utils.find_neighborhood(Point(bus['coordinates']['longitude'], bus['coordinates']['latitude']))
    else:
        bus['neighborhood'] = 'Unknown'
    container_rest.upsert_item(bus)
print('Load time:', datetime.datetime.now()- t_dedupe)
print('Total time:', datetime.datetime.now() - t_0)