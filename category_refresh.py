import requests
import json
from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey
import utils

# https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/cosmos/azure-cosmos
# Cosmos Connection Parameters
client = CosmosClient(utils.cosmos_key['url'], credential = utils.cosmos_key['key'])
database = client.get_database_client(utils.cosmos_key['database'])
container_cat = database.get_container_client('categories')

# Yelp API Parameters
yelp_key = utils.yelp_key
headers = {'Authorization': f'Bearer {yelp_key}'}
cat_url = 'https://api.yelp.com/v3/categories'
params = {'locale': 'en_US'}

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