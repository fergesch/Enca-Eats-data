import requests
import json
from treelib import Node, Tree
import re
from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey

# https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/cosmos/azure-cosmos
# Cosmos Connection Parameters
cosmos_url = 'https://enca-eats-cosmos.documents.azure.com:443/'
cosmos_key = 'pAu4Vbd3QCBsOy3FdN9Cq5hTAeMMTnITZODR4QCA5j1O3bOtcRTQpV9kbLicNcZXlIBVcVhkHeSt1BqyZE4rJw=='
client = CosmosClient(cosmos_url, credential = cosmos_key)
database_name = 'enca-eats'
database = client.get_database_client(database_name)
container_cat = database.get_container_client('categories')
container_cat_hier = database.get_container_client('category_hierarchy')

# API Parameters
with open('yelp_key.txt', 'r') as file:
    api_key = file.read().replace('\n', '')
headers = {'Authorization': 'Bearer %s' % api_key}
url = 'https://api.yelp.com/v3/categories'
params = {'locale': 'en_US'}

# Get list of all categories
cat_req = requests.get(url, params = params, headers=headers)
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
                tree.create_node(cat_copy[i]['alias'], cat_copy[i]['alias'], parent = 'root')
            else:
                tree.create_node(cat_copy[i]['alias'], cat_copy[i]['alias'], parent = cat_copy[i]['parent_aliases'][0])
            pop_list.append(i)
        except:
            pass
    pop_list.sort(reverse = True)
    for i in pop_list:
        cat_copy.pop(i)

# Convert tree to json and upload to cosmos
cat_dict = json.loads(re.sub(',*\s*"data": null', '',tree.to_json(with_data=True)))
database.delete_container('category_hierarchy')
database.create_container(id='category_hierarchy', partition_key=PartitionKey(path='/root', kind='Hash'))
container_cat_hier.upsert_item(cat_dict)

# tree.save2file('tree.txt')