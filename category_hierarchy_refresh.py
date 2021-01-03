from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey
import utils

client = CosmosClient(utils.cosmos_key['url'], credential = utils.cosmos_key['key'])
database = client.get_database_client(utils.cosmos_key['database'])
container_cat = database.get_container_client('categories')
container_cat_hier = database.get_container_client('category_hierarchy')

cat_list = []
for item in container_cat.query_items('select * from categories', enable_cross_partition_query = True):
    cat_list.append(item)

tree = utils.build_tree(cat_list)

hier_list = []
for a in ['food', 'restaurants']:
    for i in tree.children(a):
        tmp_dict = {'alias': i.identifier,
                    'title': i.tag,
                    'children': []}
        for j in tree.expand_tree(i.identifier):
            child = tree.get_node(j)
            child_dict = {'alias': child.identifier,
                          'title': child.tag}
            tmp_dict['children'].append(child_dict)
        hier_list.append(tmp_dict)

database.delete_container('category_hierarchy')
database.create_container(id='category_hierarchy', partition_key=PartitionKey(path='/alias', kind='Hash'))
for i in hier_list:
    container_cat_hier.upsert_item(i)