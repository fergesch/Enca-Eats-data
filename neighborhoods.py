import geopandas
from azure.cosmos import CosmosClient
from azure.cosmos.partition_key import PartitionKey
import utils

neighborhoods = geopandas.read_file("Boundaries - Neighborhoods.geojson")

client = CosmosClient(utils.cosmos_key['url'], credential = utils.cosmos_key['key'])
database = client.get_database_client(utils.cosmos_key['database'])
container = database.get_container_client('neighborhoods')

database.delete_container('neighborhoods')
database.create_container(id='neighborhoods', partition_key=PartitionKey(path='/name', kind='Hash'))

for index, row in neighborhoods.iterrows():
    n_dict = {
        'name': row['pri_neigh'],
        'sec_name': row['sec_neigh']
    }
    container.upsert_item(n_dict)