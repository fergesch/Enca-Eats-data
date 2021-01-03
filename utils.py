import json
import geopandas
from treelib import Node, Tree

with open('yelp_key.txt', 'r') as file:
    yelp_key = file.read().replace('\n', '')

with open('cosmos_key.json', 'r') as file:
    cosmos_key = json.loads(file.read())

def find_neighborhood(p):
    neighborhoods = geopandas.read_file("Boundaries - Neighborhoods.geojson")
    for index, row in neighborhoods.iterrows():
        if row['geometry'].contains(p):
            return(row['pri_neigh'])
    return('Unknown')

def build_tree(cat_list):
    tree = Tree()
    tree.create_node('root','root')
    while len(cat_list) > 0:
        pop_list = []
        for i in range(len(cat_list)):
            try:
                if cat_list[i]['parent_aliases'] == []:
                    tree.create_node(cat_list[i]['title'], cat_list[i]['alias'], parent = 'root')
                else:
                    tree.create_node(cat_list[i]['title'], cat_list[i]['alias'], parent = cat_list[i]['parent_aliases'][0])
                pop_list.append(i)
            except:
                pass
        pop_list.sort(reverse = True)
        for i in pop_list:
            cat_list.pop(i)
    return(tree)
