# Enca-Eats-data
For a full refresh the below scripts must be run in order.
- category_refresh.py
    - Refreshes categories table
    - Uses yelp api via requests to get all categories for yelp
- category_hierarchy_refresh.py
    - Refreshes category hierarchy table
    - Gets all categories from categories table
    - Creates hierarchy of selectable categories with all descendants
- restaurants_refresh.py
    - Refreshes restaurants table
    - Gets all selectable categories from category_hierarchy table
    - Gets restaurants from categories using yelpapi package
    - Adds neighborhood to restaurant

## Environment setup
Install miniconda and setup virtual environment
``` bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_54.sh
bash https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_54.sh
conda enc create --prefix ./.venv -f environment.yml
```

To activate, run `conda activate ./.venv` from directory that contains virtual environment (.venv)