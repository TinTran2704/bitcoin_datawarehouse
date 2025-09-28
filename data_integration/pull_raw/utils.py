# Standard imports
import fnmatch
import re
import yaml


# Library imports
import pandas


# Local imports
# from data_integration.arguments import SELECT
# from data_integration.utils.utils import cleanse_database_object_name



def get_tables_to_sync(yaml_path):
    with open(yaml_path) as file_to_read:
        yaml_content = file_to_read.read()
    tables_to_sync = yaml.safe_load(yaml_content).get('tables')

    # Check the parameters
    for table in tables_to_sync:
        name = table.get('name')
        id = table.get('id')
        
    tables_to_sync_df = pandas.DataFrame(tables_to_sync)

    return tables_to_sync
