# Import packages
import pyspark
import dxpy
import dxdata

# Spark initialization (Done only once; do not rerun this cell unless you select Kernel -> Restart kernel).
sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession(sc)

def load_dataset():
    # Automatically discover dispensed database name and dataset id
    dispensed_database = dxpy.find_one_data_object(
        classname='database', 
        name='app*', 
        folder='/', 
        name_mode='glob', 
        describe=True)
    dispensed_database_name = dispensed_database['describe']['name']

    dispensed_dataset = dxpy.find_one_data_object(
        typename='Dataset', 
        name='app*.dataset', 
        folder='/', 
        name_mode='glob')
    dispensed_dataset_id = dispensed_dataset['id']
    
    return dxdata.load_dataset(id=dispensed_dataset_id)

# Returns all field objects for a given UKB showcase field id

def fields_for_id(field_id, dataset):
    from distutils.version import LooseVersion
    field_id = str(field_id)
    fields = participant.find_fields(name_regex=r'^p{}(_i\d+)?(_a\d+)?$'.format(field_id))
    return sorted(fields, key=lambda f: LooseVersion(f.name))

# Returns all field names for a given UKB showcase field id

def field_names_for_id(field_id, dataset):
    return [f.name for f in fields_for_id(field_id, dataset)]

# Returns all field objects for a given title keyword

def fields_by_title_keyword(keyword, dataset):
    from distutils.version import LooseVersion
    fields = list(participant.find_fields(lambda f: keyword.lower() in f.title.lower()))
    return sorted(fields, key=lambda f: LooseVersion(f.name))

# Returns all field names for a given title keyword

def field_names_by_title_keyword(keyword, dataset):
    return [f.name for f in fields_by_title_keyword(keyword, dataset)]

# Returns all field titles for a given title keyword

def field_titles_by_title_keyword(keyword, dataset):
    return [f.title for f in fields_by_title_keyword(keyword, dataset)]