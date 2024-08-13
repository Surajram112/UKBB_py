# Import packages
import pyspark
import dxpy
import dxdata
import os
import subprocess
import polars as pl
import pandas as pd
from pyspark.sql.functions import col, count, when, isnan

# Spark configuration
conf = pyspark.SparkConf() \
    .set("spark.kryoserializer.buffer.max", "2046m") \
    .set("spark.driver.maxResultSize", "0")  # Set to 0 for unlimited
    
# Spark initialization (Done only once; do not rerun this cell unless you select Kernel -> Restart kernel).
sc = pyspark.SparkContext(conf=conf)
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

def read_traits_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # remove any newline characters from each line
    lines = [line.rstrip('\n') for line in lines]

    # split each line into a list of values
    lines = [line.split('\t') for line in lines]

    # the first line contains the column names
    column_names = lines[0]

    # the rest of the lines contain the data
    data = lines[1:]

    # create a DataFrame from the data
    df = pd.DataFrame(data, columns=column_names)

    return df

# Returns all field objects for a given UKB showcase field id
def fields_for_id(field_id, dataset):
    from distutils.version import LooseVersion
    field_id = str(field_id)
    fields = dataset.find_fields(name_regex=r'^p{}(_i\d+)?(_a\d+)?$'.format(field_id))
    return sorted(fields, key=lambda f: LooseVersion(f.name))

# Returns all field names for a given UKB showcase field id
def field_names_for_id(field_id, dataset):
    return [f.name for f in fields_for_id(field_id, dataset)]

# Returns all field objects for a given title keyword
def fields_by_title_keyword(keyword, dataset):
    from distutils.version import LooseVersion
    fields = list(dataset.find_fields(lambda f: keyword.lower() in f.title.lower()))
    return sorted(fields, key=lambda f: LooseVersion(f.name))

# Returns all field names for a given title keyword
def field_names_by_title_keyword(keyword, dataset):
    return [f.name for f in fields_by_title_keyword(keyword, dataset)]

# Returns all field titles for a given title keyword
def field_titles_by_title_keyword(keyword, dataset):
    return [f.title for f in fields_by_title_keyword(keyword, dataset)]

# Extract and save datasets in efficient format with desired columns
def extract_and_save_data(dataset_name, columns_file, search_terms, output_path, extension=".parquet"):
    """
    Extracts specific columns from a dataset and saves them as a Parquet file.

    Parameters:
    - dataset_name (str): The name of the dataset to be loaded.
    - columns_file (str): Path to the file containing the list of columns to be extracted.
    - search_terms (list): List of search terms to find additional columns.
    - output_path (str): The path where the output Parquet file will be saved. Default is 'ukbb_data/'.
    - extension (str): The extension for the output file. Default is ".parquet".
    """
    
    # Load the dataset
    datasets = load_dataset()

    # Set DNAnexus project, data and traits folder
    project_folder = "../../mnt/project/"
    data_folder = "ukbb_data/"
    ext_folder = "extract_table_codes/"

    # Set up local dir for ukbb data
    os.makedirs(data_folder, exist_ok=True)

    # Set up local dir for field names used to extract data
    os.makedirs(ext_folder, exist_ok=True)

    # Load the columns file from the ubkk project folder. Check if it exists in the instance first.
    if os.path.exists(ext_folder + columns_file):
        base_fields = read_traits_file(ext_folder + columns_file)['Code'].tolist()
    else:
        base_fields = read_traits_file(project_folder + ext_folder + columns_file)['Code'].tolist()

    # Take columns file name as file name for output
    output_filename = os.path.basename(columns_file).split('.')[0]

    # Access the main dataset_name entity and get columns names as list
    dataset = datasets[dataset_name]

    # Expand codes to include all instances (visits)
    base_fields_exp = []
    for code in base_fields:
        base_fields_exp.extend(field_names_for_id(code, dataset))

    # Read additional columns based on search terms
    additional_columns = []
    if search_terms:
        for term in search_terms:
            additional_columns.extend(field_names_by_title_keyword(term, dataset))

    # Combine file columns with additional columns
    all_field_names = base_fields + base_fields_exp + additional_columns

    # Create a dictionary where the key is the field.name and the value is the field.title
    field_names_dict = {field.name: field.title for field in dataset.fields if field.name in all_field_names}

    # Save field names directly from the the spark data frame and their definition to a text file
    with open(ext_folder + output_filename + '.txt', 'w') as f:
        # Write the column names
        f.write('Code' + '\t' + 'Description' + '\n')
        # Iterate over each field in the dictionary
        for field_name, field_title in field_names_dict.items():
            # Write the name and title of the field
            f.write(field_name + '\t' + field_title + '\n')
    
    # Ensure the directory exists on DNAnexus
    subprocess.run(f'dx mkdir -p {output_path + ext_folder}', shell=True, check=True)
    
    # Upload to DNAnexus
    subprocess.run(f'dx upload {ext_folder + output_filename + ".txt"} --path {output_path + ext_folder}', shell=True, check=True)
    print(f"Field names saved and uploaded to DNAnexus Project folder")
    
    # Check if file already exists
    if os.path.exists(project_folder + data_folder + output_filename + extension):
        # Load existing data
        existing_data = pl.read_parquet(project_folder + data_folder + output_filename + extension)

        # Determine which columns have not been processed yet
        existing_columns = existing_data.columns
        new_columns = [col for col in list(field_names_dict.keys()) if col not in existing_columns]

        # If there are new columns to process
        if new_columns:
            # Retrieve fields and convert Spark DataFrame to Pandas DataFrame
            df_new = dataset.retrieve_fields(names=new_columns, engine=dxdata.connect()).toPandas()
            
            # Calculate the count of null values in each column
            null_counts = df_new.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df_new.columns]).collect()[0].asDict()

            # Identify columns where the null count equals the total row count of the DataFrame
            total_rows = df_new.count()
            columns_to_drop = [col for col, null_count in null_counts.items() if null_count == total_rows]
            
            # Drop these columns from the DataFrame
            df_new = df_new.drop(*columns_to_drop)

            # Concatenate existing and new data using pd.concat
            df_combined = pd.concat([existing_data.to_pandas(), df_new], axis=1)

            # Save as Parquet file
            df_combined.to_parquet(data_folder + output_filename + extension)

            # Ensure the directory exists on DNAnexus
            subprocess.run(f'dx mkdir -p {output_path + data_folder}', shell=True, check=True)
            
            # Upload to DNAnexus
            subprocess.run(f'dx upload {data_folder + output_filename + extension} --path {output_path + data_folder}', shell=True, check=True)
            print(f"Data saved and uploaded to DNAnexus Project folder")
        else:
            # Save as Parquet file
            existing_data.write_parquet(data_folder + output_filename + extension)
            
            # Upload to DNAnexus
            subprocess.run(f'dx upload {data_folder + output_filename + extension} --path {output_path + data_folder}', shell=True, check=True)
            print(f"No additional columns were requested and data already exists in DNAnexus Project folder")
    else:
        # Retrieve fields
        df = dataset.retrieve_fields(names=list(field_names_dict.keys()), engine=dxdata.connect())
        
        # Calculate the count of null values in each column
        null_counts = df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns]).collect()[0].asDict()

        # Identify columns where the null count equals the total row count of the DataFrame
        total_rows = df.count()
        columns_to_drop = [col for col, null_count in null_counts.items() if null_count == total_rows]
        
        # Drop these columns from the DataFrame
        df = df.drop(*columns_to_drop)
        
        # Save as Parquet file with renamed columns
        df.toPandas().to_parquet(data_folder + output_filename + extension, index=False)
        
        # Ensure the directory exists on DNAnexus
        subprocess.run(f'dx mkdir -p {output_path + data_folder}', shell=True, check=True)
        
        # Upload to DNAnexus
        subprocess.run(f'dx upload {data_folder + output_filename + extension} --path {output_path + data_folder}', shell=True, check=True)
        print(f"Data saved and uploaded to DNAnexus Project folder")