import os
import subprocess
import re
import csv
from collections import Counter
import pickle
import polars as pl
import pandas as pd
import matplotlib.pyplot as plt


# Function to run system commands
def run_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Command '{e.cmd}' returned non-zero exit status {e.returncode}.")
        print(f"Error output: {e.stderr}")
        raise

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

def dx_exists(file_name):
    # Function to check if a file exists on DNAnexus using dx find command
    command = f"dx find data --name {file_name} --brief"
    result = run_command(command)
    return bool(result.strip())

def load_files(file_ids, data_folder, local_folder, efficient_format='parquet', force_download=False):
    
    # Create the output folder if it doesn't already exist
    os.makedirs(data_folder, exist_ok=True)
    
    for file_id in file_ids:
        # Extract the file name from the file ID
        file_name = run_command(f'dx describe {file_id} --name').strip()

        # Set up file paths
        efficient_file_path = os.path.join(data_folder, file_name).replace('.csv', f'.{efficient_format}')
        local_efficient_file_path = os.path.join(local_folder, file_name).replace('.csv', f'.{efficient_format}')
        
        # Get project ID and create project folder
        project_folder = run_command(f'dx pwd').strip()
        project_folder = os.path.join(project_folder, data_folder)
        project_efficient_file_path = os.path.join(project_folder, file_name).replace('.csv', f'.{efficient_format}')
        
        # If the file does not exist in the folders both local and in the instance, go through the pipeline
        if not os.path.exists(local_efficient_file_path) and not os.path.exists(efficient_file_path):
            # Create temporary folder for large files, if they are not in the efficient format
            os.makedirs('temp', exist_ok=True)
            # Set temporary file path 
            temp_file_path = os.path.join('temp', file_name)
            
            # if csv file go through pipeline and then save it to efficient format
            if temp_file_path.endswith('.csv'):
                # Download the file to the instance ukbb_data file
                run_command(f'dx download {file_id} -o {temp_file_path} --overwrite')
                print(f"Downloaded {file_name} to {temp_file_path}")
                
                # Convert to efficient format and save
                convert_output_file_path = convert_to_efficient_format(temp_file_path, efficient_file_path, efficient_format)
                print(f"Converted {file_name} to {efficient_format} and saved to {convert_output_file_path}")
            else:
                # Download the file to the instance ukbb_data file
                run_command(f'dx download {file_id} -o {data_folder}')
                print(f"{file_name} is not a CSV file. Saving original format to {data_folder}.")
            
            # Transfer the files from instance ukbb_data file to local biobank project ukbb_data file
            run_command(f'dx upload {efficient_file_path} -o {project_folder}')
            print(f"Transferred {file_name} back to {project_folder}")
            
            # Delete temp folder directory
            delete_directory('temp')

        # Transfer the files from efficient instance ukbb_data file to efficient local biobank project ukbb_data file if not in ukbb project folder
        if os.path.exists(efficient_file_path) and not os.path.exists(local_efficient_file_path):
            run_command(f'dx upload {efficient_file_path} -o {project_folder}')
            print(f"Transferred {file_name} back to {project_folder}")
        else:
            print(f"{file_name} in {efficient_format} format already exists in {efficient_file_path}")
        
        # Transfer the files from efficient local biobank project ukbb_data file to efficient instance ukbb_data file if not in instance
        if os.path.exists(local_efficient_file_path) and not os.path.exists(efficient_file_path):
            run_command(f'dx download {project_efficient_file_path} -o {data_folder}')
            print(f"Transferred {file_name} to {efficient_file_path}")
        else:
            print(f"{file_name} in {efficient_format} format already exists in {local_efficient_file_path}")

def adjust_num_col(temp_file_path, sample_size=100):
    """
    Preprocess a CSV file.

    Parameters:
    - file_path: The path to the CSV file.
    - sample_size: The number of lines to sample to determine the most common number of fields.
    """
    # Function to handle extra fields
    def handle_extra_fields(fields, num_expected_cols):
        fields[num_expected_cols - 1:] = [','.join(fields[num_expected_cols - 1:])]
        return fields[:num_expected_cols]

    # Read the CSV file line by line
    with open(temp_file_path, 'r') as f:
        reader = csv.reader(f)
        lines = list(reader)

    # Determine the most common number of fields in the first sample_size lines
    field_counts = Counter(len(line) for line in lines[:sample_size])
    num_expected_cols = field_counts.most_common(1)[0][0]

    # Iterate over the rows and handle lines with extra fields
    for i, line in enumerate(lines):
        if len(line) != num_expected_cols:
            lines[i] = handle_extra_fields(line, num_expected_cols)

    # Write the corrected lines back to the file
    with open(temp_file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(lines)

def convert_to_efficient_format(input_file_path, output_file_path, efficient_format='parquet'):
    try:
        # Load the data into a temporary variable with specified dtypes
        df = pd.read_csv(input_file_path, dtype=str)
        efficient_file_path = output_file_path.replace('.csv', f'.{efficient_format}')
    except:
        # Checked number of columns in file and adjusted to the same
        adjust_num_col(input_file_path)
        # Load the data into a temporary variable with specified dtype as string 
        df = pd.read_csv(input_file_path, dtype=str)
        efficient_file_path = output_file_path.replace('.csv', f'.{efficient_format}')
    
    if efficient_format == 'parquet':
        df.to_parquet(efficient_file_path)
    elif efficient_format == 'pickle':
        with open(efficient_file_path, 'wb') as f:
            pickle.dump(df, f)
            
    return efficient_file_path

def delete_directory(directory):
    # Delete all files in the directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            delete_directory(file_path)
    # Delete the directory itself
    os.rmdir(directory)

def load_efficient_format(file_path, efficient_format='parquet'):
    efficient_file_path = file_path.replace('.csv', f'.{efficient_format}')
    
    if os.path.exists(efficient_file_path):
        if efficient_format == 'parquet':
            return pd.read_parquet(efficient_file_path)
        elif efficient_format == 'pickle':
            with open(efficient_file_path, 'rb') as f:
                return pickle.load(f)
    else:
        return None

# Create a data and traits folder if it doesn't exist
ukbb_folder  = 'ukbb_data/'
os.makedirs(ukbb_folder, exist_ok=True)

traits_folder  = 'ukbb_traits/'
os.makedirs(traits_folder, exist_ok=True)

# Set the local project folder just to run find
local_data_folder = '../../mnt/project/ukbb_data/'
local_traits_folder = '../../mnt/project/ukbb_traits/'

# List of file IDs for data to download
data_file_ids = [
    'file-GZKXxpQJKVzZ0BgzQZ1YYj9Z',  # GP registrations
    'file-GZKXxpQJKVzVb7gQvQbYfxKF',  # GP clinical
    'file-GZKXxpQJKVzxKb0FYfGg28v9',  # GP scripts
    'file-Gp36v5jJ0qKKgQXxxQ0gjf0f',  # HES Diagnoses
    'file-Gp36v5QJ0qKJv0pQ0X5Z2B5K',  # HES Records
    'file-Gp36v5jJ0qK58yQkJ06gzGvv',  # OPCS Records
    'file-GZKXgQjJY2Fkg1ZG4Bg8ZJBP',  # Cancer_Registry
    'file-Gpp9P0jJ40Y3ZPg13B3kzj2g',  # self report
    'file-GZJ9Z98Jj59fZFqVq69b6p2Z',  # read code 2 list
    'file-GZJ9Z98Jj59b8zjVF1x1Z19G',  # read code 3 list
    'file-GZJ9bbQJj59YBg8j4Kffpx9v',  # data coding 3
    'file-GZJ9Z98Jj59YBg8j4Kffpx78',  # data coding 4
    'file-GZJ9Z98Jj59gQ0zX6p3Jx3p9',  # data coding 6
    'file-GZq40X0Jj59QzkKqx73PX3ff',  # ICD-O3 coding
    'file-GZKXVx8J9jFp1qpBQZ8z5PbJ',  # death records
    'file-GZKXVx8J9jFqG2GvJV8vzxK1'   # death causes
]

# List of file IDs for traits codes to download
traits_file_ids = [
    'file-GpFq378J40Y1PGqP8gQpKzp6',  # self reported diabetes
    'file-GpFq9vjJ40Y559k7FZpx4bQf',  # self reported type 1 dm
    'file-Gpp7QJ8J40Y0P0Fj0g0fpvKV',  # self reported treatments type 1 dm
    'file-GpFjjb0J40YJ1Xkj442Y7pPq',  # manchester diabetes research codes
    'file-GpFjjb0J40YJXV7Y8xGbXx1Q',  # read code v2 drugs
    'file-GpFjjb0J40Y2KQ1yK3Qzx3V9',  # read code v2 exclusion
    'file-GpFjjb0J40YJ1Xkj442Y7pPk',  # read code v2 type 1 dm
    'file-GpFjjb0J40Y0Jky82gPj2fxq',  # read code v2 type 2 dm
    'file-GpFjjb0J40Y7py7pKGxZ4FV6',  # read code v3 exclusion
    'file-GpFjjb0J40YJXV7Y8xGbXx1X',  # read code v2 type 1 dm
    'file-GpFjjb0J40Y11QYZBVfQ7YF4',  # read code v2 type 2 dm
    'file-GpFpg18J40YBYgZXG11Y2qq6',  # ICD10 diabetes
    'file-GpFq9vjJ40Y3Gz5KyqZjKx24',  # ICD10 type 1 dm
    'file-GppGB38J40YKP747926fbV5J',  # ICD9 type 1 dm
]

# Load data files, if force download is True then original files will be reloaded
load_files(data_file_ids, ukbb_folder, local_data_folder)

# Load codes lists , if force download is True then original files will be reloaded
load_files(traits_file_ids, traits_folder, local_traits_folder)

# Load baseline table and import file to run it
run_command("curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/new_baseline.py > new_baseline.py")
import new_baseline

def read_GP(codes, folder='ukbb_data/', filename='GP_gp_clinical', efficient_format='.parquet'):
    gp_header = ['eid', 'data_provider', 'event_dt', 'read_2', 'read_3', 'value1', 'value2', 'value3', 'dob', 'assess_date', 'event_age', 'prev']
     
    if not codes:
        return pl.DataFrame(schema={col: pl.Utf8 for col in gp_header})
    
    # Read the parquet file using polars
    data = pl.read_parquet(folder + filename + efficient_format, use_pyarrow=True)
    
    # Filter data using vectorized operations
    data2 = data.filter(pl.col('read_3').is_in(codes) | pl.col('read_2').is_in(codes))
    
    if data2.is_empty():
        return pl.DataFrame(schema={col: pl.Utf8 for col in gp_header})
    
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    # Convert date columns to datetime and handle invalid dates
    data2 = data2.with_columns([
        pl.col('event_dt').str.strptime(pl.Datetime, strict=False).dt.date().alias('event_dt')
    ])

    # Update exclude and exclude_reason for invalid dates
    data2 = data2.with_columns([
        pl.when(pl.col('event_dt').is_null())
        .then(True)
        .otherwise(pl.col('exclude'))
        .alias('exclude'),
        pl.when(pl.col('event_dt').is_null())
        .then(pl.lit("Missing event_dt"))
        .otherwise(pl.col('exclude_reason'))
        .alias('exclude_reason')
    ])
    
    # Convert value columns to float
    data2 = data2.with_columns([
        pl.col('value1').cast(pl.Float64, strict=False),
        pl.col('value2').cast(pl.Float64, strict=False),
        pl.col('value3').cast(pl.Float64, strict=False), 
        pl.lit('GP').alias('source')
    ])
    
    return data2

def read_OPCS(codes, folder='ukbb_data/', filename='HES_hesin_oper', extension='.parquet'):
    opcs_header = ['dnx_hesin_oper_id', 'eid', 'ins_index', 'arr_index', 'opdate', 'level', 'oper3', 'oper3_nb', 'oper4', 'oper4_nb', 'posopdur', 'preopdur']
    
    if not codes:
        return pl.DataFrame(schema={col: pl.Utf8 for col in opcs_header}), pl.DataFrame(schema={col: pl.Utf8 for col in opcs_header})
    
    # Read the parquet file using polars
    data = pl.read_parquet(folder + filename + extension, use_pyarrow=True)
    
    # Filter data using vectorized operations
    data2 = data.filter(
        pl.col('oper3').str.contains('|'.join(codes)) | 
        pl.col('oper4').str.contains('|'.join(codes))
    )
    
    if data2.is_empty():
        return pl.DataFrame(schema={col: pl.Utf8 for col in opcs_header}), pl.DataFrame(schema={col: pl.Utf8 for col in opcs_header})
    
    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64),
        pl.col('ins_index').cast(pl.Int64),
        pl.col('arr_index').cast(pl.Int64),
        pl.col('posopdur').cast(pl.Int64),
        pl.col('preopdur').cast(pl.Int64)
    ])
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    # Convert date columns to datetime and handle invalid dates
    data2 = data2.with_columns([
        pl.col('opdate').str.strptime(pl.Datetime, strict=False).dt.date().alias('opdate')
    ])

    # Update exclude and exclude_reason for invalid dates
    data2 = data2.with_columns([
        pl.when(pl.col('opdate').is_null())
        .then(True)
        .otherwise(pl.col('exclude'))
        .alias('exclude'),
        pl.when(pl.col('opdate').is_null())
        .then(pl.lit("Missing opdate"))
        .otherwise(pl.col('exclude_reason'))
        .alias('exclude_reason')
    ])
    
    # Calculate op_age and prev
    data2 = data2.with_columns([
        pl.lit('OPCS').alias('source')
    ])
    
    return data2.drop('dnx_hesin_oper_id')

def read_ICD10(codes, folder='ukbb_data/', diagfile='HES_hesin_diag', recordfile='HES_hesin', extension='.parquet'):
    icd10_header = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'level', 'diag_icd9', 'diag_icd10', 'dnx_hesin_id', 'epistart', 'epiend']
    
    if not codes:
        return pl.DataFrame(schema={col: pl.Utf8 for col in icd10_header})
    
    # Read the parquet diagnosis file using polars
    diag_data = pl.read_parquet(folder + diagfile + extension, use_pyarrow=True)
    
    # Filter data using vectorized operations to check if any code is in the strings
    data = diag_data.filter(
        pl.col('diag_icd10').str.contains('|'.join(codes))
    )
    
    if data.is_empty():
        return pl.DataFrame(schema={col: pl.Utf8 for col in icd10_header})
    
    data = data.select(['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'level', 'diag_icd9', 'diag_icd10'])
    
    # Read the parquet records file using polars
    record_data = pl.read_parquet(folder + recordfile + extension, use_pyarrow=True)
    
    record_data = record_data.with_columns([
        pl.col('eid').cast(pl.Int64),
        pl.col('ins_index').cast(pl.Int64)
    ])
    
    # Join with record data
    data2 = data.join(record_data, on=['eid', 'ins_index'])

    # Check for the existence of 'epistart' and 'epiend' columns
    columns_to_check = ['epistart', 'epiend']
    for col in columns_to_check:
        if col not in data2.columns:
            data2 = data2.with_columns(pl.lit(None).alias(col))

    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason'),
        pl.lit(True).alias('epistart_invalid'),
        pl.lit(True).alias('epiend_invalid'),
        pl.lit("Missing epistart").alias('epistart_reason'),
        pl.lit("Missing epiend").alias('epiend_reason')
    ])

    # Convert valid dates to datetime and back-calculate missing dates
    data2 = data2.with_columns([
        pl.col('epistart').str.strptime(pl.Datetime, strict=False).dt.date().alias('epistart'),
        pl.col('epiend').str.strptime(pl.Datetime, strict=False).dt.date().alias('epiend'),
        pl.col('epidur').cast(pl.Int64, strict=False)
    ])

    # Back-calculate missing dates
    data2 = data2.with_columns([
        pl.when(pl.col('epistart').is_null() & pl.col('epiend').is_not_null() & pl.col('epidur').is_not_null())
        .then(pl.col('epiend') - pl.duration(days=pl.col('epidur')))
        .otherwise(pl.col('epistart'))
        .alias('epistart'),
        pl.when(pl.col('epiend').is_null() & pl.col('epistart').is_not_null() & pl.col('epidur').is_not_null())
        .then(pl.col('epistart') + pl.duration(days=pl.col('epidur')))
        .otherwise(pl.col('epiend'))
        .alias('epiend')
    ])

    # Update exclude and exclude_reason for back-calculated dates
    data2 = data2.with_columns([
        pl.when(pl.col('epistart').is_not_null() | pl.col('epiend').is_not_null())
        .then(False)
        .otherwise(pl.col('exclude'))
        .alias('exclude'),
        pl.when(pl.col('epistart').is_not_null() | pl.col('epiend').is_not_null())
        .then(pl.concat_str([pl.col('exclude_reason'), pl.lit(" (back-calculated)")]))
        .otherwise(pl.col('exclude_reason'))
        .alias('exclude_reason')
    ])

    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64),
        pl.col('epidur').cast(pl.Int64),
        pl.col('bedyear').cast(pl.Int64),
        pl.lit('HES_ICD10').alias('source')
    ])

    return data2.drop(['dnx_hesin_diag_id', 'dnx_hesin_id', 'epistart_invalid', 'epiend_invalid', 'epistart_reason', 'epiend_reason'])

def read_ICD9(codes, folder='ukbb_data/', diagfile='HES_hesin_diag', recordfile='HES_hesin', extension='.parquet'):
    icd9_header = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'level', 'diag_icd9', 'diag_icd10', 'dnx_hesin_id', 'epistart', 'epiend']
    
    if not codes:
        return pl.DataFrame(schema={col: pl.Utf8 for col in icd9_header}), pl.DataFrame(schema={col: pl.Utf8 for col in icd9_header})
    
    # Read the parquet diagnosis file using polars
    diag_data = pl.read_parquet(folder + diagfile + extension, use_pyarrow=True)
    
    # Filter data using vectorized operations to check if any code is in the strings
    data = diag_data.filter(
        pl.col('diag_icd9').str.contains('|'.join(codes))
    )
    
    if data.is_empty():
        return pl.DataFrame(schema={col: pl.Utf8 for col in icd9_header}), pl.DataFrame(schema={col: pl.Utf8 for col in icd9_header})
    
    data.columns = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'classification', 'diag_icd9', 'diag_icd9_add', 'diag_icd10', 'diag_icd10_add']
    
    data = data.select(['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'classification', 'diag_icd9', 'diag_icd10'])
    
    # Read the parquet records file using polars
    record_data = pl.read_parquet(folder + recordfile + extension, use_pyarrow=True)
    
    record_data = record_data.with_columns([
        pl.col('eid').cast(pl.Int64),
        pl.col('ins_index').cast(pl.Int64)
    ])
    
    # Join with record data
    data2 = data.join(record_data, on=['eid', 'ins_index'])

    # Check for the existence of 'epistart' and 'epiend' columns
    columns_to_check = ['epistart', 'epiend']
    for col in columns_to_check:
        if col not in data2.columns:
            data2 = data2.with_columns(pl.lit(None).alias(col))

    # Check if dates are valid and add exclusion information
    data2 = data2.with_columns([
        pl.lit(True).alias('epistart_invalid'),
        pl.lit(True).alias('epiend_invalid'),
        pl.lit("Missing epistart").alias('epistart_reason'),
        pl.lit("Missing epiend").alias('epiend_reason')
    ])

    # Add exclusion information based on invalid datetime values
    data2 = data2.with_columns([
        pl.lit(True).alias('exclude'),
        pl.lit("Invalid epistart and epiend").alias('exclude_reason')
    ])

    # Convert valid dates to datetime and back-calculate missing dates
    data2 = data2.with_columns([
        pl.col('epistart').str.strptime(pl.Datetime, strict=False).dt.date().alias('epistart'),
        pl.col('epiend').str.strptime(pl.Datetime, strict=False).dt.date().alias('epiend'),
        pl.col('epidur').cast(pl.Int64, strict=False)
    ])

    # Back-calculate missing dates
    data2 = data2.with_columns([
        pl.when(pl.col('epistart').is_null() & pl.col('epiend').is_not_null() & pl.col('epidur').is_not_null())
        .then(pl.col('epiend') - pl.duration(days=pl.col('epidur')))
        .otherwise(pl.col('epistart'))
        .alias('epistart'),
        pl.when(pl.col('epiend').is_null() & pl.col('epistart').is_not_null() & pl.col('epidur').is_not_null())
        .then(pl.col('epistart') + pl.duration(days=pl.col('epidur')))
        .otherwise(pl.col('epiend'))
        .alias('epiend')
    ])

    # Update exclude and exclude_reason for back-calculated dates
    data2 = data2.with_columns([
        pl.when(pl.col('epistart').is_not_null() | pl.col('epiend').is_not_null())
        .then(False)
        .otherwise(pl.col('exclude'))
        .alias('exclude'),
        pl.when(pl.col('epistart').is_not_null() | pl.col('epiend').is_not_null())
        .then(pl.concat_str([pl.col('exclude_reason'), pl.lit(" (back-calculated)")]))
        .otherwise(pl.col('exclude_reason'))
        .alias('exclude_reason')
    ])

    # Drop temporary columns
    data2 = data2.drop(['epistart_invalid', 'epiend_invalid', 'epistart_reason', 'epiend_reason'])

    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64),
        pl.col('epidur').cast(pl.Int64),
        pl.col('bedyear').cast(pl.Int64),
        pl.col('epiorder').cast(pl.Int64),
        pl.lit('HES_ICD9').alias('source')
    ])
    
    return data2.drop(['dnx_hesin_diag_id', 'dnx_hesin_id'])

# def read_cancer(codes, folder='ukbb_data/', filename='cancer_participant', baseline_filename='Baseline.csv', extension='.parquet'):
#     cancer_header = ["eid", "reg_date", "site", "age", "histology", "behaviour", "dob", "assess_date", "diag_age", "prev", "code", "description"]
#     if not codes:
#         return pd.DataFrame(columns=cancer_header)
    
#     run_command(f"sed -i 's/\"//g' {folder + filename}")
#     codes2 = [f",{code}" for code in codes]
#     codes3 = '\\|'.join(codes2)
#     grepcode = f'grep \'{codes3}\' {folder + filename} > temp.csv'
#     run_command(grepcode)
    
#     if not pd.read_csv('temp.csv').shape[0]:
#         return pd.DataFrame(columns=cancer_header)
    
#     data = pd.read_csv('temp.csv', header=None)
#     data.columns = pd.read_csv(file, nrows=1).columns
    
#     ids = data.iloc[:, 0].repeat(22).values
#     datesvars = [f'p40005_i{i}' for i in range(22)]
#     cancersvars = [f'p40006_i{i}' for i in range(22)]
#     agevars = [f'p40008_i{i}' for i in range(22)]
#     histologyvars = [f'p40011_i{i}' for i in range(22)]
#     behaviourvars = [f'p40012_i{i}' for i in range(22)]
    
#     dateslist = data[datesvars].values.flatten()
#     cancerslist = data[cancersvars].values.flatten()
#     agelist = data[agevars].values.flatten()
#     histologylist = data[histologyvars].values.flatten()
#     behaviourlist = data[behaviourvars].values.flatten()
    
#     data = pd.DataFrame({'eid': ids, 'reg_date': dateslist, 'site': cancerslist, 'age': agelist, 'histology': histologylist, 'behaviour': behaviourlist})
#     data['reg_date'] = pd.to_datetime(data['reg_date'])
    
#     codes4 = '|'.join(codes)
#     data = data[data['site'].str.contains(codes4)]
    
#     baseline_table = pd.read_csv(baseline_filename)
#     baseline_table['dob'] = pd.to_datetime(baseline_table['dob'])
#     baseline_table['assess_date'] = pd.to_datetime(baseline_table['assess_date'])
#     data = data.merge(baseline_table[['eid', 'dob', 'assess_date']], on='eid')
#     data['diag_age'] = (data['reg_date'] - data['dob']).dt.days / 365.25
#     data['prev'] = data['reg_date'] < data['assess_date']
#     data['code'] = data['histology'] + '/' + data['behaviour']
    
#     icdo3 = pd.read_csv('ICDO3.csv', sep='\t')
#     icdo3 = icdo3.rename(columns={'histology': 'description'})
#     data = data.merge(icdo3, on='description', how='left')
    
#     return data

def read_selfreport_illness(codes, folder='ukbb_data/', file='selfreport_participant', coding_file='coding6.tsv', extension='.parquet'):
    if not codes:
        return pl.DataFrame(), pl.DataFrame()
    
    # Read the parquet file using polars
    data = pl.read_parquet(folder + file + extension, use_pyarrow=True)
    
    # Read the coding6 file
    coding6 = pl.read_csv(folder + coding_file, separator='\t')
    coding6 = coding6.filter(pl.col('coding') > 1)

    # Filter data using vectorized operations
    outlines = []
    for code in codes:
        # Search in all p20002_i* columns, reporting 'Non-cancer illness code, self-reported'
        non_cancer_illness_columns = [col for col in data.columns if col.startswith('p20002_i')]
        for col in non_cancer_illness_columns:
            outline = data.filter(pl.col(col).str.contains(code))['eid']
            outlines.extend(outline.to_list())
            
    if not outlines:
        return pl.DataFrame(), pl.DataFrame()
    
    data2 = data.filter(pl.col('eid').is_in(outlines))
    
    if data2.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64)
    ])
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    return data2.with_columns(pl.lit('Self').alias('source'))

def read_selfreport_cancer(codes, folder='ukbb_data/', file='selfreport_participant', coding_file='coding3.tsv', extension='.parquet'):
    if not codes:
        return pl.DataFrame(), pl.DataFrame()
    
    # Read the parquet file using polars
    data = pl.read_parquet(folder + file + extension, use_pyarrow=True)
    
    # Read the coding3 file
    coding3 = pl.read_csv(folder + coding_file, separator='\t')
    coding3 = coding3.filter(pl.col('coding') > 1)
    
    # Filter data using vectorized operations            
    outlines = []
    for code in codes:
        # Search in all p20001_i* columns, related to 'Cancer code, self-reported'
        treatment_columns = [col for col in data.columns if col.startswith('p20001_i')]
        for col in treatment_columns:
            outline = data.filter(pl.col(col).str.contains(code))['eid']
            outlines.extend(outline.to_list())
    
    if not outlines:
        return pl.DataFrame(), pl.DataFrame()
    
    data = data.filter(pl.col('eid').is_in(outlines))
    
    if data.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    data = data.with_columns([
        pl.col('eid').cast(pl.Int64)
    ])
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    return data2.with_columns(pl.lit('Self').alias('source'))

def read_selfreport_treatment(codes, folder='ukbb_data/', file='selfreport_participant', coding_file='coding4.tsv', extension='.parquet'):
    if not codes:
        return pl.DataFrame(), pl.DataFrame()
    
    # Read the parquet file using polars
    data = pl.read_parquet(folder + file + extension, use_pyarrow=True)
    
    # Read the coding4 file
    coding4 = pl.read_csv(folder + coding_file, separator='\t')
    
    # Filter coding4 data
    coding4 = coding4.filter(pl.col('coding') > 1)
    
    # Filter data using vectorized operations
    outlines = []
    for code in codes:
        # Search in all p20003_i* columns, reported as 'Treatment/medication code'
        treatment_columns = [col for col in data.columns if col.startswith('p20003_i')]
        for col in treatment_columns:
            outline = data.filter(pl.col(col).str.contains(code))['eid']
            outlines.extend(outline.to_list())
    
    if not outlines:
        return pl.DataFrame(), pl.DataFrame()
    
    data = data.filter(pl.col('eid').is_in(outlines))
    
    if data.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    data = data.with_columns([
        pl.col('eid').cast(pl.Int64)
    ])
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    return data2.with_columns(pl.lit('Self').alias('source'))

def read_selfreport_operation(codes, folder='ukbb_data/', file='selfreport_participant', coding_file='coding4.tsv', extension='.parquet'):
    if not codes:
        return pl.DataFrame(), pl.DataFrame()
    
    # Read the parquet file using polars
    data = pl.read_parquet(folder + file + extension, use_pyarrow=True)
    
    # Read the coding4 file
    coding4 = pl.read_csv(folder + coding_file, separator='\t')
    
    # Filter coding4 data
    coding4 = coding4.filter(pl.col('coding') > 1)
    
    outlines = []
    for code in codes:
        meaning = coding4.filter(pl.col('coding') == int(code))['meaning']
        if not meaning.is_empty():
            # Search in all p20004_i* columns, recorded as 'Operation code'
            treatment_columns = [col for col in data.columns if col.startswith('p20004_i')]
            for col in treatment_columns:
                outline = data.filter(pl.col(col).str.contains(meaning[0]))['eid']
                outlines.extend(outline.to_list())
                
    if not outlines:
        return pl.DataFrame(), pl.DataFrame()
    
    data2 = data.filter(pl.col('eid').is_in(outlines))
    
    if data2.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64)
    ])
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    return data2.with_columns(pl.lit('Self').alias('source'))

def first_occurance(*dataframes):
    # Create a new column 'Date_diag_earliest' for each DataFrame
    processed_dfs = []
    for df in dataframes:
        if 'event_dt' in df.columns:
            df = df.with_columns(pl.col('event_dt').cast(pl.Datetime).alias('Date_diag_earliest'))
        elif 'date' in df.columns:
            df = df.with_columns(pl.col('date').cast(pl.Datetime).alias('Date_diag_earliest'))
        elif 'assess_date' in df.columns:
            df = df.with_columns(pl.col('assess_date').cast(pl.Datetime).alias('Date_diag_earliest'))
        processed_dfs.append(df)
    
    # Concatenate the DataFrames
    all_records = pl.concat(processed_dfs, how="diagonal")

    # Group by 'eid' and get the earliest 'Date_diag_earliest'
    earliest_dates = all_records.group_by('eid').agg(pl.col('Date_diag_earliest').min().alias('Date_diag_earliest'))

    # Join the earliest dates back to the original DataFrame to retain all columns
    all_records = all_records.join(earliest_dates, on='eid', how='left')

    # Drop duplicates based on 'eid' and keep the first occurrence
    all_records = all_records.sort('Date_diag_earliest').group_by('eid').first()

    # Drop the original 'date' column if it exists
    if 'date' in all_records.columns:
        all_records = all_records.drop('date')

    # Calculate Diagnosis Age
    all_records = all_records.with_columns(
        ((pl.col('Date_diag_earliest') - pl.col('dob')).dt.total_days() / 365.25)
        .round()
        .alias('Diagnosis Age')
    )
    
    return all_records        

def read_GP_scripts(codes, folder='ukbb_date/', file='GP_gp_scripts.csv'):
    gp_header = ['eid', 'data_provider', 'issue_date', 'read_2', 'dmd_code', 'bnf_code', 'drug_name', 'quantity']
    
    if not codes:
        return pd.DataFrame(columns=gp_header)
    
    codes = [f",{code}" for code in codes]
    codes2 = '\\|'.join(codes)
    grepcode = f'grep \'{codes2}\' {file} > temp.csv'
    run_command(grepcode)
    
    if os.path.getsize('temp.csv') == 0:
        return pd.DataFrame(columns=gp_header)
    
    data = pd.read_csv('temp.csv', header=None)
    data.columns = gp_header
    data['issue_date'] = pd.to_datetime(data['issue_date'])
    
    data2 = pd.DataFrame()
    for code in codes:
        data2 = pd.concat([data2, data[data['dmd_code'] == code]], ignore_index=True)
        data2 = pd.concat([data2, data[data['read_2'] == code]], ignore_index=True)
        data2 = pd.concat([data2, data[data['bnf_code'] == code]], ignore_index=True)
    
    return data2

def read_death(codes, folder='ukbb_date/', diagfile='death_death_cause.csv', recordfile='death_death.csv', baseline_filename='Baseline.csv'):
    death_header = ['dnx_death_id', 'eid', 'ins_index', 'dsource', 'source', 'date_of_death', 'level', 'cause_icd10']
    
    if not codes:
        return pd.DataFrame(columns=death_header)
    
    codes = [f"\"{code}" for code in codes]
    codes2 = '\\|'.join(codes)
    grepcode = f'grep \'{codes2}\' {diagfile} > temp.csv'
    run_command(grepcode)
    
    if os.path.getsize('temp.csv') == 0:
        return pd.DataFrame(columns=death_header)
    
    data = pd.read_csv('temp.csv', header=None)
    data.columns = ['dnx_death_cause_id', 'eid', 'ins_index', 'arr_index', 'level', 'cause_icd10']
    
    records = pd.read_csv(recordfile)
    data2 = data.merge(records, on=['eid', 'ins_index'])
    data2['date_of_death'] = pd.to_datetime(data2['date_of_death'])
    
    baseline_table = pd.read_csv(baseline_filename)
    baseline_table['dob'] = pd.to_datetime(baseline_table['dob'])
    baseline_table['assess_date'] = pd.to_datetime(baseline_table['assess_date'])
    data2 = data2.merge(baseline_table[['eid', 'dob', 'assess_date']], on='eid')
    data2['death_age'] = (data2['date_of_death'] - data2['dob']).dt.days / 365.25
    data2['prev'] = data2['date_of_death'] < data2['assess_date']
    
    return data2

run_command('curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/Generate_GRS.sh > Generate_GRS.sh')
run_command('chmod +777 Generate_GRS.sh')

def Generate_GRS(grs_file, folder='ukbb_date/'):
    command = f'./Generate_GRS.sh {grs_file}'
    run_command(command)
    plink_score_df = pd.read_csv('score.profile', delim_whitespace=True)
    plink_score_df = plink_score_df.rename(columns={'FID': 'eid', 'SCORE': 'score'})
    plink_score_df = plink_score_df[['eid', 'score']]
    return plink_score_df

# Example usage
# Self_codes = ['1222']
# Self_records = read_selfreport(Self_codes)

# GP_codes = ['C10..']
# GP_records = read_GP(GP_codes)

# OPCS_codes = ['Z92.4', 'C29.2']
# OPCS_records = read_OPCS(OPCS_codes)

# ICD9_codes = ['250']
# ICD9_records = read_ICD9(ICD9_codes)

# ICD10_codes = ['E10']
# ICD10_records = read_ICD10(ICD10_codes)
