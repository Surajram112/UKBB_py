import os
import subprocess
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

# Load the field ids
run_command("curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/data_file_ids.txt > data_file_ids.txt")
run_command("curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/cols_file_ids.txt > cols_file_ids.txt")

# Load Generate_GRS basch script
run_command('curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/Generate_GRS.sh > Generate_GRS.sh')
run_command('chmod +777 Generate_GRS.sh')

def read_txt_file(file_path):
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

def load_save_data(project_folder):
    # Create a data and traits folder if it doesn't exist
    ukbb_folder  = f'{project_folder}/ukbb_data/'
    os.makedirs(ukbb_folder, exist_ok=True)
    
    tables_folder  = f'{project_folder}/cols_in_tables/'
    os.makedirs(tables_folder, exist_ok=True)
    
    data_file_ids = read_txt_file('data_file_ids.txt')['Id'].tolist()
    cols_file_ids = read_txt_file('cols_file_ids.txt')['Id'].tolist()
    
    # Load data files, if force download is True then original files will be reloaded
    load_files(data_file_ids, ukbb_folder)
    
    # Load extracted tables columns lists , if force download is True then original files will be reloaded
    load_files(cols_file_ids, tables_folder)

def load_baseline_data(output_file='Baseline.parquet'):
    if os.path.exists(output_file):
        print(f"{output_file} already exists. Skipping processing.")
        return

    def month_name_to_number(month_name):
        month_name = month_name.strip().capitalize()
        month_dict = {name: num for num, name in enumerate(calendar.month_name) if name}
        return month_dict.get(month_name, 0)

    # Download and load baseline table
    subprocess.run('dx download file-GZPzVp0JkBXbqJjYZvzvkjg4 -o Baseline.csv', shell=True, check=True)
    baseline_table = pd.read_csv('Baseline.csv')

    baseline_table.columns = ['eid', 'recruit_age', 'mob', 'yob', 'sex', 'tdi', 'ethnicity', 'alcohol', 'alcohol_freq', 
                              'former_alcohol', 'ever_smoked', 'pack_years', 'smoking_status', 'current_smoking', 
                              'father_illness', 'mother_illness', 'overall_health', 'ever_psa', 'ever_bowel_cancer_screening', 
                              'recent_bowel_cancer_screening', 'time_since_psa', 'diabetes_diagnosed', 'gestational_diabetes', 
                              'age_diabetes_diagnosed', 'ins_1_year', 'cancer_diagnosed', 'chol_med', 'chol_hormone_med', 
                              'prescription_meds', 'had_menopause', 'age_menopause', 'age_menarche', 'fluid_intelligence', 
                              'matches_time', 'birth_weight', 'diastolic_blood_pressure', 'diastolic_blood_pressure_manual', 
                              'systolic_blood_pressure', 'height', 'waist', 'weight', 'bmi', 'hip', 'standing_height', 'body_fat', 
                              'heel_BMD_manual', 'heel_sound_speed', 'heel_BMD', 'glucose', 'hba1c', 'hdl_cholesterol', 
                              'ldl_cholesterol', 'total_cholesterol', 'blood_type', 'assess_date', 'centre']

    # Convert month name to number
    baseline_table['mob'] = baseline_table['mob'].apply(lambda x: month_name_to_number(x))

    # Ensure 'yob' and 'mob' columns are integers
    baseline_table['yob'] = baseline_table['yob'].astype(int)
    baseline_table['mob'] = baseline_table['mob'].astype(int)

    # Create 'dob' column
    baseline_table['dob'] = pd.to_datetime(baseline_table[['yob', 'mob']].assign(day=15).astype(str).agg('-'.join, axis=1))

    # Convert 'assess_date' to datetime
    baseline_table['assess_date'] = pd.to_datetime(baseline_table['assess_date'])

    # Calculate 'assess_age' and 'whr' columns
    baseline_table['assess_age'] = (baseline_table['assess_date'] - baseline_table['dob']).dt.days / 365.25
    baseline_table['whr'] = baseline_table['waist'] / baseline_table['hip']

    # Save the processed baseline table and delete csv file
    os.remove('Baseline.csv')
    baseline_table.to_parquet(output_file)
    print(f"Processed baseline data saved to {output_file}")

def load_files(file_ids, data_folder):
    
    # Create the output folder if it doesn't already exist
    os.makedirs(data_folder, exist_ok=True)
    
    for file_id in file_ids:
        # Extract the file name from the file ID
        file_name = run_command(f'dx describe {file_id} --name').strip()

        # Set up file paths
        file_path = os.path.join(data_folder, file_name)
        
        # Get project ID and create project folder
        local_file_path = os.path.join('../../mnt/project/', file_name)
        
        # If the file does not exist in the folders both local and in the instance, go through the pipeline
        if not os.path.exists(local_file_path) and not os.path.exists(file_path):
            # Download the file to the instance ukbb_data file
            run_command(f'dx download {file_id} -o {data_folder}')
            print(f"Getting original file and saving to {data_folder}.")
            
            # Transfer the files from instance ukbb_data file to local biobank project ukbb_data file
            run_command(f'dx upload {file_path} -o {data_folder}')
            print(f"Uploaded {file_name} back to DNAnexus Project.")

        # Transfer the files from efficient instance ukbb_data file to efficient local biobank project ukbb_data file if not in ukbb project folder
        if os.path.exists(file_path) and not os.path.exists(local_file_path):
            run_command(f'dx upload {file_path} -o {data_folder}')
            print(f"Uploaded {file_name} back to DNAnexus Project.")
        else:
            print(f"{file_name} already exists in the instance, at {file_path}")
        
        # Transfer the files from efficient local biobank project ukbb_data file to efficient instance ukbb_data file if not in instance
        if os.path.exists(local_file_path) and not os.path.exists(file_path):
            run_command(f'dx download {file_path} -o {data_folder}')
            print(f"Transferred {file_name} to {file_path}")
        else:
            print(f"{file_name} already exists in the DNAnexus Project.")

def read_GP(codes, project_folder, filename='GP_gp_clinical', extension='.parquet', filter_column=None, filter_criteria=None):
    gp_header = ['eid', 'data_provider', 'event_dt', 'read_2', 'read_3', 'value1', 'value2', 'value3', 'dob', 'assess_date', 'event_age', 'prev']
     
    # Set up local dir for ukbb data
    data_folder = f'{project_folder}/ukbb_data/' 
    
    # Read the parquet file using polars
    data = pl.read_parquet(data_folder + filename + extension, use_pyarrow=True)
    
    if codes:
        # Filter data using vectorized operations
        data2 = data.filter(pl.col('read_3').is_in(codes) | pl.col('read_2').is_in(codes))
    else:
        # If codes is empty, use the entire dataset
        data2 = data
    
    if data2.is_empty():
        return pl.DataFrame(schema={col: pl.Utf8 for col in gp_header})
    
    # Apply additional filtering based on filter_column and filter_criteria
    if filter_column and filter_criteria:
        data2 = data2.filter(pl.col(filter_column).is_in(filter_criteria))
    
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

def read_OPCS(codes, project_folder, filename='HES_hesin_oper', extension='.parquet', filter_column=None, filter_criteria=None):
    opcs_header = ['dnx_hesin_oper_id', 'eid', 'ins_index', 'arr_index', 'opdate', 'level', 'oper3', 'oper3_nb', 'oper4', 'oper4_nb', 'posopdur', 'preopdur']
    
    # Set up local dir for ukbb data
    data_folder = f'{project_folder}/ukbb_data/' 
    
    # Read the parquet file using polars
    data = pl.read_parquet(data_folder + filename + extension, use_pyarrow=True)
    
    if codes:
        # Filter data using vectorized operations
        data2 = data.filter(
            pl.col('oper3').str.contains('|'.join(codes)) | 
            pl.col('oper4').str.contains('|'.join(codes))
        )
    else:
        # If codes is empty, use the entire dataset
        data2 = data
    
    if data2.is_empty():
        return pl.DataFrame(schema={col: pl.Utf8 for col in opcs_header}), pl.DataFrame(schema={col: pl.Utf8 for col in opcs_header})
    
    # Apply additional filtering based on filter_column and filter_criteria
    if filter_column and filter_criteria:
        data2 = data2.filter(pl.col(filter_column).is_in(filter_criteria))
    
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

def read_ICD10(codes, project_folder, diagfile='HES_hesin_diag', recordfile='HES_hesin', extension='.parquet', filter_column=None, filter_criteria=None):
    icd10_header = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'level', 'diag_icd9', 'diag_icd10', 'dnx_hesin_id', 'epistart', 'epiend']
    
    # Set up local dir for ukbb data
    data_folder = f'{project_folder}/ukbb_data/' 
    
    # Read the parquet file using polars
    diag_data = pl.read_parquet(data_folder + diagfile + extension, use_pyarrow=True)
    
    if codes:
        # Filter data using vectorized operations to check if any code is in the strings
        data = diag_data.filter(
            pl.col('diag_icd10').str.contains('|'.join(codes))
        )
    else:
        # If codes is empty, use the entire dataset
        data = diag_data
    
    if data.is_empty():
        return pl.DataFrame(schema={col: pl.Utf8 for col in icd10_header})
    
    data = data.select(['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'level', 'diag_icd9', 'diag_icd10'])
    
    # Read the parquet records file using polars
    record_data = pl.read_parquet(data_folder + recordfile + extension, use_pyarrow=True)
    
    record_data = record_data.with_columns([
        pl.col('eid').cast(pl.Int64),
        pl.col('ins_index').cast(pl.Int64)
    ])
    
    # Join with record data
    data2 = data.join(record_data, on=['eid', 'ins_index'])

    # Apply additional filtering based on filter_column and filter_criteria
    if filter_column and filter_criteria:
        data2 = data2.filter(pl.col(filter_column).is_in(filter_criteria))

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

def read_ICD9(codes, project_folder, diagfile='HES_hesin_diag', recordfile='HES_hesin', extension='.parquet', filter_column=None, filter_criteria=None):
    icd9_header = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'level', 'diag_icd9', 'diag_icd10', 'dnx_hesin_id', 'epistart', 'epiend']
    
    # Set up local dir for ukbb data
    data_folder = f'{project_folder}/ukbb_data/' 
    
    # Read the parquet file using polars
    diag_data = pl.read_parquet(data_folder + diagfile + extension, use_pyarrow=True)
    
    if codes:
        # Filter data using vectorized operations to check if any code is in the strings
        data = diag_data.filter(
            pl.col('diag_icd9').str.contains('|'.join(codes))
        )
    else:
        # If codes is empty, use the entire dataset
        data = diag_data
    
    if data.is_empty():
        return pl.DataFrame(schema={col: pl.Utf8 for col in icd9_header}), pl.DataFrame(schema={col: pl.Utf8 for col in icd9_header})
    
    data.columns = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'classification', 'diag_icd9', 'diag_icd9_add', 'diag_icd10', 'diag_icd10_add']
    
    data = data.select(['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'classification', 'diag_icd9', 'diag_icd10'])
    
    # Read the parquet records file using polars
    record_data = pl.read_parquet(data_folder + recordfile + extension, use_pyarrow=True)
    
    record_data = record_data.with_columns([
        pl.col('eid').cast(pl.Int64),
        pl.col('ins_index').cast(pl.Int64)
    ])
    
    # Join with record data
    data2 = data.join(record_data, on=['eid', 'ins_index'])

    # Apply additional filtering based on filter_column and filter_criteria
    if filter_column and filter_criteria:
        data2 = data2.filter(pl.col(filter_column).is_in(filter_criteria))

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

def read_selfreport_illness(codes, project_folder, filename='selfreport_participant', coding_file='coding6.tsv', extension='.parquet', filter_column=None, filter_criteria=None):
    # Set up local dir for ukbb data
    data_folder = f'{project_folder}/ukbb_data/'
    cols_folder = f'{project_folder}/cols_in_tables/'
    
    # Read the parquet file using polars
    data = pl.read_parquet(data_folder + filename + extension, use_pyarrow=True)
    
    # Read the coding6 file
    coding6 = pl.read_csv(data_folder + coding_file, separator='\t')
    coding6 = coding6.filter(pl.col('coding') > 1)

    # Read the columns file to get the current and new column names
    columns_df = pl.read_csv(cols_folder + filename + '.txt', separator='\t')
    columns_dict = dict(zip(columns_df['Code'], columns_df['Description']))

    if codes:
        # Filter data using vectorized operations
        outlines = []
        for code in codes:
            # Search in all p20002_i* columns, reporting 'Non-cancer illness code, self-reported'
            non_cancer_illness_columns = [col for col in data.columns if col.startswith('p20002_i')]
            for col in non_cancer_illness_columns:
                outline = data.filter(pl.col(col).str.contains(code))['eid']
                outlines.extend(outline.to_list())
        
        if outlines:
            data2 = data.filter(pl.col('eid').is_in(outlines))
        else:
            data2 = data
    else:
        # If codes is empty, use the entire dataset
        data2 = data
    
    # Apply additional filtering based on filter_column and filter_criteria
    if filter_column and filter_criteria:
        data2 = data2.filter(pl.col(filter_column).is_in(filter_criteria))
    
    if data2.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64)
    ])
    
    # Rename the columns in the DataFrame
    data2 = data2.rename(columns_dict)
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    return data2.with_columns(pl.lit('Self').alias('source'))

def read_selfreport_cancer(codes, project_folder, filename='selfreport_participant', coding_file='coding3.tsv', extension='.parquet', filter_column=None, filter_criteria=None):
    # Set up local dir for ukbb data
    data_folder = f'{project_folder}/ukbb_data/' 
    cols_folder = f'{project_folder}/cols_in_tables/'
    
    # Read the parquet file using polars
    data = pl.read_parquet(data_folder + filename + extension, use_pyarrow=True)
    
    # Read the coding3 file
    coding3 = pl.read_csv(data_folder + coding_file, separator='\t')
    coding3 = coding3.filter(pl.col('coding') > 1)
    
    # Read the columns file to get the current and new column names
    columns_df = pl.read_csv(cols_folder + filename + '.txt', separator='\t')
    columns_dict = dict(zip(columns_df['Code'], columns_df['Description']))
    
    if codes:
        # Filter data using vectorized operations            
        outlines = []
        for code in codes:
            # Search in all p20001_i* columns, related to 'Cancer code, self-reported'
            treatment_columns = [col for col in data.columns if col.startswith('p20001_i')]
            for col in treatment_columns:
                outline = data.filter(pl.col(col).str.contains(code))['eid']
                outlines.extend(outline.to_list())
        
        if outlines:
            data2 = data.filter(pl.col('eid').is_in(outlines))
        else:
            data2 = data
    else:
        # If codes is empty, use the entire dataset
        data2 = data
    
    # Apply additional filtering based on filter_column and filter_criteria
    if filter_column and filter_criteria:
        data2 = data2.filter(pl.col(filter_column).is_in(filter_criteria))
    
    if data2.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64)
    ])
    
    # Rename the columns in the DataFrame
    data2 = data2.rename(columns_dict)
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    return data2.with_columns(pl.lit('Self').alias('source'))

def read_selfreport_treatment(codes, project_folder, filename='selfreport_participant', coding_file='coding4.tsv', extension='.parquet', filter_column=None, filter_criteria=None):
    # Set up local dir for ukbb data
    data_folder = f'{project_folder}/ukbb_data/' 
    cols_folder = f'{project_folder}/cols_in_tables/'
    
    # Read the parquet file using polars
    data = pl.read_parquet(data_folder + filename + extension, use_pyarrow=True)
    
    # Read the coding4 file
    coding4 = pl.read_csv(data_folder + coding_file, separator='\t')
    
    # Filter coding4 data
    coding4 = coding4.filter(pl.col('coding') > 1)
    
    # Read the columns file to get the current and new column names
    columns_df = pl.read_csv(cols_folder + filename + '.txt', separator='\t')
    columns_dict = dict(zip(columns_df['Code'], columns_df['Description']))
    
    if codes:
        # Filter data using vectorized operations
        outlines = []
        for code in codes:
            # Search in all p20003_i* columns, reported as 'Treatment/medication code'
            treatment_columns = [col for col in data.columns if col.startswith('p20003_i')]
            for col in treatment_columns:
                outline = data.filter(pl.col(col).str.contains(code))['eid']
                outlines.extend(outline.to_list())
        
        if outlines:
            data2 = data.filter(pl.col('eid').is_in(outlines))
        else:
            data2 = data
    else:
        # If codes is empty, use the entire dataset
        data2 = data
    
    # Apply additional filtering based on filter_column and filter_criteria
    if filter_column and filter_criteria:
        data2 = data2.filter(pl.col(filter_column).is_in(filter_criteria))
    
    if data2.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64)
    ])
    
    # Rename the columns in the DataFrame
    data2 = data2.rename(columns_dict)
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    return data2.with_columns(pl.lit('Self').alias('source'))

def read_selfreport_operation(codes, project_folder, filename='selfreport_participant', coding_file='coding4.tsv', extension='.parquet', filter_column=None, filter_criteria=None):
    # Set up local dir for ukbb data
    data_folder = f'{project_folder}/ukbb_data/'
    cols_folder = f'{project_folder}/cols_in_tables/'
    
    # Read the parquet file using polars
    data = pl.read_parquet(data_folder + filename + extension, use_pyarrow=True)
    
    # Read the coding4 file
    coding4 = pl.read_csv(data_folder + coding_file, separator='\t')
    
    # Filter coding4 data
    coding4 = coding4.filter(pl.col('coding') > 1)
    
    # Read the columns file to get the current and new column names
    columns_df = pl.read_csv(cols_folder + filename + '.txt', separator='\t')
    columns_dict = dict(zip(columns_df['Code'], columns_df['Description']))
    
    if codes:
        outlines = []
        for code in codes:
            meaning = coding4.filter(pl.col('coding') == int(code))['meaning']
            if not meaning.is_empty():
                # Search in all p20004_i* columns, recorded as 'Operation code'
                treatment_columns = [col for col in data.columns if col.startswith('p20004_i')]
                for col in treatment_columns:
                    outline = data.filter(pl.col(col).str.contains(meaning[0]))['eid']
                    outlines.extend(outline.to_list())
        
        if outlines:
            data2 = data.filter(pl.col('eid').is_in(outlines))
        else:
            data2 = data
    else:
        # If codes is empty, use the entire dataset
        data2 = data
    
    # Apply additional filtering based on filter_column and filter_criteria
    if filter_column and filter_criteria:
        data2 = data2.filter(pl.col(filter_column).is_in(filter_criteria))
    
    if data2.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    data2 = data2.with_columns([
        pl.col('eid').cast(pl.Int64)
    ])
    
    # Rename the columns in the DataFrame
    data2 = data2.rename(columns_dict)
    
    # Add exclusion columns
    data2 = data2.with_columns([
        pl.lit(False).alias('exclude'),
        pl.lit("").alias('exclude_reason')
    ])
    
    return data2.with_columns(pl.lit('Self').alias('source'))

def align_participant_records(*dataframes):
    # Create a new column 'diag_date' for each DataFrame
    processed_dfs = []
    for df in dataframes:
        if 'event_dt' in df.columns:
            df = df.with_columns(pl.col('event_dt').cast(pl.Datetime).alias('diag_date'))
        elif 'date' in df.columns:
            df = df.with_columns(pl.col('date').cast(pl.Datetime).alias('diag_date'))
        elif 'opdate' in df.columns:
            df = df.with_columns(pl.col('opdate').cast(pl.Datetime).alias('diag_date'))
        elif 'epistart' in df.columns:
            df = df.with_columns(pl.col('epistart').cast(pl.Datetime).alias('diag_date'))
        processed_dfs.append(df)
    
    # Perform an outer join on 'eid' and 'diag_date' if available, otherwise just on 'eid'
    merged_df = processed_dfs[0]
    for df in processed_dfs[1:]:
        join_columns = ['eid']
        if 'diag_date' in df.columns and 'diag_date' in merged_df.columns:
            join_columns.append('diag_date')
        
        # Get unique columns from the right DataFrame
        right_columns = [col for col in df.columns if col not in merged_df.columns and col not in join_columns]
        
        merged_df = merged_df.join(
            df.select(join_columns + right_columns),
            on=join_columns,
            how='outer'
        )
    
    # Sort the result by 'eid' and 'diag_date' if available
    if 'diag_date' in merged_df.columns:
        merged_df = merged_df.sort(['eid', 'diag_date'])
    else:
        merged_df = merged_df.sort('eid')
    
    return merged_df

def first_occurance(all_records):
    # Group by 'eid' and get the earliest 'Date_diag_earliest'
    earliest_dates = all_records.group_by('eid').agg(pl.col('diag_date').min().alias('diag_date'))

    # Join the earliest dates back to the original DataFrame to retain all columns
    all_records = all_records.join(earliest_dates, on='eid', how='left')

    # Drop duplicates based on 'eid' and keep the first occurrence
    all_records = all_records.sort('diag_date').group_by('eid').first()

    # Drop the original 'date' column if it exists
    if 'date' in all_records.columns:
        all_records = all_records.drop('date')

    # Calculate Diagnosis Age
    all_records = all_records.with_columns(
        ((pl.col('diag_date') - pl.col('dob')).dt.total_days() / 365.25)
        .round()
        .alias('Diagnosis Age')
    )
    return all_records        

def plot_age_groups(records, bin_size=10):
    """
    Plots the number of individuals by age group with a specified bin size.

    Parameters:
    - records: A Polars DataFrame containing the data.
    - bin_size: The size of the bins for age grouping (default is 10).
    """
    # Remove duplicates based on 'eid'
    unique_records = records.unique(subset=['eid'])

    # Convert to pandas DataFrame for easier manipulation
    df = unique_records.to_pandas()

    # Create bins for age groups based on the specified bin size
    bins = range(0, int(df['Diagnosis Age'].max()) + bin_size, bin_size)
    labels = [f'{i}-{i + bin_size - 1}' for i in bins[:-1]]

    # Bin the ages
    df['Age Group'] = pd.cut(df['Diagnosis Age'], bins=bins, labels=labels, right=False)

    # Count the number of records in each age group
    age_group_counts = df['Age Group'].value_counts().sort_index()

    # Plot the data
    plt.figure(figsize=(10, 6))
    age_group_counts.plot(kind='bar')
    plt.xlabel('Age Group')
    plt.ylabel('Number of Individuals')
    plt.title('Number of Individuals by Age Group')
    plt.xticks(rotation=45)
    plt.show()

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
