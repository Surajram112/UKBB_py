import pandas as pd
import subprocess
from datetime import datetime
import re
import os
import requests
import calendar

# Function to run system commands
def run_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Command '{e.cmd}' returned non-zero exit status {e.returncode}.")
        print(f"Error output: {e.stderr}")
        raise

def download_files(file_ids, force_download=False):
    for file_id in file_ids:
        # Extract the file name from the file ID
        file_name = run_command(f'dx describe {file_id} --name').strip()
        file_path = os.path.join(data_folder, file_name)
        
        if not os.path.exists(file_path) or force_download:
            run_command(f'dx download {file_id} -o {file_path}')
            print(f"Downloaded {file_name}")
        else:
            print(f"{file_name} already exists in {data_folder}")

# Create a data folder if it doesn't exist
data_folder = "ukbb_data"
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

# List of file IDs to download
file_ids = [
    'file-GZKXxpQJKVzZ0BgzQZ1YYj9Z',  # GP registrations
    'file-GZKXxpQJKVzVb7gQvQbYfxKF',  # GP clinical
    'file-GZKXxpQJKVzxKb0FYfGg28v9',  # GP scripts
    'file-Gp36v5jJ0qKKgQXxxQ0gjf0f',  # HES Diagnoses
    'file-Gp36v5QJ0qKJv0pQ0X5Z2B5K',  # HES Records
    'file-Gp36v5jJ0qK58yQkJ06gzGvv',  # OPCS Records
    'file-GZKXgQjJY2Fkg1ZG4Bg8ZJBP',  # Cancer_Registry
    'file-GZJKVv8J9qP9xGfpK5KpP5G9',  # self report
    'file-GZJ9bbQJj59YBg8j4Kffpx9v',  # data coding 3
    'file-GZJ9Z98Jj59YBg8j4Kffpx78',  # data coding 4
    'file-GZJ9Z98Jj59gQ0zX6p3Jx3p9',  # data coding 6
    'file-GZq40X0Jj59QzkKqx73PX3ff',  # ICD-O3 coding
    'file-GZKXVx8J9jFp1qpBQZ8z5PbJ',  # death records
    'file-GZKXVx8J9jFqG2GvJV8vzxK1'   # death causes
]

# Download files if they don't exist, unless force_download is True
download_files(file_ids, force_download=False)

# Load baseline table and import file to run it
run_command("curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/new_baseline.py > new_baseline.py")
import new_baseline
    
def read_GP(codes, folder='ukbb_data/', filename='GP_gp_clinical.csv', baseline_filename='Baseline.csv'):
    gp_header = ['eid', 'data_provider', 'event_dt', 'read_2', 'read_3', 'value1', 'value2', 'value3', 'dob', 'assess_date', 'event_age', 'prev']
    if not codes:
        return pd.DataFrame(columns=gp_header)
    
    codes2 = [f",{code}" for code in codes]
    codes3 = '\\|'.join(codes2)
    grepcode = f"grep '{codes3}' {folder + filename} > temp.csv"
    run_command(grepcode)
    
    if not pd.read_csv('temp.csv').shape[0]:
        return pd.DataFrame(columns=gp_header)
    
    data = pd.read_csv('temp.csv', header=None)
    data.columns = ['eid', 'data_provider', 'event_dt', 'read_2', 'read_3', 'value1', 'value2', 'value3']
    data['event_dt'] = data['event_dt'].astype(str).apply(lambda x: '1901-01-01' if re.search("[a-zA-Z]", x) else x)
    data['event_dt'] = pd.to_datetime(data['event_dt'])
    
    data2 = pd.DataFrame()
    for code in codes:
        data2 = pd.concat([data2, data[data['read_3'] == code]], ignore_index=True)
        data2 = pd.concat([data2, data[data['read_2'] == code]], ignore_index=True)
    
    baseline_table = pd.read_csv(baseline_filename)
    baseline_table['dob'] = pd.to_datetime(baseline_table['dob'])
    baseline_table['assess_date'] = pd.to_datetime(baseline_table['assess_date'])
    data2 = data2.merge(baseline_table[['eid', 'dob', 'assess_date']], on='eid')
    data2['event_age'] = (data2['event_dt'] - data2['dob']).dt.days / 365.25
    data2['prev'] = data2['event_dt'] < data2['assess_date']
    return data2

def read_OPCS(codes, folder='ukbb_data/', filename='HES_hesin_oper.csv', baseline_filename='Baseline.csv'):
    opcs_header = ['dnx_hesin_oper_id', 'eid', 'ins_index', 'arr_index', 'opdate', 'level', 'oper3', 'oper3_nb', 'oper4', 'oper4_nb', 'posopdur', 'preopdur']
    if not codes:
        return pd.DataFrame(columns=opcs_header)
    
    codes2 = [f",{code}" for code in codes]
    codes3 = '\\|'.join(codes2)
    grepcode = f"grep '{codes3}' {folder + filename} > temp.csv"
    run_command(grepcode)
    
    if not pd.read_csv('temp.csv').shape[0]:
        return pd.DataFrame(columns=opcs_header)
    
    data = pd.read_csv('temp.csv', header=None)
    data.columns = opcs_header
    data['opdate'] = pd.to_datetime(data['opdate'])
    
    baseline_table = pd.read_csv(baseline_filename)
    baseline_table['dob'] = pd.to_datetime(baseline_table['dob'])
    baseline_table['assess_date'] = pd.to_datetime(baseline_table['assess_date'])
    data = data.merge(baseline_table[['eid', 'dob', 'assess_date']], on='eid')
    data['op_age'] = (data['opdate'] - data['dob']).dt.days / 365.25
    data['prev'] = data['opdate'] < data['assess_date']
    return data.drop(columns=['dnx_hesin_oper_id'])

def read_ICD10(codes, folder='ukbb_data/', diagfile='HES_hesin_diag.csv', recordfile='HES_hesin.csv', baseline_filename='Baseline.csv'):
    icd10_header = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'level', 'diag_icd9', 'diag_icd10', 'dnx_hesin_id', 'epistart', 'epiend']
    if not codes:
        return pd.DataFrame(columns=icd10_header)
    
    run_command(f"sed -i 's/\"//g' {folder + diagfile}")
    codes2 = [f",{code}" for code in codes]
    codes3 = '\\|'.join(codes2)
    grepcode = f'grep \'{codes3}\' {folder + diagfile} > temp.csv'
    run_command(grepcode)
    
    if not pd.read_csv('temp.csv').shape[0]:
        return pd.DataFrame(columns=icd10_header)
    
    data = pd.read_csv('temp.csv', header=None)
    data.columns = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'classification', 'diag_icd9', 'diag_icd9_add', 'diag_icd10', 'diag_icd10_add']
    data = data[['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'classification', 'diag_icd9', 'diag_icd10']]
    records = pd.read_csv(folder + recordfile)
    data2 = data.merge(records, on=['eid', 'ins_index'])
    data2['epistart'] = pd.to_datetime(data2['epistart'])
    data2['epiend'] = pd.to_datetime(data2['epiend'])
    
    baseline_table = pd.read_csv(baseline_filename)
    baseline_table['dob'] = pd.to_datetime(baseline_table['dob'])
    baseline_table['assess_date'] = pd.to_datetime(baseline_table['assess_date'])
    data2 = data2.merge(baseline_table[['eid', 'dob', 'assess_date']], on='eid')
    data2['diag_age'] = (data2['epistart'] - data2['dob']).dt.days / 365.25
    data2['prev'] = data2['epiend'] < data2['assess_date']
    return data2.drop(columns=['dnx_hesin_diag_id','dnx_hesin_id'])

def read_ICD9(codes, folder='ukbb_data/', diagfile='HES_hesin_diag.csv', recordfile='HES_hesin.csv'):
    icd9_header = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'level', 'diag_icd9', 'diag_icd10', 'dnx_hesin_id', 'epistart', 'epiend']
    codes = [str(code) for code in codes]
    codes2 = [f",{code}" for code in codes]
    codes3 = '\\|'.join(codes2)
    grepcode = f'grep \'{codes3}\' {folder + diagfile} > temp.csv'
    run_command(grepcode)
    
    if os.path.getsize('temp.csv') == 0:
        return pd.DataFrame(columns=icd9_header)
    
    data = pd.read_csv('temp.csv', header=None)
    data.columns = ['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'classification', 'diag_icd9', 'diag_icd9_add', 'diag_icd10', 'diag_icd10_add']
    data = data[['dnx_hesin_diag_id', 'eid', 'ins_index', 'arr_index', 'classification', 'diag_icd9', 'diag_icd10']]
    
    icd9_code_data = [code.split(" ")[0] if isinstance(code, str) else "" for code in data['diag_icd9']]
    vec = [any(re.match(f"^{code}", icd9_code_data[i]) for code in codes) for i in range(len(icd9_code_data))]
    data = data[vec]
    
    records = pd.read_csv(folder + recordfile)
    data2 = data.merge(records, on=['eid', 'ins_index'])
    data2['epistart'] = pd.to_datetime(data2['epistart'])
    data2['epiend'] = pd.to_datetime(data2['epiend'])
    return data2.drop(columns=['dnx_hesin_diag_id', 'dnx_hesin_id'])

def read_cancer(codes, folder='ukbb_data/', filename='cancer_participant.csv', baseline_filename='Baseline.csv'):
    cancer_header = ["eid", "reg_date", "site", "age", "histology", "behaviour", "dob", "assess_date", "diag_age", "prev", "code", "description"]
    if not codes:
        return pd.DataFrame(columns=cancer_header)
    
    run_command(f"sed -i 's/\"//g' {folder + filename}")
    codes2 = [f",{code}" for code in codes]
    codes3 = '\\|'.join(codes2)
    grepcode = f'grep \'{codes3}\' {folder + filename} > temp.csv'
    run_command(grepcode)
    
    if not pd.read_csv('temp.csv').shape[0]:
        return pd.DataFrame(columns=cancer_header)
    
    data = pd.read_csv('temp.csv', header=None)
    data.columns = pd.read_csv(file, nrows=1).columns
    
    ids = data.iloc[:, 0].repeat(22).values
    datesvars = [f'p40005_i{i}' for i in range(22)]
    cancersvars = [f'p40006_i{i}' for i in range(22)]
    agevars = [f'p40008_i{i}' for i in range(22)]
    histologyvars = [f'p40011_i{i}' for i in range(22)]
    behaviourvars = [f'p40012_i{i}' for i in range(22)]
    
    dateslist = data[datesvars].values.flatten()
    cancerslist = data[cancersvars].values.flatten()
    agelist = data[agevars].values.flatten()
    histologylist = data[histologyvars].values.flatten()
    behaviourlist = data[behaviourvars].values.flatten()
    
    data = pd.DataFrame({'eid': ids, 'reg_date': dateslist, 'site': cancerslist, 'age': agelist, 'histology': histologylist, 'behaviour': behaviourlist})
    data['reg_date'] = pd.to_datetime(data['reg_date'])
    
    codes4 = '|'.join(codes)
    data = data[data['site'].str.contains(codes4)]
    
    baseline_table = pd.read_csv(baseline_filename)
    baseline_table['dob'] = pd.to_datetime(baseline_table['dob'])
    baseline_table['assess_date'] = pd.to_datetime(baseline_table['assess_date'])
    data = data.merge(baseline_table[['eid', 'dob', 'assess_date']], on='eid')
    data['diag_age'] = (data['reg_date'] - data['dob']).dt.days / 365.25
    data['prev'] = data['reg_date'] < data['assess_date']
    data['code'] = data['histology'] + '/' + data['behaviour']
    
    icdo3 = pd.read_csv('ICDO3.csv', sep='\t')
    icdo3 = icdo3.rename(columns={'histology': 'description'})
    data = data.merge(icdo3, on='description', how='left')
    
    return data

def read_selfreport(codes, folder='ukbb_data/', file='selfreport_participant.csv'):
    data = pd.read_csv(folder + file)
    coding6 = pd.read_csv(folder + 'coding6.tsv', sep='\t')
    coding6 = coding6[coding6['coding'] > 1]
    
    outlines = []
    for code in codes:
        if len(coding6[coding6['coding'] == code]['meaning']) > 0:
            outline = data[data['p20002_i0'].str.contains(coding6[coding6['coding'] == code]['meaning'].values[0])].index
            outlines.extend(outline)
    
    return data.loc[outlines, ['eid']]

def read_selfreport_cancer(codes, folder='ukbb_data/', file='selfreport_participant.csv'):
    data = pd.read_csv(folder + file)
    coding3 = pd.read_csv(folder + 'coding3.tsv', sep='\t')
    coding3 = coding3[coding3['coding'] > 1]
    
    outlines = []
    for code in codes:
        if len(coding6[coding6['coding'] == int(code)]['meaning']) > 0:
            outline = data[data['p20002_i0'].str.contains(coding6[coding6['coding'] == int(code)]['meaning'].values[0], na=False)].index
            outlines.extend(outline)
    
    return data.loc[outlines, ['eid']]

def read_treatment(codes, folder='ukbb_date/', file='treatment_participant.csv'):
    data = pd.read_csv(file)
    coding4 = pd.read_csv(folder + 'coding4.tsv', sep='\t')
    coding4 = coding4[coding4['coding'] > 1]
    
    outlines = []
    for code in codes:
        if len(coding4[coding4['coding'] == code]['meaning']) > 0:
            outline = data[data['Treatment.medication.code...Instance.0'].str.contains(coding4[coding4['coding'] == code]['meaning'].values[0])].index
            outlines.extend(outline)
    
    return data.loc[outlines, ['eid']]

def first_occurence(ICD10='', GP='', OPCS='', cancer=''):
    ICD10_records = read_ICD10(ICD10).assign(date=lambda x: x['epistart']).loc[:, ['eid', 'date']].assign(source='HES')
    OPCS_records = read_OPCS(OPCS).assign(date=lambda x: x['opdate']).loc[:, ['eid', 'date']].assign(source='OPCS')
    GP_records = read_GP(GP).assign(date=lambda x: x['event_dt']).loc[:, ['eid', 'date']].assign(source='GP')
    cancer_records = read_cancer(cancer).assign(date=lambda x: x['reg_date']).loc[:, ['eid', 'date']].assign(source='Cancer_Registry')
    
    all_records = pd.concat([ICD10_records, OPCS_records, GP_records, cancer_records])
    all_records['date'] = pd.to_datetime(all_records['date'])
    all_records = all_records.sort_values('date').drop_duplicates('eid')
    
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
# GP_codes = ['XE2eD', '22K..']
# GP_records = read_GP(GP_codes)

# OPCS_codes = ['Z92.4', 'C29.2']
# OPCS_records = read_OPCS(OPCS_codes)

# ICD10_codes = ['E11', 'K52.8']
# ICD10_records = read_ICD10(ICD10_codes)
