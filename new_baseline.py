# process_baseline.py
import pandas as pd
import subprocess
import re
from datetime import datetime

# Function to run system commands
def run_command(command):
    subprocess.run(command, shell=True, check=True)

# Download and load baseline table
run_command('dx download file-GZPzVp0JkBXbqJjYZvzvkjg4')
# Download and load baseline table
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
baseline_table['mob'] = baseline_table['mob'].apply(lambda x: pd.to_datetime(x, format='%B').month)
# Create 'dob' column
baseline_table['dob'] = pd.to_datetime(baseline_table[['yob', 'mob']].assign(day=15))
# Convert 'assess_date' to datetime
baseline_table['assess_date'] = pd.to_datetime(baseline_table['assess_date'])
# Calculate 'assess_age' and 'whr' columns
baseline_table['assess_age'] = (baseline_table['assess_date'] - baseline_table['dob']).dt.days / 365.25
baseline_table['whr'] = baseline_table['waist'] / baseline_table['hip']

# Save the processed baseline table
baseline_table.to_csv('processed_baseline.csv', index=False)
