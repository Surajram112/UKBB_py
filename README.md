# UKBB Health Care Records

This repository contains an ensemble of functions for use analyzing the UKBB records on DNA Nexus.

## Available Functions

- **`read_GP`**: Reads data from the GP clinical records. It takes a list of read3 or read2 codes and returns one line per matching record, including eid date, value, and read code.

- **`read_OPCS`**: Reads OPCS codes from the operation records. It takes a list of OPCS codes and returns eid, opdate, and code for each matching record.

- **`read_ICD10`**: Reads data from the HES records using ICD10. It performs an inner join on the diagnosis and returns the eid, code, in date, out date, and whether it was a primary or secondary diagnosis.

- **`read_ICD9`**: Reads data from the HES records using ICD9. It performs an inner join on the diagnosis but there is no data on ICD9 dates of diagnosis in the UKBB HES records.

- **`read_selfreport`**: Reads data from the UK Biobank's non-cancer self-reported illness codes. It takes a list of codes from https://biobank.ctsu.ox.ac.uk/crystal/coding.cgi?id=6 and returns a list of matching IDs.

## How to Use

### Setup

To set up the environment for running the Python scripts, you need to have Python installed along with the necessary packages. You can install the required packages using pip:

```sh
pip install pandas numpy scipy matplotlib seaborn statsmodels polars pyarrow fastparquet
```

```python
import subprocess
subprocess.run("curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/UKBB_Health_Records_New_Project.py > UKBB_Health_Records_New_Project.py", shell=True, check=True)
from UKBB_Health_Records_New_Project import *
```
### Loading data into your UkBiobank Project
```python
project_folder = 't1diabetes'
load_save_data(project_folder)
 ```
The project foler is where the data will be imported to.
project_folder = "name you want to give to the particular project you are going to be working on"

### Extracting Healthcare Records

You can use the functions provided to extract healthcare records. For example, to extract ICD10 records, you can run:

```python
ICD10_codes = ['E10', 'E11']
ICD_records = read_ICD10(ICD10_codes, project_folder)
```

This will return a DataFrame `ICD10_records` which will contain all HES records that match either E10 (Insulin-dependent diabetes mellitus) or E11 (Non-insulin-dependent diabetes mellitus). This can also be run on sub-codes, e.g. E11.3, for Diabetic Retinopathy.

### Combining Healthcare Sources

Many phenotypes can be defined in a variety of ways. For example, Frozen Shoulder can be defined by ICD10 code M75.0, GP codes N210., XE1FL, and XE1Hm or OPCS4 code W78.1.

The function `first_occurence` can take ICD, GP, OPCS, and cancer registry codes (also ICD10) and output the first date the phenotype appears and where it first appears. Running

```python
frozen_shoulder = first_occurence(ICD10='M75.0', GP=["N210.", "XE1FL", "XE1Hm"], OPCS='W78.1', cancer='', project_folder)
```

will return a DataFrame with three columns: the id, the date of the first frozen shoulder record, and the source that appeared in. For this phenotype, I don't need to query the cancer registry, so '' is used as the input.

### Longitudinal Primary Care Records

`read_GP` preserves the value from the GP records and can be used for longitudinal analysis. Using the read_3 code 22K.. for BMI, you can run `read_GP(['22K..'])` and it will return all BMI recordings in the GP records.

These are longitudinal and have the date in `event_dt` and the actual BMI value in `value1`, `value2`, or `value3`.

## Working on

- **`read_cancer`**: Reads data from the Cancer Registry data using ICD10. It returns the eid, date, and cancer type.

- **`read_selfreport_cancer`**: Reads data from the UK Biobank's cancer self-reported illness codes. It takes a list of codes from https://biobank.ctsu.ox.ac.uk/crystal/coding.cgi?id=3 and returns a list of matching IDs.

- **`first_occurence`**: Takes a list of ICD10, read3, OPCS, and cancer ICD10 codes and returns the date and source of the first occurrence of disease. It does not use ICD9, because the dates are not present in these records.

## Example Usage

Below is an example usage of the main script:

```python
import subprocess
subprocess.run("curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/UKBB_Health_Records_New_Project.py > UKBB_Health_Records_New_Project.py", shell=True, check=True)
from UKBB_Health_Records_New_Project import *
project_folder = 'test'
load_save_data(project_folder)

# Define read functions and other functionality here
GP_codes = ['XE2eD', '22K..']
GP_records = read_GP(GP_codes, project_folder)
```
