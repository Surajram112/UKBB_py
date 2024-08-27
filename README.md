# UKBB Health Care Records

This repository contains an ensemble of functions for use analyzing the UKBB records on DNA Nexus.

## Available Functions

- **`read_GP`**: Reads data from the GP clinical records. It takes a list of read3 or read2 codes and returns one line per matching record, including eid date, value, and read code.

- **`read_OPCS`**: Reads OPCS codes from the operation records. It takes a list of OPCS codes and returns eid, opdate, and code for each matching record.

- **`read_ICD10`**: Reads data from the HES records using ICD10. It performs an inner join on the diagnosis and returns the eid, code, in date, out date, and whether it was a primary or secondary diagnosis.

- **`read_ICD9`**: Reads data from the HES records using ICD9. It performs an inner join on the diagnosis but there is no data on ICD9 dates of diagnosis in the UKBB HES records.

- **`read_cancer`**: Reads data from the Cancer Registry data using ICD10. It returns the eid, date, and cancer type.

- **`read_selfreport`**: Reads data from the UK Biobank's non-cancer self-reported illness codes. It takes a list of codes from https://biobank.ctsu.ox.ac.uk/crystal/coding.cgi?id=6 and returns a list of matching IDs.

- **`read_selfreport_cancer`**: Reads data from the UK Biobank's cancer self-reported illness codes. It takes a list of codes from https://biobank.ctsu.ox.ac.uk/crystal/coding.cgi?id=3 and returns a list of matching IDs.

- **`first_occurence`**: Takes a list of ICD10, read3, OPCS, and cancer ICD10 codes and returns the date and source of the first occurrence of disease. It does not use ICD9, because the dates are not present in these records.

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
### Extracting Healthcare Records

You can use the functions provided to extract healthcare records. For example, to extract ICD10 records, you can run:

```python
ICD10_codes = ['E10', 'E11']
ICD10_records = read_ICD10(ICD10_codes)
```

This will return a DataFrame `ICD10_records` which will contain all HES records that match either E10 (Insulin-dependent diabetes mellitus) or E11 (Non-insulin-dependent diabetes mellitus). This can also be run on sub-codes, e.g. E11.3, for Diabetic Retinopathy.

### Combining Healthcare Sources

Many phenotypes can be defined in a variety of ways. For example, Frozen Shoulder can be defined by ICD10 code M75.0, GP codes N210., XE1FL, and XE1Hm or OPCS4 code W78.1.

The function `first_occurence` can take ICD, GP, OPCS, and cancer registry codes (also ICD10) and output the first date the phenotype appears and where it first appears. Running

```python
frozen_shoulder = first_occurence(ICD10='M75.0', GP=["N210.", "XE1FL", "XE1Hm"], OPCS='W78.1', cancer='')
```

will return a DataFrame with three columns: the id, the date of the first frozen shoulder record, and the source that appeared in. For this phenotype, I don't need to query the cancer registry, so '' is used as the input.

### Longitudinal Primary Care Records

`read_GP` preserves the value from the GP records and can be used for longitudinal analysis. Using the read_3 code 22K.. for BMI, you can run `read_GP(['22K..'])` and it will return all BMI recordings in the GP records.

These are longitudinal and have the date in `event_dt` and the actual BMI value in `value1`, `value2`, or `value3`.

### Combining with Baseline Data

To combine the output with baseline data, you can use pandas' merge function. For example, if you have a DataFrame with the baseline data called `baseline`, you can run:

```python
frozen_shoulder = first_occurence(ICD10='M75.0', GP=["N210.", "XE1FL", "XE1Hm"], OPCS='W78.1', cancer='')
frozen_shoulder['FS'] = 1
all_data = baseline.merge(frozen_shoulder, on='eid', how='left')
all_data['FS'] = all_data['FS'].fillna(0)
```

`all_data` will now contain the frozen shoulder records, with a column `FS` which is 1 if they were in the record and 0 if they weren't.

## Known Issues

The script relies on grepping entire lines from a CSV. As ICD9 codes are purely numeric, these can match on participant IDs and cannot currently be read in. We're working on replacing the grep with an awk command which should address this issue and allow ICD9 codes to be read.

The line-level grep might also accidentally tag other things that happen to match. This is still in testing, and we'd recommend looking at the data to check there isn't anything in there you don't want.

## Example Usage

Below is an example usage of the main script:

```python
import subprocess
subprocess.run("curl https://raw.githubusercontent.com/Surajram112/UKBB_py/main/UKBB_Health_Records_New_Project.py > UKBB_Health_Records_New_Project.py", shell=True, check=True)
from UKBB_Health_Records_New_Project import *

# Define read functions and other functionality here
GP_codes = ['XE2eD', '22K..']
GP_records = read_GP(GP_codes)
```
