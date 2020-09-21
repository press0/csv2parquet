## csv2parquet

#### Getting Started

Install this application by running the following commands:

```bash
git clone https://github.com/press0/csv2parquet.git
cd csv2parquet
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
python -m unittest  TestData.py
```

The test objectives:

```text
load csv data into pyarrow.Table #1 
extract and transform csv data to parquet
save parquet data
load parquest data into pyarrow.Table #2
compare the 2 pyarrow.Tables (csv and parquet)
assert the schema, sample content are equal
display the data as a pandasTable
```
