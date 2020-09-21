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

What the test does:

```text
load csv data as a pyarrow.Table 
extract and transform csv data to parquet
save parquet data
load parquest data as another pyarrow.Table
compare the 2 pyarrow.Tables (csv and parquet)
display the data as a pandasTable
```
