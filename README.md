## csv2parquet

Simple [Apache Arrow](https://arrow.apache.org/overview/) CSV <=> Parquet transform utility.
  
 - [CSV2Parquet.py](CSV2Parquet.py)

#### Motivation
Big data platforms rely on Parquet; data analysts rely on CSV. 
 
#### Getting Started

Install the application by running the following commands:

```bash
git clone https://github.com/press0/csv2parquet.git
cd csv2parquet
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Test objective

verify the CSV <=> Parquet transform is reversible.  

Approach:
  
    transform a csv file into a Parquet file  
    transform the Parquet file back to a 2nd CSV file  
    transform the 2 CSV files into Pandas DataFrames  
    compare the two Pandas DataFrames for equality

 - [TestCSV2Parquet.py](TestCSV2Parquet.py)

```text
python TestCSV2Parquet.py
```

###AWS Glue ETL

this PySpark script performs the CSV to Parquet transform on the AWS Glue service
 - [AwsCvs2ParquetGlue.py](AwsCvs2ParquetGlue.py)

Approach:
    
    create AWS Glue crawler  
    create AWS Glue Data Catalog table  
    create AWS Glue ETL job 
    create AWS IAM policy   
    create AWS S3 bucket   
    upload CSV file to S3
    run AWS Glue crawler
    run AWS Glue ETL job
    download Parquet file from S3
    load parquest data into pyarrow.Table
   
 todo: CloudFormation template