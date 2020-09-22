import unittest
from Data import Data

class TestData(unittest.TestCase):
    def setUp(self):
        self.data = Data()


class TestData(TestData):

    def test_aws_glue_parquet_etl(self):

        #assemble
        PARQUET_FILE = 'resources/part-00000-238c0762-29e8-444f-904d-25b960b8717c-c000.snappy.parquet'

        #act
        self.data.loadParquetFromFile(PARQUET_FILE)
        parquetTable = self.data.getParquet()

        #assert
        pandasTable = parquetTable.to_pandas()
        print(pandasTable)


if __name__ == '__main__':
    unittest.main()
