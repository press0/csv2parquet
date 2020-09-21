import unittest
from Data import Data

class TestData(unittest.TestCase):
    def setUp(self):
        self.data = Data()


class TestCsvData(TestData):

    def test_csv_to_parquet_etl(self):

        #assemble
        CSV_FILE = 'resources/prices.csv'

        #act
        self.data.loadCsv(CSV_FILE)
        self.data.transformSaveAsParquet()
        self.data.loadParquet()
        csvTable = self.data.getCsv()
        parquetTable = self.data.getParquet()

        #assert
        self.assertEqual(csvTable.num_columns, parquetTable.num_columns)
        self.assertEqual(csvTable.num_rows, parquetTable.num_rows)
        self.assertEqual(csvTable.columns, parquetTable.columns)
        self.assertEqual(csvTable.shape, parquetTable.shape)
        self.assertEqual(csvTable.schema, parquetTable.schema)

        pandasTable = parquetTable.to_pandas()
        print(pandasTable)


if __name__ == '__main__':
    unittest.main()
