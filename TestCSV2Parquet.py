import unittest
from CSV2Parquet import CSV2Parquet


class TestCSV2Parquet(unittest.TestCase):

    def setUp(self):
        # assemble
        self.CSV_FILE = 'resources/prices.csv'
        self.PARQUET_FILE = 'resources/prices.parquet'
        self.CSV_FILE2 = 'resources/prices.out.csv'
        self.CSV_FILE3 = 'resources/prices3.csv'
        self.data = CSV2Parquet()

    def test_csv_to_parquet_is_reversible(self):
        """
        load csv, save to parquet, load parquet, save to 2nd csv file
        verify equality with pandas
        """

        # act
        self.data.load_from_file(csv_file=self.CSV_FILE)
        csv_table = self.data.get_csv()
        pandas_table = csv_table.to_pandas()

        # load csv file, save as parquet file, load parquet file, save as 2nd csv file
        self.data.save_csv_as_parquet_file()
        self.data.load_from_file(parquet_file=self.PARQUET_FILE)
        self.data.save_parquet_as_csv_file(csv_file=self.CSV_FILE2)

        # load 2nd csv file
        self.data.load_from_file(csv_file=self.CSV_FILE2)
        csv_table2 = self.data.get_csv()
        pandas_table2 = csv_table2.to_pandas()

        # assert
        self.assertTrue((pandas_table.columns == pandas_table2.columns).all())
        self.assertTrue((pandas_table.equals(pandas_table2)))

        print(pandas_table2)

    def test_different_dataframes_not_equal(self):
        """
        verify different dataframes are *not* equal
        """

        # act
        self.data.load_from_file(csv_file=self.CSV_FILE)
        csv_table = self.data.get_csv()
        pandas_table = csv_table.to_pandas()

        # load second csv file
        self.data.load_from_file(csv_file=self.CSV_FILE3)
        csv_table2 = self.data.get_csv()
        pandas_table2 = csv_table2.to_pandas()

        # assert
        self.assertFalse((pandas_table.columns == pandas_table2.columns).all())
        self.assertFalse((pandas_table.equals(pandas_table2)))

        print(pandas_table2)


if __name__ == '__main__':
    unittest.main()
