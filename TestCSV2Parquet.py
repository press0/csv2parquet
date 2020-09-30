import unittest
from CSV2Parquet import CSV2Parquet


class TestCSV2Parquet(unittest.TestCase):

    def test_csv_to_parquet_is_reversible(self):
        """
        load csv, save to parquet, load parquet, save to 2nd csv file
        verify equality with pandas
        """

        # assemble
        CSV_FILE = 'resources/prices.csv'
        PARQUET_FILE = 'resources/prices.parquet'
        CSV_FILE2 = 'resources/prices.out.csv'
        data = CSV2Parquet()

        # act
        data.load_from_file(csv_file=CSV_FILE)
        csv_table = data.get_csv()
        pandas_table = csv_table.to_pandas()

        # load csv file, save as parquet file, load parquet file, save as 2nd csv file
        data.save_csv_as_parquet_file()
        data.load_from_file(parquet_file=PARQUET_FILE)
        data.save_parquet_as_csv_file(csv_file=CSV_FILE2)

        # load 2nd csv file
        data.load_from_file(csv_file=CSV_FILE2)
        csv_table2 = data.get_csv()
        pandas_table2 = csv_table2.to_pandas()

        # assert
        self.assertTrue((pandas_table.columns == pandas_table2.columns).all())
        self.assertTrue((pandas_table.equals(pandas_table2)))

        print(pandas_table2)

    def test_different_dataframes_not_equal(self):
        """
        verify different dataframes are *not* equal
        """

        # assemble
        CSV_FILE = 'resources/prices.csv'
        CSV_FILE3 = 'resources/prices3.csv'
        data = CSV2Parquet()

        # act
        data.load_from_file(csv_file=CSV_FILE)
        csv_table = data.get_csv()
        pandas_table = csv_table.to_pandas()

        # load second csv file
        data.load_from_file(csv_file=CSV_FILE3)
        csv_table2 = data.get_csv()
        pandas_table2 = csv_table2.to_pandas()

        # assert
        self.assertFalse((pandas_table.columns == pandas_table2.columns).all())
        self.assertFalse((pandas_table.equals(pandas_table2)))

        print(pandas_table2)


if __name__ == '__main__':
    unittest.main()
