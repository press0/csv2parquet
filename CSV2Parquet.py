from pyarrow import csv
import pyarrow.parquet as pq
import pandas as pd
import sys


# CSV <=> Parquet transform utility.
class CSV2Parquet:
    def __init__(self):
        self.__csv_file = None
        self.__csv_data = None
        self.__parquet_file = None
        self.__parquet_data = None

    def load_from_file(self, csv_file=None, parquet_file=None):
        """load a csv or parquet formatted file with Apache Arrow.
        Keyword arguments:
        csv_file -- path to a csv formatted file
        parquet_file -- path to a parquet formatted file
        """
        if csv_file is None and parquet_file is None:
            sys.exit('required: either csv_file or parquet_file')
        elif csv_file is not None and parquet_file is not None:
            sys.exit('required: either csv_file or parquet_file')
        elif csv_file is not None:
            self.__csv_file = csv_file
            self.__csv_data = csv.read_csv(self.__csv_file)
            print(f'load {self.__csv_file}')
        elif parquet_file is not None:
            self.__parquet_file = parquet_file
            self.__parquet_data = pq.read_table(self.__parquet_file)
            # self.__parquet = pq
            print(f'load {self.__parquet_file}')

    def save_csv_as_parquet_file(self, parquet_file=None):
        """save a previously loaded csv file in parquet format.
        change the file name by substituting 'csv' for 'out.parquet'
        """
        if self.__csv_data is not None:
            if parquet_file is None:
                self.__parquet_file = self.__csv_file.replace('csv', 'out.parquet')
            else:
                self.__parquet_file = parquet_file

            pq.write_table(self.__csv_data, self.__parquet_file)
            print(f'save {self.__parquet_file}')

    def save_parquet_as_csv_file(self, csv_file=None):
        """save a previously loaded parquet file in csv format.
        change the file name by substituting 'parquet' for 'out.csv'
        """
        if self.__parquet_data is not None:
            if csv_file is None:
                self.__csv_file = self.__parquet_file.replace('parquet', 'out.csv')
            else:
                self.__csv_file = csv_file

            pydict = self.__parquet_data.to_pydict()
            pd.DataFrame(pydict).to_csv(self.__csv_file, index=False)
            print(f'save {self.__csv_file}')

    def get_csv(self):
        """return Apache Arrow csv table"""
        return self.__csv_data

    def get_parquet(self):
        """return Apache Arrow parquet table"""
        return self.__parquet_data


if __name__ == '__main__':
    CSV_FILE = 'resources/prices.csv'
    PARQUET_FILE = 'resources/prices.parquet'
    CSV_FILE2 = 'resources/prices.out.csv'
    util = CSV2Parquet()

    util.load_from_file(csv_file=CSV_FILE)
    csvTable = util.get_csv()
    print('-------> original csv:')
    print(csvTable)

    util.save_csv_as_parquet_file()
    util.load_from_file(parquet_file=PARQUET_FILE)
    parquetTable = util.get_parquet()
    print('-------> csv converted to parquest:')
    print(parquetTable)

    util.save_parquet_as_csv_file()
    util.load_from_file(csv_file=CSV_FILE2)
    csvTable = util.get_csv()
    print('-------> csv converted to parquest converted *back* to csv:')
    print(csvTable)
