from pyarrow import csv
import pyarrow.parquet as pq

# csv to parquet ETL
class Data:
    def __init__(self):
        pass
        
    def loadCsv(self, fileName):
        self.__csvFileName = fileName
        self.__ParquetFileName = fileName.replace('csv', 'parquet')
        self.__csvData = csv.read_csv(self.__csvFileName)
        print(f'load {self.__csvFileName}')

    def transformSaveAsParquet(self):
        pq.write_table(self.__csvData, self.__ParquetFileName)
        print(f'save {self.__ParquetFileName}')

    def loadParquet(self):
        self.__parquetData = pq.read_table(self.__ParquetFileName)
        print(f'load {self.__ParquetFileName}')

    def loadParquetFromFile(self, fileName):
        self.__ParquetFileName = fileName
        self.__csvFileName = fileName.replace('parquet', 'csv')
        self.__parquetData = pq.read_table(self.__ParquetFileName)
        print(f'load {self.__ParquetFileName}')

    def getCsv(self):
        return self.__csvData

    def getParquet(self):
        return self.__parquetData


if __name__ == '__main__':
    CSV_FILE = 'resources/prices.csv'
    data = Data()
    data.loadCsv(CSV_FILE)
    data.transformSaveAsParquet()
    data.loadParquet()

    csvTable = data.getCsv()
    print(csvTable)
    parquetTable = data.getParquet()
    print(parquetTable)
    pandasTable = parquetTable.to_pandas()
    print(pandasTable)
