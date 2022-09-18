import psycopg2
from collections import defaultdict 
from math import radians, cos, sin, asin, sqrt
from fuzzywuzzy import fuzz


#new import 

from json.encoder import INFINITY
import glob
import pandas as pd, glob
import os


class DataIngestionPipeline:

	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'postgres'
		self.password = 'EiOiJja3ZqaTlrcnQyNzh'
		self.database = 'kesari_bharat'
		self.port = 5432


	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 

	def excelToCSV(self):


		file_format = 'xlsx'
		pwd = os.getcwd()
		os.chdir('../All_Excel_data_new')
		path = os.getcwd()

		dirctories = os.listdir(path)
		INFINITY_ERROR = [] 

		for direcotry_name in dirctories:

			os.chdir(f'./{direcotry_name}')
			path = os.getcwd()
			names = glob.glob(f'{path}/*.{file_format}')

			for i in names:

				try:

					file_name_without_extenstion = i.replace(f'.{file_format}','')
					
					df = pd.DataFrame(pd.read_excel(i))

					df.to_csv(file_name_without_extenstion + '.csv')

					print("file_name ", i, "\n  has merged into csv")

				except Exception as e:
					print(str(e))
					print("file = ", i)
					INFINITY_ERROR.append(i)
			os.chdir("../")

		print(INFINITY_ERROR)
		file = open("INFINITY_ERROR_file.txt", 'w')
		file.writelines(str(INFINITY_ERROR))
	



if __name__ == '__main__':


	ingestion_object = DataIngestionPipeline() 
	ingestion_object.excelToCSV()
