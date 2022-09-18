import psycopg2
import os 
import glob
import csv 


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


	def sendCsvData(self):


		sqlExecuter, connection = self.makeConnection() 

		pwd = os.getcwd()
		os.chdir("../All_Excel_data_new")
		pwd = os.getcwd()
		directories = os.listdir()
		file_format = 'csv'


		for directory in directories[:]:
			
			# if directory in ['one', 'two', 'three', 'four']: continue
			os.chdir(f"./{directory}")
			path = os.getcwd()
			names = glob.glob(f'{path}/*.{file_format}')

			query = f"""

					create table if not exists scrapping_data.{directory} 
					as table public.deepak_sir_round_5 with no data;

			"""
			print(query)
			sqlExecuter.execute(query)

			
			query = f"""

				alter table scrapping_data.{directory} 
				drop column if exists id,
				add column id serial primary key;
			"""
			print(query)
			sqlExecuter.execute(query)


			for file_name in names:
            
				file = open(f'{file_name}',encoding="utf-8",  errors = 'ignore' )
				csvreader = csv.reader(file)
				header =  next(csvreader)

				for row in csvreader:

					try:
						x, y, name, category, email, phone, address, latitude, longitude, website, rating, reviews, closed = row 

					except Exception as e:

						x, y, name, category, email, phone, address, latitude, longitude, website, rating, reviews  = row 

					latitude = float(latitude)
					longitude = float(longitude)
					email = email.replace("'", '').replace('"', '') if email else None 
					website = website.replace("'", '').replace('"', '') if website else None 
					address = address.replace("'", "").replace('"', "") if address else None 
					category = category.replace("'", "").replace('"', "") if category else None 
					name = name.replace("'", "").replace('"', "")
					phone = phone if phone else 0

					query  =f"""

						insert into scrapping_data.{directory}
						(name, category_name, address, email, phone_number, website, rating, reviews, latitude, longitude)
						values('{name}', '{category}', '{address}', '{email}', '{phone}',
						'{website}', '{rating}', '{reviews}', {latitude}, {longitude})

					"""

					print(query)
					sqlExecuter.execute(query)
					connection.commit() 
					# return




			print(path)
			os.chdir("../")




if __name__ == "__main__":

	pipeline_object = DataIngestionPipeline()
	pipeline_object.sendCsvData() 
