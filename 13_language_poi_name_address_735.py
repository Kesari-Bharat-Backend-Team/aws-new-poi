import psycopg2
import mtranslate as mp
import pandas as pd,time

class MultiLanguageTranslate:

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


	def doTranslation(self):


		sqlExecuter, connection = self.makeConnection()

		try:
				
			for i in range(1, 200):

				query = f"""

					select name, id from 
					jan_2022_poi.poi_{i}
					where name_translate_flag = 0 or name_translate_flag is null
				"""
				sqlExecuter.execute(query)

				poi_records = sqlExecuter.fetchall() 

				for data in poi_records:

					poi_name, poi_id = data 

					try:

						bengali = mp.translate(poi_name, "bn", "auto").replace("'", "").replace('"', "")
						gujrati = mp.translate(poi_name, "gu", "auto").replace("'", "").replace('"', "")
						hindi = mp.translate(poi_name, "hi", "auto").replace("'", "").replace('"', "")
						kannada = mp.translate(poi_name, "kn", "auto").replace("'", "").replace('"', "")
						malayalam = mp.translate(poi_name, "ml", "auto").replace("'", "").replace('"', "")
						oriya = mp.translate(poi_name, "or", "auto").replace("'", "").replace('"', "")
						punjabi = mp.translate(poi_name, "pa", "auto").replace("'", "").replace('"', "")
						sindhi = mp.translate(poi_name, "sd", "auto").replace("'", "").replace('"', "")
						tamil = mp.translate(poi_name, "ta", "auto").replace("'", "").replace('"', "")
						telegu = mp.translate(poi_name, "te", "auto").replace("'", "").replace('"', "")
						urdu = mp.translate(poi_name, "ur", "auto").replace("'", "").replace('"', "")

					except Exception as e:
						print("error aa gayi hai ", id)
						print(str(e))

					translated_names = tuple([bengali, gujrati, hindi, kannada, malayalam, oriya, punjabi, sindhi, tamil, telegu, urdu, poi_id])

					query = f"""

						insert into translated_poi_name.poi_{i}

						(name,bengali, gujrati, hindi, kannada, malayalam, oriya, punjabi, sindhi, tamil, telegu, urdu, poi_id)


						values ('{poi_name}', '{bengali}', '{gujrati}', '{hindi}', '{kannada}', '{malayalam}', '{oriya}', '{punjabi}', '{sindhi}', '{tamil}', '{telegu}', '{urdu}', {poi_id}) ;
					"""

					print(query)
					print("*" * 150, '\n\n')
					sqlExecuter.execute(query)

					query = f"""
						update jan_2022_poi.poi_{i}
						set name_translate_flag = 1 
						where id = {poi_id}
					"""
					sqlExecuter.execute(query)
					connection.commit()
					time.sleep(1)
				
				print("Done!!! ", i)

		except Exception as e:
			time.sleep(100)
			self.doTranslation() 

	def addTables(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(3, 736):

			query = f"""
			create table 
			translated_poi_name.poi_{i}
			as table translated_poi_name.poi_1 with no data;
			"""
			sqlExecuter.execute(query)
			connection.commit() 


	def addColumn(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(1, 736):

			query = f"""

					ALTER TABLE IF EXISTS jan_2022_poi.poi_{i}
					ADD COLUMN if not exists name_translate_flag integer default 0;

			"""
			sqlExecuter.execute(query)
			connection.commit() 
		
			print("Column Added !!!", i)



	def countGoogleData(self):

		sqlExecuter, connection = self.makeConnection() 

		total = 0 
		for i in range(1,736):

			query = f"""
			
						select count(*) from jan_2022_poi.poi_{i}
						where data_source ='public.sanjay_round_4'
			"""

			sqlExecuter.execute(query)

			records = sqlExecuter.fetchall()[0][0]
			total += records
			print(records)

		print(total)


	

if __name__ == '__main__':

	translate_object = MultiLanguageTranslate() 
	# translate_object.addColumn() 
	translate_object.doTranslation() 
	# translate_object.countGoogleData() 

	# translate_object.addTables() 

