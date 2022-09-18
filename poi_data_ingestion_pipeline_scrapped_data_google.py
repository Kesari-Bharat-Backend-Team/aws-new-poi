import psycopg2
import re 
from collections import defaultdict 
from math import radians, cos, sin, asin, sqrt
from fuzzywuzzy import fuzz


class DataIngestionPipeline:

	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'postgres'
		self.password = 'EiOiJja3ZqaTlrcnQyNzh'
		self.database = 'kesari_bharat'
		self.port = 5432
		self.sub_locality_hash = defaultdict(bool)

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 


	def distance(self, lat1, lat2, lon1, lon2):
     
		# The math module contains a function named
		# radians which converts from degrees to radians.
		lon1 = radians(lon1)
		lon2 = radians(lon2)
		lat1 = radians(lat1)
		lat2 = radians(lat2)
		
		# Haversine formula
		dlon = lon2 - lon1
		dlat = lat2 - lat1
		a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
	
		c = 2 * asin(sqrt(a))
		
		# Radius of earth in kilometers. Use 3956 for miles
		r = 6371
		
		# calculate the result
		return round( (c * r), 3)
     
		
	def getfwEditDistance(self, string1, string2):
		if string1 and string2:
			Ratio = fuzz.ratio(string1.strip().lower(),string2.strip().lower())
			return Ratio
		return 0


	def pointInPolygonDtCode(self, schema_name, primary_key = 'id'):

		sqlExecuter, connection = self.makeConnection()


		query = f"""
				update  {schema_name}
				set geom = st_makepoint(longitude, latitude)
				where geom is null

		"""
		sqlExecuter.execute(query)
		connection.commit() 


		query = f"""
				
			select updategeometrysrid('{schema_name.split(".")[0]}','{schema_name.split(".")[1]}', 'geom', 4326);

		"""
		sqlExecuter.execute(query)
		connection.commit() 

		query = f"""

				select {primary_key}, geom 
				from {schema_name}
				where (dt_code = 0 or dt_code is null) and geom is not null  
				and (flag = 0 or flag is null)
		"""

		print(query)
		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall() 

		for data in records:

			my_id, geom = data 

			query = f"""

					select 
					polygon.district.kb_lev3_id  from polygon.district

					where 
					
						ST_intersects(
						polygon.district.geom,
						'{geom}'
					) 

			"""
			print(query)

			sqlExecuter.execute(query)
			kb_lev3_id = sqlExecuter.fetchall()
			if kb_lev3_id:
				kb_lev3_id = kb_lev3_id[0][0]
			else:
				print(kb_lev3_id)
				kb_lev3_id = -1

			
			query = f"""

				update {schema_name}
				set dt_code = {kb_lev3_id},
				flag = 1
				where {primary_key} = {my_id}

			"""
			sqlExecuter.execute(query)
			connection.commit() 
			print(query)




	def ingestDataWithLatLongcheck(self, schema_name, primary_key = "id"):

		sqlExecuter, connection = self.makeConnection() 

		query = f"""
		select {primary_key}, name, category_name, address, email, phone_number, website, rating, reviews, latitude, longitude, dt_code from {schema_name} where flag = 1 and (inserted_flag is Null or inserted_flag = 0)  and dt_code is not null and dt_code > 0  and length(name) > 1 and  
		remove_anomalies_flag = 1

		"""

		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall() 

		for data in records:

			google_id, google_name, google_category_name, google_address, google_email, google_phone_number, google_website, google_rating, google_reviews, google_latitude, google_longitude, google_dt_code = data 
			data_source = schema_name

			query = f"""
					select name, id, latitude, longitude, address, email, phone_1, website from jan_2022_poi.poi_{google_dt_code}
					where name ilike '%{google_name.strip()[1:12]}%'
			"""
			# print(query)
			sqlExecuter.execute(query)
			poi_records = sqlExecuter.fetchall()
			match_flag = False 

			for poi_data in poi_records:

				poi_name, poi_id, poi_latitude, poi_longitude, poi_address, poi_email, poi_phone_number, poi_website = poi_data 
				poi_latitude = float(poi_latitude)
				poi_longitude = float(poi_longitude)

				total_distance =  self.distance(google_latitude, poi_latitude, google_longitude, poi_longitude)
				ratio = self.getfwEditDistance(google_name, poi_name)


				if total_distance <= 0.2 and  ratio >= 90:

					
					if ( sum([1 for ch in google_name if ch.isdigit()]) >= 1 or 'block' in google_name.lower() or 'sector' in google_name.lower() ) or (sum([1 for ch in poi_name if ch.isdigit()]) >= 1 or 'block' in poi_name.lower() or 'sector' in poi_name.lower() ):


						if ratio >= 98 and total_distance <= .06:
								pass 
						else:

							print("we are in continue")
							print(total_distance, "Ratio = > ", ratio)
							print(google_name , " \t\t\t ", "poi ", poi_name)
							print("*" * 150, '\n' * 5)
							# return
							continue


				#Fixing phone_number 
					if poi_phone_number  and google_phone_number and len(poi_phone_number) > 20:
						final_phone_number = poi_phone_number + ", " + google_phone_number
						final_phone_number = final_phone_number.replace(" ", "").strip()
					
					elif poi_phone_number and   google_phone_number and  "000000" not in poi_phone_number and "000000" not in google_phone_number and len(poi_phone_number) >= 10 and len(google_phone_number) >= 10:
							

							poi_phone_number = poi_phone_number.replace(" ", "").strip()
							google_phone_number = google_phone_number.replace(" ", "").strip()


							if poi_phone_number[-9:] == google_phone_number[-9:]: 
								final_phone_number = google_phone_number.replace(" ", "")
							else:
								final_phone_number = ",".join(set([poi_phone_number.replace(" ", ""), google_phone_number.replace(" ", "")]))

					else:

						final_phone_number = google_phone_number if "000000" not in google_phone_number else poi_phone_number

						if final_phone_number and len(final_phone_number) < 10:
							if  len(google_phone_number) >= 10:
								final_phone_number = google_phone_number
							else:
								final_phone_number = poi_phone_number

						final_phone_number = final_phone_number.replace(" ", "").strip() if final_phone_number else final_phone_number #ha ha ha 
					
				#fixing website 

					#checking both 
					if google_website and poi_website and len(google_website) > 5 and len(poi_website) > 5:

						if r'\u00' in poi_website:
							final_website = google_website
						else:
							final_website = max(poi_website, google_website)


					elif google_website and len(google_website) > 5 :

						final_website = google_website

					else:
						final_website = poi_website 

					# print("\nFinal website = ", final_website) 

				#fixing email 


					if google_email and poi_email and len(google_email) > 5 and len(poi_email) > 5:

						final_email = google_email
					else:

						final_email = google_email if google_email else poi_email

				#fixing address 

					final_address = google_address if google_address and len(google_address) > 5 else poi_address

					query = f"""

						update jan_2022_poi.poi_{google_dt_code}

						SET  
						category = '{google_category_name}',
						 address = '{final_address}',
						  email = '{final_email}',
						   phone_1 = '{final_phone_number}',
						    website = '{final_website}',
							 rating = {google_rating},
							  reviews = {google_reviews},
							data_source = 'updated from {schema_name}'
												where id = {poi_id}
					""".replace("None", "NULL")

					sqlExecuter.execute(query)
					print(query)
					connection.commit() 
					print("\n" * 2, "*" * 150)

					match_flag = True 
					break  #
			

			if match_flag == False:

				print('We are inserting data')

				data = tuple([google_name, google_category_name, google_address, google_email, google_phone_number, google_website, google_rating, google_reviews, google_latitude, google_longitude, data_source])


				query = f"""
				INSERT INTO jan_2022_poi.poi_{google_dt_code}
					(name, category, address, email, phone_1, website, rating, reviews, latitude, longitude, data_source)
				values {data}
				""".replace("None", "NULL")
				sqlExecuter.execute(query)
				print(query)
				connection.commit() 
				print("\n" * 2, "*" * 150)

			
			query = f""" 
					update {schema_name}
					set inserted_flag = 1
					where {primary_key} = {google_id}
			"""
			print(query)
			sqlExecuter.execute(query)
			connection.commit()

		print("Done !!!", schema_name)
		connection.close() 


	def refineScrappedDataBeforeSending(self, schema_name = 'public.google_raghav', primary_key = 'id'):

		sqlExecuter, connection = self.makeConnection() 

		query = f"""

				select name, {primary_key}, category_name, address
				FROM {schema_name} 
				 where remove_anomalies_flag is null or remove_anomalies_flag != 1 
		"""
		print(query)

		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall() 
		

		for data in records:

			name, my_id, category_name, address  = data 

			#fixing name 

			print("old name = ", name)
			if name and len(name) >= 1:

				name = name.replace("*", "").replace("#", "").replace("...", "").replace("..", "").replace("..", "").replace("?", "").replace(r"\\", "").replace("₹", "").replace("%", "").replace("^", "").replace("@@", "").replace(":", "")

				if "_xd" in name.lower():
					name = name.lower().split('_') # Sana chichen center _xD83D__xDC14_
					new_name = ''
					for word in name:

						if 'xd' not in word.lower():
							new_name = new_name + " " + (word)

					name = new_name.replace("  ", " ").replace("  ", " ").replace(".", "").strip().title()
				


				name = name.replace("  ", " ").replace("  ", " ").strip().title() 
				print("new name = ", name)

			#fixing categoryies 

			if category_name and len(category_name) >= 1:

				print("old cat ", category_name )

				category_name = category_name.split(" in ")[0]
				category_name = category_name.split(",")[0]
				
				category_name = category_name.replace("*", "").replace("#", "").replace("...", "").replace("..", "").replace("..", "").replace("?", "").replace(r"\\", "").replace("₹", "").replace("%", "").replace("^", "").replace("@@", "") .replace(":", "")

				category_name = category_name.replace("  ", " ").replace("  ", " ").strip().title() 
				print(category_name)

			#fixing address 
			
			if address and len(address) >= 1:
					
				address = address.replace("*", "").replace("#", "").replace("...", "").replace("..", "").replace("..", "").replace("?", "").replace(r"\\", "").replace("₹", "").replace("%", "").replace("^", "").replace("@@", "").replace("'", "").replace('"', "").replace(":", "")

				address = address.replace("  ", " ").replace("  ", " ").strip().title()


			query = f"""

					UPDATE {schema_name}
					set name = '{name}',
					address = '{address}',
					remove_anomalies_flag = 1,
					category_name = '{category_name}'

					where {primary_key} = {my_id}

			"""

			print(query)
			sqlExecuter.execute(query)
			connection.commit() 


			print( "\n" * 2 , "*" * 150)
		print("Done !!!", schema_name)


	def deleteDuplicatePoi(self, schema_name):

		sqlExecuter, connection = self.makeConnection() 

		query = f"""
		delete  
		from
			{schema_name} a
			using {schema_name} b 
		where a.id < b.id 
		and lower(a.name) = lower(b.name)
		and a.latitude = b.latitude
		and a.longitude = b.longitude;
		"""
		print(query)
		sqlExecuter.execute(query)
		connection.commit() 
		print("Done!!! ", schema_name)



	def AllInOneLogic(self):

		sqlExecuter, connection = self.makeConnection() 

		schema_name = "scrapping_data"

		# query = f"""

		# 		SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = '{schema_name}') 

		# """
		# sqlExecuter.execute(query)
		# all_table_names = sqlExecuter.fetchall() 
		# # print(all_table_names)
		# temp  = []

		# all_table_names = ['two', 'ten', 'one', 'six', 'three', 'eight', 'eighteen', 'four', 'sixteen', 'seven', 'fourteen', 'seventheen', 'five', 'twelve', 'six_temp', 'twenty', 'thirteen', 'fifteen', 'nine', 'ninetheen', 'eleven']

		all_table_names = ['raghav', 'pradeep'] # List of table names which you have created during excel_to_csv ex-> All_Excel_data_new

		start = int(input("enter start = "))
		end = int(input("enter end = "))

		print("all_table_names = ", all_table_names)
		print("total = ", len(all_table_names))

		for table_name in all_table_names[start: end]:

			path = f"{schema_name}.{table_name}"

			print(path)
			
			#Step 1 Delete Duplicates 
			
			self.deleteDuplicatePoi(path)

			#step 2 Point In polygon 

			self.pointInPolygonDtCode(path, "id")

			#step 3 Filter name, category, phone etc before ingesting 

			self.refineScrappedDataBeforeSending(path, "id")

			#step 4 Ingest data in "jan_2022_poi" Finally 

			self.ingestDataWithLatLongcheck(path, "id")

	
	def countInsertedData(self):

		sqlExecuter, connection = self.makeConnection()

		start = 1
		end = 736

		total_insrted_data = 0


		for i in range(start, end):

			schema_name = f"jan_2022_poi.poi_{i}"

			query = f"""

				select count(*) from {schema_name}
				where data_source not like '%updated%' 
				and data_source ilike '%scrapping_data%'
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()[0][0]

			total_insrted_data += records
			print(records)
		print(total_insrted_data)



if __name__ == "__main__":

	pipeline_object = DataIngestionPipeline()
	# pipeline_object.AllInOneLogic()
	# pipeline_object.countInsertedData() 
	


	# schema_name = 'public.fastractor_deepak_sir_august'



	# schema_name = 'public.fastractor_deepak_sir_august_2'

	# pipeline_object.deleteDuplicatePoi(schema_name)
	# pipeline_object.refineScrappedDataBeforeSending(schema_name, "id") # we have to pass the schema and id here
	# pipeline_object.pointInPolygonDtCode(schema_name, "id")
	# pipeline_object.ingestDataWithLatLongcheck(schema_name, "id")
	
