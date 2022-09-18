import psycopg2
from elastic_enterprise_search import AppSearch
import pandas as pd,time

class ElasticAppSearch:
	
	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'postgres'
		self.password = 'EiOiJja3ZqaTlrcnQyNzh'
		self.database = 'kesari_bharat'
		self.port = 5432

		self.endpoint = 'https://my-deployment-499492.ent.ap-south-1.aws.elastic-cloud.com'
		self.private_key = 'private-h9auc4cya44sx8464i52cggj'
		self.app_search = AppSearch(self.endpoint, self.private_key)

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 


	def createNewEngine(self, engine_name):

		try:
			self.app_search.create_engine(
				engine_name, 
				language = 'en',
			)
			print(f"engine name = '{engine_name.upper()} has created successfully.")
		
		except Exception as e:
			print(str(e))


	def updateIds(self, schema_name = 'jan_2022_poi.poi_1', array_of_ids = [1], primary_key = 'id'):

		sqlExecuter, connection = self.makeConnection() 

		if len(array_of_ids) == 1: #Important (12342,) 
			array_of_ids = f"({array_of_ids[0]})"
		else:
			array_of_ids = tuple(array_of_ids)

		query = f"""

			update {schema_name}
			set ingest_es_flag = 1 
			where {primary_key} in {array_of_ids}

		"""
		sqlExecuter.execute(query)
		connection.commit() 


	def ingestPOI(self, start=1, end = 736):

		sqlExecuter, connection = self.makeConnection() 
		engine_name = 'poi'
		self.createNewEngine(engine_name)
		level_rating = 3 

		for i in range(start, end):

			query = f"""

				select id, name, phone_1, email, 
				rating, category, category_code, address,
				website, latitude, longitude, pincode,
				opening_time, closing_time
				from jan_2022_poi.poi_{i}

				where ingest_es_flag = 0 and
				address is not null

			"""

			sqlExecuter.execute(query)

			print("Wait!! poi_", i, "is processing.")



			while True:

				records = sqlExecuter.fetchmany(size=100) #Fetch only 100 records because we can't ingest more then 100 in ES app search.

				if not records : break #base case We don't have data

				json_data = [] 
				array_of_ids = [] 

				for data in records:

					my_id, name, phone_number, email, rating, category,category_code, address, website, latitude, longitude, pincode,opening_time, closing_time = data
					# print(name)

					array_of_ids.append(my_id)
					
					website = website if website else 'www.kesari-bharat.com'

					email = email  if email else 'himanshu.kesari@gmail.com'

					category = category if category else 'Playground'

					category_code = category_code if category_code and len(category_code) > 5 else "400-4100-0035"

					opening_days = '{1,2,3,4,5,6,7}'
					opening_timing = 600
					closing_timing = 1080


					phone_number = phone_number.split(",")[0][:12] if phone_number else '8319050525'

					latitude = float(latitude)
					longitude = float(longitude)
					rating = float(rating) if rating else 5.0


					json_data.append({

							'name' : name,
							'rating' : rating,
							'website' :  website,
							'email' : email,
							'phone_number' : phone_number,
							'opening_days' : opening_days,
							'opening_timing' : opening_time,
							'closing_timing' : closing_time,
							'category' : category,
							'category_code' : category_code,
							'address' : address,
							'level_rating' : level_rating,

							'location' : f'{latitude}, {longitude}',
							'id' : f'poi_{i}.{my_id}'

					})


				try:

					self.app_search.index_documents(engine_name, json_data)
					self.updateIds(f"jan_2022_poi.poi_{i}", array_of_ids)
				
				except Exception as e:
					print("We are in recursion😍")
					time.sleep(60)
					self.ingestPOI(start=i, end=end)

	
	def addColumns(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(1, 736):

			query = f"""

				alter table 
				building_no.final_id_{i}
				/*jan_2022_poi.poi_{i}*/

				add column if not exists ingest_es_flag integer default 0;

			"""
			sqlExecuter.execute(query)
			print(query)
			# connection.commit()


	def ingestState(self, engine_name, level_rating):

		sqlExecuter, connection = self.makeConnection() 

		schema_name = 'try_admin_point.state_capitals'
		query = f"""

			select name, gid, address,
			st_x(geom), st_y(geom)
			from {schema_name}

		"""
		sqlExecuter.execute(query)
		
		while True:
			
			records = sqlExecuter.fetchmany(size = 100)

			if not records : break #base case 

			json_data = [] 

			for data in records:

				name, gid, address, longitude, latitude = data 


				json_data.append({

					'name' : name,
					'address': address,
					'level_rating' : level_rating,
					'type' : 'state_capitals',
					'location' : f"{latitude}, {longitude}",
					'id':  f'state_name.{gid}'
					
				})
			self.app_search.index_documents(engine_name, json_data)
			print("States are done now !!!")


	def ingestDistrictCapitals(self, engine_name, level_rating):

		sqlExecuter, connection = self.makeConnection() 

		schema_name = 'try_admin_point.district_capitals'
		query = f"""

			select name, gid, address,
			st_x(geom), st_y(geom)
			from {schema_name}

		"""
		sqlExecuter.execute(query)
		
		while True:
			
			records = sqlExecuter.fetchmany(size = 100)

			if not records : break #base case 

			json_data = [] 

			for data in records:

				name, gid, address, longitude, latitude = data 


				json_data.append({

					'name' : name,
					'address': address,
					'level_rating' : level_rating,
					'type' : 'district_capitals',
					'location' : f"{latitude}, {longitude}",
					'id':  f'district_capital.{gid}'
					
				})
			self.app_search.index_documents(engine_name, json_data)
			print("District are done now !!!")


	def ingestHabitantPoints(self, engine_name, level_rating):


		sqlExecuter, connection = self.makeConnection() 

		schema_name = 'try_admin_point.habitant'
		query = f"""

			select  hab_name, latitude, longitude, id,suggested_data
			from {schema_name}
			where ingest_es_flag = 0

		"""
		
		sqlExecuter.execute(query)

		print("Wait Habitation is prodessiong.")
		
		while True:
			
			records = sqlExecuter.fetchmany(size = 100)

			if not records : break #base case 

			json_data = [] 
			array_of_ids = [] 

			for data in records:

				name, latitude, longitude, id, suggested_data = data 
				print(name)

				latitude = float(latitude) if latitude else 26.922922
				longitude = float(longitude) if longitude else 75.778855

				array_of_ids.append(id)
				json_data.append({

					'name' : name,
					'address': suggested_data,
					'level_rating' : level_rating,
					'type' : 'habitant',
					'location' : f"{latitude}, {longitude}",
					'id':  f'habitant.{id}'
					
				})

			try:
				self.app_search.index_documents(engine_name, json_data)
				self.updateIds(schema_name, array_of_ids)

			except Exception as e:
					print("We are in recursion😍(Habitant)😁")
					time.sleep(60)
					self.ingestHabitantPoints(engine_name, level_rating)
		print("Habitants are done now !!!")


	def ingestSublocalities(self, engine_name, level_rating):


		sqlExecuter, connection = self.makeConnection() 

		schema_name = 'point.sub_locality_point_2'

		query = f"""

			select name, latitude, longitude, gid, address
			from {schema_name}
			where length(name) > 2

		"""
		sqlExecuter.execute(query)
		
		while True:
			
			records = sqlExecuter.fetchmany(size = 100)

			if not records : break #base case 

			json_data = [] 

			for data in records:

				name, latitude, longitude, gid, address = data 
				print(name)

				latitude = float(latitude) if latitude else 26.922922
				longitude = float(longitude) if longitude else 75.778855


				json_data.append({

					'name' : name.title(),
					'address': address,
					'level_rating' : level_rating,
					'type' : 'sub-locality-2',
					'location' : f"{latitude}, {longitude}",
					'id':  f'sub-locality.{gid}'
					
				})

			self.app_search.index_documents(engine_name, json_data)
		print("sub-localities are done now !!!")




	def ingestLocalities(self):

		sqlExecuter, connection = self.makeConnection() 
		engine_name = 'locality'
		self.createNewEngine(engine_name)
		level_rating = 4

		self.ingestState(engine_name, level_rating=5)
		self.ingestDistrictCapitals(engine_name, level_rating=5)
		self.ingestSublocalities(engine_name, level_rating)
		self.ingestHabitantPoints(engine_name, level_rating)

		print("Done!!!")


	def ingestBuildingNumbers(self, start = 1, end = 736):

		sqlExecuter, connection = self.makeConnection() 
		engine_name = 'building-address'
		self.createNewEngine(engine_name)
		level_rating = 2 

		for i in range(start, end):

			
			query = f"""

			select my_id, buildingna, address, latitude, longitude  
			from
			 building_no.final_id_{i}
			 where ingest_es_flag = 0

			"""
			# print(query)
			print("final_id_", i)
			sqlExecuter.execute(query)

			while True:

				records = sqlExecuter.fetchmany(size = 100)
				if not records: break 

				
				json_data = [] 
				array_of_ids = [] 
				for data in records:

					my_id, building_name, address, latitude, longitude = data 
					longitude = float(longitude)
					latitude = float(latitude)

					array_of_ids.append(my_id)

					json_data.append({

						'address' : address,
						'level_rating' : level_rating,
						'location' : f"{latitude}, {longitude}",
						"id" : f'final_id_{i}.{my_id}'
					})

				try:

					self.app_search.index_documents(engine_name, json_data)

					self.updateIds(f"building_no.final_id_{i}", array_of_ids, primary_key = 'my_id')

				except Exception as e:
					print("We are in recursion😍")
					time.sleep(60)
					self.ingestBuildingNumbers(start=i, end=end)


	def countTotalPoi(self):

		sqlExecuter, connection = self.makeConnection() 

		total = 0 
		#without address 565684

		for i in range(1, 736):
			
			query = f"""
					select count(*) from jan_2022_poi.poi_{i}
					
			"""
			sqlExecuter.execute(query)
			
			temp = sqlExecuter.fetchall()[0][0]
			total += temp 
			print("poi_", i, " -->> ", temp)
		print(total)



es_object = ElasticAppSearch() 

es_object.countTotalPoi()

# es_object.ingestPOI(1, 736) # 135 having 100 limit 

# es_object.ingestLocalities()  #Done 

# es_object.ingestBuildingNumbers(624, 736) # till 98 Done 

# es_object.addColumns()
