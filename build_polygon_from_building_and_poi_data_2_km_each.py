import psycopg2
import re 
from collections import defaultdict 
from math import radians, cos, sin, asin, sqrt


class BuildPolygons:

	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'postgres'
		self.password = 'EiOiJja3ZqaTlrcnQyNzh'
		self.database = 'kesari_bharat'
		self.port = 5432
		self.state_hash = defaultdict(bool)
		self.district_hash = defaultdict(bool)
		self.sub_district_hash = defaultdict(bool)
		self.locality_hash = defaultdict(bool)
		self.sub_locality_hash = defaultdict(bool)

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		
		sqlExecuter = connection.cursor()

		return sqlExecuter, connection 

	def filter_data_by_x_km(self, address, match_string,lat1, long1, lat2, long2):

		my_address = address.split(",")
		# if not any([True for split_address in my_address if 
		# len(split_address) > 1 and split_address.strip().upper() == match_string.strip().upper()]):
		# 	print("Missmatch data", address, match_string)
		# 	return False

		if "," in match_string:
			print(match_string)
		true_flag = False 

		for split_address in my_address:

			if len(split_address) > 1 and split_address.strip().upper() == match_string.strip().upper():
				true_flag = True 

		
		if true_flag == False and  'block'.strip().upper() in match_string.strip().upper() and 'sector'.upper() not in match_string.strip().upper() and ',' in match_string: # now we will check the Block
				# print("we got the block in false condition", match_string)
				block_name, sub_loc_tag = match_string.strip().upper().split(",")
				block_flag = False 
				sub_loc_flag = False 

				for split_address in my_address:

					if block_name.strip().upper() == split_address.strip().upper():
						block_flag = True 
					if sub_loc_tag.strip().upper() == split_address.strip().upper():
						sub_loc_flag = True 
				
				if not block_flag and sub_loc_flag:
					# print("missmatch we are returning.")
					# print(match_string, address)
					return False 
			
		if true_flag == False and 'sector'.upper() in match_string.strip().upper() and ',' in match_string:

				sector_name, sub_loc_tag = match_string.strip().upper().split(",")
				sector_flag = False
				sub_loc_flag = False 

				for split_address in my_address:

					if sector_name.strip().upper() == split_address.strip().upper():
						sector_flag = True 
					if sub_loc_tag.strip().upper() == split_address.strip().upper():
						sub_loc_flag = True 
				
				if not sector_flag and sub_loc_flag:
					# print("miss match ")
					# print(match_string, " ->  ", address)
					return False 



		total_distance =  self.distance(lat1, lat2, long1, long2)
		# print()
		if "," in match_string:
			print( match_string , "distance = ", total_distance)

		# if 'block'.upper() in match_string:

		# 	return True if total_distance < .3 else False
			
		return True if total_distance <= 1 else False

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
		return round(c * r, 2)


	def collectPointdata(self, match_string, match_lat, match_long, schema_name):
		

		sqlExecuter, connection = self.makeConnection() 
		
		query = f"""

			Truncate Table sub_locality_points.mixed_sub_locality_points_building_google

		"""
		sqlExecuter.execute(query)
		connection.commit() 

		string_1 = "" 
		string_2 = ""

		if "," in match_string:
			string_1, string_2 = match_string.strip().upper().split(",")
			string_1 = string_1.strip().upper()
			string_2 = string_2.strip().upper()
		
		else:

			string_1 =  match_string.strip().upper()
		

		query = f"""

			select address, latitude, longitude from 
			{schema_name}

			where 
			address is not null and latitude is not null and 
			address ilike '%{string_1}%'
			
		"""

		if string_2:

			query += f" and address ilike '%{string_2}%' "

		
		# print(query)
		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall() 
		print(match_string, " frequency = ", len(records))
		for data in records:

			address, latitude, longitude = data 

			if  self.filter_data_by_x_km(address, match_string, match_lat, match_long, latitude, longitude) == False:
				continue 
			
			#inserting data into our table 

			query = f"""

			INSERT INTO 

				 sub_locality_points.mixed_sub_locality_points_building_google
				
				(geom, address)
				values(st_makepoint({longitude}, {latitude}),'{address}')

			"""

			sqlExecuter.execute(query)
			# print("query ", query)
			connection.commit()
		connection.close() 


	def makePolygonfromGeom(self, sub_locality_name, file_number):


		sqlExecuter, connection = self.makeConnection() 

		query = f"""
			select count(*) from sub_locality_points.mixed_sub_locality_points_building_google

		"""
		sqlExecuter.execute(query)
		total_data = sqlExecuter.fetchall() 

		flag = 1  #means less then three 
		if total_data[0][0] > 3:


			query = f"""

				
				insert into sub_locality_polygons.polygon_{file_number}
				(name, geom) 
				values(
					'{sub_locality_name}', 
					(SELECT st_concavehull(st_collect(geom), 1)
					FROM sub_locality_points.mixed_sub_locality_points_building_google 
					
					)
				)
			"""
			# print(query)	
			sqlExecuter.execute(query)
			connection.commit()

			flag = 0 #means > 3 

		query = f"""

			Truncate Table sub_locality_points.mixed_sub_locality_points_building_google

		"""
		sqlExecuter.execute(query)
		connection.commit() 
		# print(query)
		connection.close() 
		return flag


	def databaseQueries(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(1, 736):

			print("we are running ", i)

			query = f"""

			select name, latitude, longitude, my_id from 
			sub_locality_points.sub_locality_points
			
			where flag = 0 and data_source = '{f'final_id_{i}'}'

			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
 
				name, latitude, longitude, id = data 

				# if not name.strip().upper() == 'sector 9,Malviya nagar'.strip().upper():
				# 	continue
				
				# print(latitude, longitude)
				# return 
				self.collectPointdata(name, latitude, longitude, f'building_no.final_id_{i}')
				# self.collectPointdata(name, latitude, longitude, f'jan_2022_poi.poi_{i}') 
				flag_count_less_than_10 =  self.makePolygonfromGeom(name, file_number = i)

				query = f"""

						update sub_locality_points.sub_locality_points
						set flag_count_less_than_10 = {flag_count_less_than_10},
						flag = 1
						where my_id = {id}
				""" 

				sqlExecuter.execute(query)
				connection.commit() 
		
			print("Done!!!", i)
		
		#Last Line Don't remove it.
		connection.close()


if __name__ == "__main__":

	polygon_object = BuildPolygons() 
	polygon_object.databaseQueries() 