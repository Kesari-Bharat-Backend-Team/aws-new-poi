import psycopg2
from collections import defaultdict 
from fuzzywuzzy import fuzz
import time 
from math import radians, cos, sin, asin, sqrt


class PoiData:

	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'postgres'
		self.password = 'EiOiJja3ZqaTlrcnQyNzh'
		self.database = 'kesari_bharat'
		self.port = 5432

		# self.hostname =  '65.1.151.184'  #
		# self.username = 'ubuntu'
		# self.password = '$Ks123' 
		# self.database = 'Kesari_bharat'

	def getfwEditDistance(self, string1, string2):
		if string1 and string2:
			r_for_ratio = fuzz.ratio(string1.strip().lower(),string2.strip().lower())
			return r_for_ratio
		return 0


	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 
	

	def makeSSD2Connection(self):
		try:
			print("We are making connection SSD_2.....")
			connection = psycopg2.connect(host='89.233.105.147', user= 'postgres', password= '616161', dbname= 'postgres', port=self.port)
			sqlExecuter = connection.cursor()
			return sqlExecuter, connection 
		except Exception as e:
			print(str(e))


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


	def updateNoneFalse(self):

		sqlExecuter, connection = self.makeConnection() 

		# total = 0
		for i in range(1, 736):

			query = f"""

					update   jan_2022_poi.poi_{i}
					set phone_1 = NULL
					where phone_1 ilike '%NULL%' or phone_1 ilike '%None%'
			"""
			sqlExecuter.execute(query)

			query = f"""

					update   jan_2022_poi.poi_{i}
					set email = NULL
					where length(email) <= 6
			"""
			sqlExecuter.execute(query)

			query = f"""

					update   jan_2022_poi.poi_{i}
					set landmark_1 = NULL
					where  upper(landmark_1) = 'NULL' or landmark_1 = 'None'
			"""
			sqlExecuter.execute(query)

			query = f"""

					update   jan_2022_poi.poi_{i}
					set road = NULL
					where   upper(road) = 'NULL' or road = 'None'
			"""
			sqlExecuter.execute(query)

			query = f"""

					update   jan_2022_poi.poi_{i}
					set category = NULL
					where  upper(category) = 'NULL' or category = 'None'
			"""
			sqlExecuter.execute(query)



			query = f"""
					update jan_2022_poi.poi_{i}
					set website = NULL
					where website = 'false' or length(website) <= 5
			"""


			print("Done !!!", i)
			sqlExecuter.execute(query)
			
			# connection.commit() 

	def removeDuplicates(self): #very imported code will be helpful in the near future.

		sqlExecuter, connection = self.makeConnection() 

		file = open("remove_duplicates_list_new_1.txt", 'a')
		for i in range(600, 632):

			print(f"poi_{i}")
			file.writelines(str(i) + ", ")

			query = f"""
					SELECT name, latitude, longitude, id, data_source, address, phone_1, website FROM jan_2022_poi.poi_{i}

					where data_source is null 
					
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:

				name, latitude, longitude, id, data_source, address, phone_1, website = data 
				query = f"""
					select name, latitude, longitude, id, address, phone_1, website from 
					jan_2022_poi.poi_{i}
					where name ilike '%{name[1:10]}%'
				"""

				sqlExecuter.execute(query)
				matching_records = sqlExecuter.fetchall() 

				for match_data in matching_records:
					
					match_name, match_latitude, match_longitude, match_id, match_address, match_phone_1, match_website = match_data 
					if match_id == id: continue 


					website = website if website and len(website) > 5 else match_website


					if phone_1 and len(phone_1) > 8 and match_phone_1 and len(match_phone_1) > 8 and  "0000" not in phone_1 and  "0000" not in match_phone_1:

						my_phone_1 = ", ".join([phone_1, match_phone_1])
					
					else:
						my_phone_1 = phone_1 if  phone_1 and "0000" not in phone_1 else match_phone_1


					# my_address = address if len()
					# print("Phone number = ", )

					ratio = self.getfwEditDistance(match_name, name )

					total_distance = self.distance(latitude, match_latitude, longitude, match_longitude)
					# print(name, '\t', match_name, "ratio = ", ratio, "distance = ", total_distance)

					# print(total_distance)

					if ( sum([1 for ch in name if ch.isdigit()]) >= 1 or 'block' in name.lower() or 'sector' in name.lower() ) or (sum([1 for ch in match_name if ch.isdigit()]) >= 1 or 'block' in match_name.lower() or 'sector' in match_name.lower() ):
						# print('name contain digit ', name, "\t",  match_name, "\t", ratio)

						if ratio >= 99 and total_distance <= .1:

							query = f"""
									delete from jan_2022_poi.poi_{i}
									where id = {match_id}
							"""
							print(query)

							sqlExecuter.execute(query)
							connection.commit()

							
							query = f"""
										update jan_2022_poi.poi_{i}

										set phone_1 = '{my_phone_1}',
										website = '{website}'

										where id = {id}
							""".replace("None", "NULL")
							print(query)
							sqlExecuter.execute(query)
							connection.commit() 



							print( "ratio = ", ratio, "distance = ", total_distance)
							print(match_name, "\t" * 2 , name)
							print(match_address, "\t" * 2 , address )
							print(match_phone_1, "\t" * 2 , phone_1 )
							print(match_website, "\t" * 2 , website )

							print("id = ", id, "match_id = ", match_id)
							print("\n", "*" * 120, "\n")
							# print(latitude, longitude, "\t", match_latitude, match_longitude)

						elif total_distance <= 0.2:
							print("\n" * 5)
							print("We are in continue")
							print( "ratio = ", ratio, "distance = ", total_distance)
							print(match_name, "\t" * 2 , name)
							print(match_address, "\t" * 2 , address )
							print(match_phone_1, "\t" * 2 , phone_1 )
							print(match_website, "\t" * 2 , website )
							print("id = ", id, "match_id = ", match_id)
							print("\n", "*" * 120, "\n")
							# return 


					elif ratio >= 85  and total_distance <= 0.2:# self.checkNearyBywithLatLon(latitude, longitude, match_latitude, match_longitude): #and total_distance <= 0.1: 

							print( "ratio = ", ratio, "distance = ", total_distance)
							print(match_name, "\t" * 2 , name)
							print(match_address, "\t" * 2 , address )
							print(match_phone_1, "\t" * 2 , phone_1 )
							print(match_website, "\t" * 2 , website )

							print("id = ", id, "match_id = ", match_id)

							query = f"""
									delete from jan_2022_poi.poi_{i}
									where id = {match_id}
							"""
							print(query)
							sqlExecuter.execute(query)
							connection.commit()


							query = f"""
										update jan_2022_poi.poi_{i}

										set phone_1 = '{my_phone_1}',
										website = '{website}'

										where id = {id}
							""".replace("None", "NULL")

							print(query)
							sqlExecuter.execute(query)
							connection.commit() 



							print("\n", "*" * 120, "\n")
				
			print("Done !!!", i)


	def pointInPolygon(self):

		sqlExecuter, connection = self.makeConnection()
		
		query = f"""
				SELECT building_polygon_1.northen.gid, 
				polygon.pincode_or_locality.pincode, 
				polygon.pincode_or_locality.officename,
				polygon.pincode_or_locality.stname, 
				polygon.pincode_or_locality.dtname 
				FROM building_polygon_1.northen, polygon.pincode_or_locality
				where st_intersects(
				ST_SetSRID(ST_MakePoint(st_x(centroid), st_y(centroid)), 4326), 
					polygon.pincode_or_locality.geom)
				and building_polygon_1.northen.name is not null
		"""
		print(" Wait query is running ", query)
		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall() 

		for data in records:

			gid, pincode, locality, stname, dtname = data 
			
			# data = tuple([locality, dtname, stname, pincode])
			
			query = f"""
				update building_polygon_1.northen
				set address = '{locality.strip()}, {dtname.strip()}, {stname.strip()}, {pincode.strip()}'
				where gid = {gid}
			"""
			print(query)
			sqlExecuter.execute(query)
			# connection.commit() 
		print("Done !!!")



	def addColumn(self):

		print("We are adding Column")
		sqlExecuter, connection = self.makeConnection() 

		for i in range(10, 20):
			print(i)
			query = f"""
					alter table jan_2022_poi.poi_{i}
					add column address_without_name integer default 0;
			"""
			sqlExecuter.execute(query)
			print(query)


	def findNearbyPoi(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(110, 111):

			search_string = 'Government senior secondary school' #'Wow! Momo'

			query = f"""
				SELECT  name, id, latitude, longitude, address, data_source, phone_1, website FROM jan_2022_poi.poi_{i}
				where name ilike '%{search_string}%' 
				LIMIT 100
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:

				name, id, latitude, longitude, address, data_source, phone_1, website = data 

				latitude = float(latitude)
				longitude = float(longitude)

				# print("name = ", name)
				query = f"""
					select name, id, latitude, longitude, address, data_source, phone_1, website from 
					jan_2022_poi.poi_{i}
						where name ilike '%{name[1:10]}%' 
						LIMIT 100
				"""
				
				sqlExecuter.execute(query)
				matching_records = sqlExecuter.fetchall() 
				# print(matching_records)
				for match_data in matching_records:

					match_name, match_id, match_latitude, match_longitude, match_address, match_data_source, match_phone_1, match_website = match_data 

					if match_id == id:
						# print("we are in continue")
						continue
					match_longitude = float(match_longitude)
					match_latitude = float(match_latitude)

					total_distance = self.distance(match_latitude, latitude, match_longitude, longitude)

					# if total_distance > 1: continue

					ratio = self.getfwEditDistance(match_name, name )
					print(total_distance)
					print(ratio)	
					print(match_name, name)
					print(match_latitude, latitude)
					print(match_longitude, longitude)

					print("*" * 150)


	def updateNameviaNameSpaceFlag(self):


		sqlExecuter_ssd2, connection_ssd2 = self.makeSSD2Connection() 
		sqlExecuter_new, connection_new = self.makeConnection() 

		# jan_2022_poi
		for i in range(109, 110):

			query = f""" 
			
				select name, id, address, latitude, longitude from 
					jan_2022_poi.poi_{i}
				where name_space_flag = 1 and latitude is not null and longitude is not null
				limit 100;

			"""
			print(query)

			sqlExecuter_new.execute(query)
			records = sqlExecuter_new.fetchall() 
			# sqlExecuter_ssd2.execute(query)
			# records = sqlExecuter_ssd2.fetchall() 

			for data in records:

				name, id, laddress, latitude, longitude = data 

				query = f"""
						select name, id, address, data_source, latitude, longitude 
						from jan_2022_poi_may.poi_{i}
						where latitude = {latitude} and longitude = {longitude} and 
						data_source ilike '%ssd%'
				"""

				sqlExecuter_ssd2.execute(query)
				match_records = sqlExecuter_ssd2.fetchall()

				# print('match_name = ', end = "   ")

				for match_data in match_records:

					match_name, match_id, match_address, match_data_source, match_latitude, match_longitude = match_data 

					total_distance  = self.distance(match_latitude, latitude, match_longitude, longitude)
					ratio = self.getfwEditDistance(match_name, name)

					if ratio >= 85:

						print("old name = ", name, id)
						print(match_name, match_id)
						print("ratio  = ", ratio, "\n", "total_distance  = ", total_distance)

						# query = 
						print("*" * 150)


	def removeNearbyHabitant(self):

		sqlExecuter, connection = self.makeConnection() 

		print("Wait! Query is running...")
		query = f"""
				select hab_name, id, latitude, longitude from try_admin_point.habitant
		"""
		# 1673766  
		
		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall() 

		for data in records:

			hab_name, id, latitude, longitude = data 
			print("finding ", hab_name)
			query = f"""
					select hab_name, id, latitude, longitude from try_admin_point.habitant
					where hab_name ilike '%{hab_name}%'
			"""
			sqlExecuter.execute(query)

			matching_records = sqlExecuter.fetchall() 

			for match_data in matching_records:
				
				matching_hab_name, matching_id, matching_latitude, matching_longitude = match_data 

				if matching_id == id:
					continue 

				total_distance = self.distance(matching_latitude, latitude, matching_longitude, longitude)
				ratio = self.getfwEditDistance(matching_hab_name, hab_name)

				if total_distance < 1 and ratio >= 85:
					print("old \t", hab_name,  id,'\t' * 3 , matching_hab_name, matching_id)
					print(total_distance)
					print(ratio)


					query = f"""
							delete from try_admin_point.habitant
							where id = {matching_id}
					"""
					print(query)
					sqlExecuter.execute(query)
					connection.commit() 
					print("*" * 150)


	def updatePrimaryKey(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(2, 736):

			query = f"""

					ALTER TABLE  jan_2022_poi.poi_{i}
					DROP COLUMN IF EXISTS id;

			"""
			sqlExecuter.execute(query)

			query = f"""

				alter table jan_2022_poi.poi_{i}
				add column id serial primary key;

			"""
			sqlExecuter.execute(query)
			connection.commit() 

			print("Done !!! ", i) 


if __name__ == '__main__':

	poi_object = PoiData() 
	print("Hello Himanshu")
	# poi_object.removeNearbyHabitant()

	poi_object.updatePrimaryKey() 

	# poi_object.updateNameviaNameSpaceFlag() 
	# poi_object.updateNoneFalse() 
		# poi_object.removeDuplicates() 
		# poi_object.addColumn() 
		# poi_object.pointInPolygon() 
		# poi_object.findNearbyPoi() 
		