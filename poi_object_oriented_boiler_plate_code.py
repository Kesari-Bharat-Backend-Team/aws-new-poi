import psycopg2
from collections import defaultdict 
from fuzzywuzzy import fuzz
from math import radians, cos, sin, asin, sqrt
import mtranslate as mp
import pandas as pd,time


class PoiData:

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


	def getfwEditDistance(self, string1, string2):
		
		if string1 and string2:
			Ratio = fuzz.ratio(string1.strip().lower(),string2.strip().lower())
			return Ratio
		return 0

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

	def deleteDuplicatePoi(self):

		sqlExecuter, connection = self.makeConnection() 

		start, end = int(input("Enter start = ")), int(input("Enter end = "))
		
		for i in range(start, end):
			
			print("wait deleteing ", i)

			schema_name = f"jan_2022_poi.poi_{i}" 
			# schema_name = f"jan_2022_poi.poi_109_copy_2" 

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
			print("Done!!! ", i, schema_name)



	def convertMultiPolygontoPolygon(self):

		sqlExecuter, connection = self.makeConnection() 

		schema = 'polygon.sub_district_new_dutta'

		query = f"""
				select geom, (ST_DUMP(geom)).geom, gid from {schema}
				where geom_new is null
		"""		
		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall()

		for data in records:
			
			geom, geom_new, gid = data 

			query = f"""

						update {schema}
						set geom_new = '{geom_new}'
						where gid = {gid}

			"""
			print(query)
			sqlExecuter.execute(query)
			# connection.commit() 



	def addDistrictSubDistrictFromDutta(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(600, 736):

			table_name = f'poi_{i}'
			print(table_name)

			query = f""" 

				select 
				polygon.sub_district_new_dutta.dtname, polygon.sub_district_new_dutta.sdtname, jan_2022_poi.{table_name}.id from jan_2022_poi.{table_name}, polygon.sub_district_new_dutta

				where 
				
					dutta_flag_dt_sdt = 0 and 
					ST_intersects(
					polygon.sub_district_new_dutta.geom_new,
					ST_SetSRID(
						ST_MakePoint(longitude, latitude), 3857)
					
				) 
				/*limit 10; */
			"""
			print(query)
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
			
				dtname_dutta, sdtname_dutta, poi_id = data 
				print(dtname_dutta, sdtname_dutta)

				query = f"""
						update jan_2022_poi.{table_name}

						set district_name = '{dtname_dutta}',
						sub_district_name = '{sdtname_dutta}',
						dutta_flag_dt_sdt = 1

						where id = {poi_id}
				"""
				sqlExecuter.execute(query)

				print(query)
				# connection.commit() 

	def addDuttaFlag(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(1, 736):

			query = f"""
					
					alter table jan_2022_poi.poi_{i} 
					add column if not exists dutta_flag_dt_sdt integer 
					default 0;
			"""
			sqlExecuter.execute(query)
			# connection.commit() 
			print(query)


	def compareGovHabitantToTryAdminHabitant(self):

		sqlExecuter, connection = self.makeConnection() 

		query = f"""
				select id, hab_id, hab_name, st_x(geom), st_y(geom), 
				total_population, suggested_data  
				from try_admin_point.habitant
				where geom is not null and govt_ingest_flag = 0

		"""
		
		sqlExecuter.execute(query)

		habitant_records = sqlExecuter.fetchall() 

		if not habitant_records: 
			print("we are returning. No data found")
			return

		for hab_data in habitant_records:

			primary_key, hab_id, hab_name, hab_longitude, hab_latitude, hab_total_population, suggested_data = hab_data


			query = f"""

						update try_admin_point.habitant
						set govt_ingest_flag = 1 
						where id = {primary_key}
			"""
			sqlExecuter.execute(query)
			connection.commit() 

			query = f"""
					select id, name, st_y (ST_Transform (geom, 4326)), ST_X (ST_Transform (geom, 4326)) from try_admin_point.hamlet_point_copy
					where name ilike '{hab_name[:10]}'
						
			"""
			sqlExecuter.execute(query)

			gov_records = sqlExecuter.fetchall() 

			if not gov_records : continue

			is_present = False 
			minimum_distance = 10000000

			for gov_data in gov_records:

				gov_id, gov_name, gov_latitude, gov_longitude = gov_data 

				if not(hab_latitude and hab_longitude and gov_latitude and gov_longitude):
					print("govt_id = ", gov_id, 'hab_PK ', primary_key)
					# return 
					continue

				total_distance = self.distance(hab_latitude, gov_latitude, hab_longitude, gov_longitude)
				
				# print("habitant_name = ", hab_name, "->>", "gov_name = ", gov_name, total_distance)
				minimum_distance = min(minimum_distance, total_distance)
				total_ratio = self.getfwEditDistance(gov_name, hab_name)
				if total_distance < 2 and total_ratio > 80:

					
					is_present = True 
				
					print("total_distance = ", total_distance, "KM.")
					print(hab_name, primary_key)
					print("\n\n\n")

			if is_present == False: # we have to insert habitant into Government hamlet_point

				print("We have to insert this.", hab_name, " -->> ", gov_name, minimum_distance )
				print("\n\n")
				query_values = tuple([hab_name,hab_id,  'self_hamlet_village_habitant',   suggested_data , hab_total_population , hab_latitude, hab_longitude])
				
				query = f"""
					insert into try_admin_point.hamlet_point_copy
					(name, hab_id,  data_source, suggested_data, tot_popula, latitude, longitude) 
					values {query_values}
					;
				""".replace("None", "null")
				

				print(query)

				sqlExecuter.execute(query)
				connection.commit() 
				# return 


	def ingestApartementBuildingToBuildingNameAsPoi(self):

		sqlExecuter, connection = self.makeConnection( )

		for i in range(20, 736):

			query = f"""

				select name, address, latitude, longitude
				from jan_2022_poi.poi_{i}

				where category ilike '%apartment%' 
				and category not ilike '%agency%' 

			"""
			print("poi_i = " , i)

			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()

			for data in records:

				name, address, latitude, longitude = data 

				latitude = float(latitude)
				longitude = float(longitude)

				insert_values = tuple([name, address, latitude, longitude, 'Apartment Buildings'])
				query = f"""

						insert into building_no.building_name
						(buildingna, address, latitude, longitude, category)
						values {insert_values}
				""".replace("None", 'null')

				print(query)
				sqlExecuter.execute(query)
				# connection.commit()



	def updateGeominPOI(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(1, 2):

			query = f"""

				update jan_2022_poi.poi_{i}
				set geom = ST_MakePoint( longitude, latitude)
				where geom is null;

			"""
			print(query)
			sqlExecuter.execute(query)



	def removeAlomalies(self):


		sqlExecuter, connection = self.makeConnection()

		start = int(input("enter Start = ")) #109 
		end = int(input("enter End = "))  #110 

		total = 0 


		for i in range(start, end):

			schema_name = f"jan_2022_poi.poi_{i}"

			query = f"""
				
				select name, id from {schema_name}
				where name ilike '%\\u200%' or name ilike '$\\U200%'

			"""
			print(query)
			sqlExecuter.execute(query)

			records = sqlExecuter.fetchall()

			for data in records:

				name, id = data 
				print("name = ", name)
				name =  mp.translate(name, "en", "auto").replace("'", "").replace('"', "")
				
				print("new_name = ", name)
				
				query = f"""

					update {schema_name}
					set name = '{name}'
					where id = {id}

				"""
				sqlExecuter.execute(query)
				connection.commit() 
				print(query)
				time.sleep(1)


	def updatePhoneNumber(self):

		sqlExecuter, connection = self.makeConnection() 

		start = 109
		end = 110

		for i in range(start, end):
		

			schema_name = f"jan_2022_poi.poi_{i}"

			query = f"""

				select phone_1, id from {schema_name}
				where length(phone_1) >= 10 
				limit 100
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:

				phone_1, id = data 
				print(phone_1)


		



if __name__ == '__main__':

	poi_object = PoiData() 
	# poi_object.removeAlomalies()
	# poi_object.updatePhoneNumber()
	poi_object.deleteDuplicatePoi() 




if False:


	poi_object.dropColumnsInBuildingPolygons()
	poi_object.updateSRID() 
	poi_object.readWriteOS() 
	poi_object.compareGovHabitantToTryAdminHabitant() 
	poi_object.filterWidthsInAirport()
	poi_object.updateGeominPOI() 
	poi_object.create735Tables()
	poi_object.ingestApartementBuildingToBuildingNameAsPoi() 
	poi_object.ingestNeighbourHoodToSublocality3() 
	poi_object.replaceNull()
	poi_object.addDistrictSubDistrictFromDutta() 
	poi_object.addDuttaFlag() 
	poi_object.convertMultiPolygontoPolygon()
	poi_object.boilterplateCode()


