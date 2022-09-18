import psycopg2
from math import radians, cos, sin, asin, sqrt
from collections import defaultdict 
from geographiclib.geodesic import Geodesic
from turfpy import measurement
from geojson import Point, Feature
import numpy
import math
#Slack Alert Module 
import json
import sys
import random
import requests



def slackAlert(message = 'This is a test message'):

    
    url = "https://hooks.slack.com/services/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"
    # message = ("")
    title = (f"New Incoming Message :zap:")
    slack_data = {
        "username": "NotificationBot",
        "icon_emoji": ":satellite:",
        #"channel" : "#somerandomcahnnel",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
						'name': "himanshu"
						
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)


class LineData:



	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'ubuntu'
		self.password = '$Ks123'
		self.database = 'Kesari_bharat'
		self.port = 5432

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 


	def point_in_polygon(self, latitude, longitude):

		sqlExecuter, connection = self.makeConnection() 

		query = f"""
			select 
			public.district.kb_lev3_id
			from  public.district

			where ST_intersects(
				public.district.geom,
				
				ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 4326)
			)

		"""
		print(query)
		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall()
		
		if records:
			return int(records[0][0])
		print("we got he records  = ", -1)
		return -1 


	def getLatLongListGovt(self, table_name = 'rajasthan', gid = 1 ):

		sqlExecuter, connection = self.makeConnection()

		query = f"""
		   
			SELECT ST_AsText ( ST_Transform( (ST_Dumppoints (geom)).geom, 3857) )
			FROM public.{table_name}
			where gid = {gid}

		"""

		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall()
		
		temp_array = [] 
		try:

			for data in records:
				print(data)
				lon, lat =  data[0][6:-4].split(" ")
				temp_array.append([float(lon), float(lat)])

			return temp_array
		
		except Exception as e:

			print(str(e))
			# print(records)
			print("we got excepitpn ")

	

	def updateGovernmentLatLongArray(self):

		sqlExecuter, connection = self.makeConnection() 


		#chhattisgarh gujarat_westbengal_
		# all_state_name = [ 'punjab', 'andhrapradesh', 'arunachalpradesh', 'assam', 'bihar', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'madhyapradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha', 'rajasthan', 'sikkim',  'telangana', 'tripura', 'uttarakhand', 'uttarpradesh']

		all_state_name = [ 'andhrapradesh', 'arunachalpradesh', 'assam', 'bihar', 'chhattisgarh',   'haryana', 'himachalpradesh', 'jammuandkashmir', 'jharkhand', 'karnataka', 'ladakh', 'madhyapradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha',  'rajasthan', 'sikkim',  'telangana', 'tripura', 'uttarakhand', 'uttarpradesh', 'westbengal_', 'punjab', 'kerala', 'gujarat_'  ]

		for table_name in  ['westbengal_'] :#all_state_name:


			table_name = table_name 
			schema_name = f'public.{table_name}'
			print(schema_name)
			query = f"""

			alter table if exists {schema_name}
			add column if not exists lat_long_array double precision[];

			"""
			sqlExecuter.execute(query)
			# connection.commit() 
			print(query)

			query = f"""

				select gid 
				from {schema_name} 
				where lat_long_array is null and geom is not null 
			"""
			print(query)
			# where lat_long_array is null

			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
				

				gid = data[0]
				print("gid = ", gid)
				govt_lat_long_array = self.getLatLongListGovt( table_name , gid)
				print("output  = ", govt_lat_long_array)



				query = f"""

					update {schema_name}
					set lat_long_array = array {govt_lat_long_array}
					where gid = {gid}
					
				"""

				print(query)
				sqlExecuter.execute(query)
				connection.commit() 

		
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
		return round(c * r, 5)


	def getLatLonDifference(self, my_id, arr1, lat, long):

		answer_array = []
		# print(arr1)
		# arr1 = eval(a/rr1)
		# print(type(arr1))
		# return 

		for data in arr1:
		
			lon1, lat1 = data #73.5140221957832, 26.0940516031
			# print(data)
			answer = self.distance(lat1, float(lat), lon1, float(long))
			answer_array.append(answer)

		answer_array.sort() 
		# print("answer Difference = ", answer_array) 


		for data in answer_array[:10]:
			
			if data < 0.12:

				# print("We got a match!!!!", answer_array)
				# print("data = ", data)
				return True 

		return False 


	def compareGovtData(self):

		sqlExecuter, connection = self.makeConnection() 

		start = int(input("Enter starting table = "))
		end = int(input("Enter ending table = "))

		try:


			for i in range(start, end):
				
				query = f"""

						SELECT EXISTS (
						SELECT FROM 
							information_schema.tables 
						WHERE 
							table_schema  = 'line' AND 
							table_name   = '{i}'
						);
				"""

				sqlExecuter.execute(query)
				result = sqlExecuter.fetchall()[0][0] 


				new_table_name = None 

				if result:

					table_path = f"""line."{i}" """
					new_table_name = f'line_{i}'
				else:

					table_path = f""" edited_line . edited_{i} """
					new_table_name = f'edited_line_{i}'

				'''

					'public.jammuandkashmir'  ---> 1 21 (done)
					'public.madhyapradesh'   --->> 471  524 (Done)
					'public.punjab'        ----> 34 , 56 (done)
					'public.rajasthan'   --> 104 136  (done)
					'public.sikkim' ->  250 253 _> (Done) 
					'public.arunachalpradesh'   ->   254 , 279  (Done)
					'public.nagaland' -> 280 -> 291 -> (done)
					manipur = 292 - 305   (done)
					uttarakhand ->>> 57 70 (done)
					odisha 416 -> 445 (done)
					chhattisgarh 446  470 (done)


					himachalpradesh ->> 22 35 (running)
					jharkhand 392 --> 415 (running)
					'public.uttarpradesh' -> 137 -> 211 -> running 
					'public.bihar' -> 211  249 -> running 

					haryana -> 71 98		 (running) 
					mizoram 306 317			(running) 
					tripura 317 325			(running)
					meghalaya 325  336		(running)
					assam 336  368  		(running)

				'''


				public_schema_name = 'public.assam'
				district_code = i #109 


				if i in [393, 394, 395, 396, 397, 398, 399 ]: #tables are not present 
					continue

				query = f"""

					ALTER TABLE IF EXISTS near_by_roads.{new_table_name}

						ADD COLUMN if not exists near_by_gov_road_id integer[],
						
						add column if not exists check_road_name_flag integer default 0;

				"""
				sqlExecuter.execute(query)
				connection.commit() 

				query = f"""
						select max(my_id) from near_by_roads.{new_table_name}
				"""
				sqlExecuter.execute(query)
				total = sqlExecuter.fetchall()[0][0]


				#chekcing if query to be perform 

				query = f"""

				select count(*) from near_by_roads.{new_table_name}
					where (check_road_name_flag = 0 or check_road_name_flag is null)
					and near_by_gov_road_id is null
				"""

				print(f"near_by_roads.{new_table_name}")
				sqlExecuter.execute(query)
				records_without_check_road_flag = sqlExecuter.fetchall()[0][0]

				if not records_without_check_road_flag: continue 

				batch_size = 100

				for batch in range(1, total + batch_size, batch_size):

					print("Batch = ", batch)

					query = f"""
							
							select my_id, latitude, longitude  from near_by_roads.{new_table_name}
							where (check_road_name_flag = 0 or check_road_name_flag is null) and(latitude is not null) and my_id between {batch} and {batch + batch_size} and near_by_gov_road_id is null

					"""
				
					
					sqlExecuter.execute(query)
					records = sqlExecuter.fetchall() 

					for data in records:
					
						govt_ids = defaultdict(list)
						line_my_id, line_lat, line_long = data 
						
						query = f"""
								select gid, lat_long_array 
								from {public_schema_name}
								WHERE
								lat_long_array is not null 
								
								and 
								dt_code = {district_code}

						"""
								# removing it for now from the main gov quey 
						sqlExecuter.execute(query)
						line_records = sqlExecuter.fetchall()

						for data in line_records:

							gid, lat_long_array = data 

							output = self.getLatLonDifference(gid, lat_long_array, line_lat, line_long) 


							if output:
								print("We got the Id ")
								govt_ids[line_my_id].append(gid)
					
						#updating Gov_ids into Linedata 


						print("Final Govt_ids_nearby = ", line_my_id, govt_ids)
						if govt_ids[line_my_id]:


							query = f"""

							update near_by_roads.{new_table_name}
							set near_by_gov_road_id = Array {govt_ids[line_my_id]}
							where my_id = {line_my_id}
							"""

							sqlExecuter.execute(query)
							connection.commit() 
							print("gov Road Ids = ",govt_ids[line_my_id] )


						query = f"""

						update near_by_roads.{new_table_name}
						set 
						check_road_name_flag = 1
						where my_id = {line_my_id}
						"""

						sqlExecuter.execute(query)
						connection.commit() 

					print("Done!!!")

		except Exception as e:

			print(str(e))
			slackAlert(f"{start}, {end} \t" + str(e))
	
		else:
			slackAlert(f"Successfully Added Road Name in Line  data !!! {start}, {end}")

		




	def addDtCode(self):

		sqlExecuter, connection = self.makeConnection() 
		# gujarat_, punjab , 'kerala', 'westbengal_',
		all_state_name = [ 'andhrapradesh', 'arunachalpradesh', 'assam', 'bihar', 'chhattisgarh',   'haryana', 'himachalpradesh', 'jammuandkashmir', 'jharkhand', 'karnataka', 'ladakh', 'madhyapradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha',  'rajasthan', 'sikkim',  'telangana', 'tripura', 'uttarakhand', 'uttarpradesh',  'punjab', 'kerala', 'gujarat_'  ]


		# start = int(input("enter array start index "))
		# end = int(input("enter array end index "))

		# for table_name in all_state_name[start: end]:
		for table_name in all_state_name:

				
			schema_name = f'public.{table_name}'

			query = f"""
					alter table {schema_name}
					add column if not exists dt_code integer default 0,

					add column if not exists lat_long_array double precision[];
			"""
			sqlExecuter.execute(query)
			connection.commit() 

			query = f"""
						
			select  gid, lat_long_array
			
				from  {schema_name}
				where lat_long_array is not null 
				and dt_code = 0 

			"""
			print(query)
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:

				gid, data = data 
				# print(data)
				# data = eval(data)
				longitude, latitude = data[0]

				dt_code = self.point_in_polygon(latitude, longitude)
				
				query = f"""

					update {schema_name}
					set dt_code = {dt_code}
					where gid = {gid}

				"""
				sqlExecuter.execute(query)
				connection.commit() 
				print(dt_code)


	def addLatitudeLongitude(self):
		

		sqlExecuter, connection = self.makeConnection() 

		start = int(input("Enter starting table = "))
		end = int(input("Enter ending table = "))

		for i in range(start, end):

			print("i = ", i)
			
			query = f"""

					SELECT EXISTS (
					SELECT FROM 
						information_schema.tables 
					WHERE 
						table_schema  = 'line' AND 
						table_name   = '{i}'
					);
			"""

			sqlExecuter.execute(query)
			result = sqlExecuter.fetchall()[0][0] 

			new_table_name = None 

			if result:

				table_path = f"""line."{i}" """
				new_table_name = f'line_{i}'
			else:

				table_path = f""" edited_line . edited_{i} """
				new_table_name = f'edited_line_{i}'


			query = f"""

				alter table if exists {table_path}
				add column if not exists converted_lat_long_flag integer default 0;

			"""

			sqlExecuter.execute(query)
			connection.commit() 


			query = f"""
				
				create table if not exists near_by_roads.{new_table_name} (

					old_my_id integer not null,
					my_id serial,
					latitude double precision,
					longitude double precision,
					near_by_gov_road_id integer[]
				)

			"""
			sqlExecuter.execute(query)
			connection.commit() 



			query = f"""
					select  max(my_id) from {table_path}
			"""
			sqlExecuter.execute(query)
			total = sqlExecuter.fetchall()[0][0]
			print("total = ", total)

			query = f"""
					select count(*) from {table_path}
					where converted_lat_long_flag = 0 
			"""
			sqlExecuter.execute(query)
			total_lat_lat_to_be_convered = sqlExecuter.fetchall()[0][0]
			print("total_lat_lat_to_be_convered = ", total_lat_lat_to_be_convered)
			if not total_lat_lat_to_be_convered: continue 


			batch_size = 10000

			for batch in range(1, total + 1, batch_size):

			

				query = f"""

					select my_id from {table_path}
					where converted_lat_long_flag = 0 
					and
					my_id  between {batch} and {batch_size + batch}				
				"""


				print(query)
				# continue 
				
				sqlExecuter.execute(query)
				records = sqlExecuter.fetchall() 

				for data in records:

					my_id = data[0]

					query = f"""

					select   ST_AsText ( ST_Transform( (ST_Dumppoints (geom)).geom, 4326) )  from  {table_path}
					where my_id = {my_id}
												limit 1

					"""

					sqlExecuter.execute(query)
					lat_long_records = sqlExecuter.fetchall() 

					if not lat_long_records: continue #wow 

					try:

						longitude, latitude =  lat_long_records[0][0][6:-4].split(" ")
						if type(longitude) != 'float' or not type(latitude) != 'float': continue 
					except Exception as e:
						print(str(e))
						'''

						POINT(nan nan) => output 

						psycopg2.errors.InvalidTextRepresentation: invalid input syntax for type double                                  precision: ""
						LINE 6:       values (1648, '', 'nan')


						'''
						print("\n\n\n")
						continue 


					insert_value = tuple([my_id, latitude, longitude])
					
					query = f"""

						insert into near_by_roads.{new_table_name}
						
						(old_my_id, latitude, longitude)
						values {insert_value}

					"""
					print(query)
					sqlExecuter.execute(query)
					connection.commit() 
						


				#update converted_lat_long_flag
				query = f"""

					update {table_path}
					
					set converted_lat_long_flag = 1
					where my_id between {batch} and {batch + batch_size}

				"""
				print(query)
				sqlExecuter.execute(query)
				connection.commit() 


	def recheckQueries(self):


		sqlExecuter, connection = self.makeConnection() 


		all_state_name = [ 'punjab', 'andhrapradesh', 'arunachalpradesh', 'assam', 'bihar', 'chhattisgarh',   'gujarat_', 'haryana', 'himachalpradesh', 'jammuandkashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'madhyapradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland', 'odisha', 'rajasthan', 'sikkim',  'telangana', 'tripura', 'uttarakhand', 'uttarpradesh']


		hash = defaultdict(list)

		

		for table_name in all_state_name:


			query = f"""

				select count(*) from public.{table_name}
				where dt_code is null  or dt_code = 0
			"""

			sqlExecuter.execute(query)

			total = sqlExecuter.fetchall() [0][0]

			# print(table_name, "\t", total)
			if total: continue

			query = f"""

				select distinct dt_code from public.{table_name}

			"""

			sqlExecuter.execute(query)

			distinct_dt_codes = sqlExecuter.fetchall() 

			for dt_code in distinct_dt_codes:
				print(table_name, "\t", dt_code)

				# hash[table_name].append(dt_code[0])
				hash[dt_code[0]].append(table_name)
			
		# hash.sort() 
		# print(hash)
		print(sorted(hash.items()))




if __name__ == '__main__':

			
	line_object = LineData() 
	
	# line_object.recheckQueries()

	#also add list of lat long from geom ex- Gov raj, punjab 
	# line_object.updateGovernmentLatLongArray() #all Done 
	
	#step 1 Add Dt code for reducing search query
	# line_object.addDtCode() #all Done 
	
	#step Add latitude and longitude  in line data for fast query 
	# line_object.addLatitudeLongitude()  #all Done 
	
	
	#step 3 Fetch line data and search 12 meters within points
	line_object.compareGovtData() 
	

