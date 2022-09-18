import psycopg2
from math import radians, cos, sin, asin, sqrt, ceil
from collections import defaultdict 
from geographiclib.geodesic import Geodesic
import math 


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
		return round( (c * r), 5)


	def __init__(self):

		self.hostname = '65.1.151.184' #'localhost'  
		self.username = 'ubuntu'
		self.password = '$Ks123'
		self.database = 'Kesari_bharat'
		self.port = 5432


	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 


	def makeConnectionLightSail(self):

		hostname =   '43.205.48.29'  # 'localhost'  
		username =  'root' # 'postgres'
		password =  '$Chintu02468'
		database = 'postgres' 
		port = 5432

		connection = psycopg2.connect(host= hostname, user= username, password= password, dbname= database, port= port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 

		
	def getLatLongList(self, table_path , my_id):

		sqlExecuter, connection = self.makeConnection()

		query = f"""
		   
			SELECT ST_AsText ( ST_Transform( (ST_Dumppoints (geom)).geom, 4326) )
			FROM {table_path}
			where my_id = {my_id}

		"""

		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall()
		
		temp_array = [] 
		for data in records:

			lon, lat =  data[0][6:-4].split(" ")
			temp_array.append([float(lon), float(lat)])

		return temp_array

	
	def findQuadrant(self, angle = 10):

		angle = math.ceil(angle)


		if  -45 <= angle <= 45:
			return "A"
		
		elif  46 <= angle <= 135:
			return "B" 
		
		elif (136 <= angle <= 180) or (-180 <= angle <= -135):
			return "C"
		
		elif  ( -45 >= angle >= -135):
			return "D"

		else:

			print("Please check youe quadrant conditions dude. 😃😁")

	def cutBearing(self, start, end):


		sqlExecuter, connection = self.makeConnection() 
		sqlExecuter_lightSail, connection_light_sail = self.makeConnectionLightSail() 
		

		print("connection Done Successfully")
		return 
		
		# start = int(input("Enter starting table = "))
		# end = int(input("Enter ending table = "))
		

		# start = 110
		# end = 111

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


			#Making New Column flag 

			query = f"""

				alter table {table_path}
				add column if not exists cut_bearing_flag integer default 0

			"""
				# add column if not exists my_id serial;
			print(query)
			sqlExecuter.execute(query)
			connection.commit() 

			query = f"""

				
				ALTER TABLE if exists edited_line_split.{new_table_name}		
				ALTER COLUMN geom TYPE character varying(5000000);	

			"""
			# print(query)
			# sqlExecuter.execute(query)
			# connection.commit() 


			#making the New Table

			query = f"""
			
				create table if not exists edited_line_split.{new_table_name} (

					gid integer,
					osm_id integer,
					code character varying(5000),
					fclass character varying(5000),
					name character varying(5000),
					ref character varying(5000),
					oneway character varying(5000),
					maxspeed integer,
					layer character varying(5000),
					bridge character varying(5000),
					tunnel character varying(5000),
					path character varying(5000),
					geom character varying(5000000),
					my_id  serial, 	
					old_my_id integer	

				)

			""" 
			sqlExecuter_lightSail.execute(query)
			connection_light_sail.commit() 

			#runing query with 100 Batch size 
			
			query = f"""

				select max(my_id) from {table_path}

			""" 
			sqlExecuter.execute(query)
			total = sqlExecuter.fetchall()[0][0] 


			query = f"""

					select count(*) from {table_path}
					where cut_bearing_flag = 0
			"""
			sqlExecuter.execute(query)
			print(query)

			cut_bearing_flag_count = sqlExecuter.fetchall()[0][0] 

			if cut_bearing_flag_count <= 0: continue 

			batch_size = 1000
			for batch in range(1, total + 100, batch_size):
					


				query = f""" select gid, osm_id, code, fclass, name, ref, oneway, maxspeed, layer, bridge, tunnel, path, geom, my_id from {table_path}
				where geom is not null

				and (cut_bearing_flag = 0 or cut_bearing_flag is null)
				
				and my_id between {batch} and {batch + batch_size}

				"""

				sqlExecuter.execute(query)
				records = sqlExecuter.fetchall() 
				print("table _name = ", table_path, "batch = ", batch,  len(records))


				for data in records:

					gid, osm_id, code, fclass, name, ref, oneway, maxspeed, layer, bridge, tunnel, path, geom, my_id  = data 

					name = name.replace("'", '"') if name else ""
					ref = ref.replace("'", '"') if ref else ""

					lat_long_array = self.getLatLongList(table_path, my_id)
				
					if lat_long_array == [] or lat_long_array[0] == lat_long_array[-1]: #circle  
						print("No data", lat_long_array)
						continue 

					print("total points = ", len(lat_long_array)  - 1)

					prev_long, prev_lat = lat_long_array[0]

					quadrant = None 


					start_position = 0 
					covered_so_far = 0 


					for index, lat_long_pair in enumerate(lat_long_array[1:], 1):

						current_long, current_lat = lat_long_pair

						brng = Geodesic.WGS84.Inverse(prev_lat, prev_long, current_lat, current_long)['azi1']


						print("Current quard = ", brng, "\t\t\t" , self.findQuadrant(brng), prev_lat, prev_long )


						if quadrant == None:

							quadrant = self.findQuadrant(brng)


						elif quadrant == self.findQuadrant(brng):
								#Same quadrant
								pass 
						else:

							print("We will cut here at Index = ", index) 
							print("prev_querant = ", quadrant, end = " ")
							quadrant = self.findQuadrant(brng)
							print("Now = ", quadrant)

							geom_of_lat_long_array = self.makeGeom(lat_long_array[start_position: index], my_id)

							print("geom of lat long ", geom_of_lat_long_array)
							start_position = index - 1
							covered_so_far = index - 1

							query = f"""

								select ST_MakeLine(

									array 
										{geom_of_lat_long_array}
									

								)

							"""
							sqlExecuter.execute(query)
							final_geom = sqlExecuter.fetchall()[0][0]


							print("final Geom = ", final_geom)


							insert_value = tuple([gid, osm_id, code, fclass, name, ref, oneway, maxspeed, layer, bridge, tunnel, path, my_id, final_geom])
							query = f"""

								insert into edited_line_split.{new_table_name}
									
									( gid, osm_id, code, fclass, name, ref, oneway, maxspeed, layer, bridge, tunnel, path, old_my_id, geom)
									
								values
									{insert_value}

							""".replace("None", "null")

							# print("insdide query", query)
							sqlExecuter_lightSail.execute(query)
							connection_light_sail.commit() 

						prev_long, prev_lat = current_long, current_lat 
					
					if start_position != 0 and covered_so_far != 0 and covered_so_far < len(lat_long_array) :

						geom_of_lat_long_array = self.makeGeom(lat_long_array[covered_so_far:], my_id)
						print(geom_of_lat_long_array)

						query = f"""

								select 
								
									ST_MakeLine(

									array 
										{geom_of_lat_long_array}
									)

							"""

						sqlExecuter.execute(query)
						final_geom = sqlExecuter.fetchall()[0][0]

						insert_value = tuple([gid, osm_id, code, fclass, name, ref, oneway, maxspeed, layer, bridge, tunnel, path, my_id, final_geom])


						print("final_geom = ", final_geom)
						query = f"""

							insert into edited_line_split.{new_table_name}
								
								( gid, osm_id, code, fclass, name, ref, oneway, maxspeed, layer, bridge, tunnel, path, old_my_id, geom)
								
							values
								{insert_value}

						""".replace("None", "null")

						print(query)
						sqlExecuter_lightSail.execute(query)
						connection_light_sail.commit() 



				#update Line data Flag 100 batch 

				query = f"""


				update {table_path}

					SET cut_bearing_flag = 1
						where my_id between {batch} and {batch + batch_size}

				"""
				sqlExecuter.execute(query)
				connection.commit() 



	def makeGeom(self, geom_array, my_id):

		sqlExecuter, connection = self.makeConnection() 
	
		geom_of_lat_long = []
		# print("geom arry = ", geom_array)

		for lat_long_pair in geom_array:

			longitude, latitude = lat_long_pair

			query = f"""


		select ST_SetSRID(ST_MakePoint({longitude}, {latitude}), 3857);


			"""

			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()[0][0]

			geom_of_lat_long.append(records)
	
		return geom_of_lat_long

	def checkSearver(self):

		try:
				
			sqlExecuter_lightSail, connection_light_sail = self.makeConnectionLightSail()
			print("Connection done Successfully")
		
		except Exception as e:
			print("We got connection error")
			print(str(e))




if __name__ == '__main__':

	# try:

	# 	start = 110 #int(input("Enter starting table = "))
	# 	end = 111 #int(input("Enter ending table = "))
			
	# 	line_object = LineData() 
	# 	line_object.cutBearing(start, end)


	# except Exception as e:

	# 	print(str(e))
	# 	# slackAlert(f"CutBearing_aws_light_sail {start}, {end} " + str(e))
	
	# else:
	# 	slackAlert(f" Bearing Done Successfully !!! {start}, {end}" )

	line_object = LineData() 
	line_object.checkSearver()



	
	