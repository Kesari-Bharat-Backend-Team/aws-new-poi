import psycopg2
from math import radians, cos, sin, asin, sqrt


import json
import sys
import random
import requests


def slackAlert(message = 'This is a test message'):

    
    url = "https://hooks.slack.com/services/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"
    # message = ("")
    title = (f"Server Script's Alert :zap:")
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

		try:

			c = 2 * asin(sqrt(a))

		except Exception as e: 

			print("We got an Exception is distance function ", str(e))
			print("returning 100 for now!!")
			return 100
			
		# Radius of earth in kilometers. Use 3956 for miles
		r = 6371
		
		# calculate the result
		return round(c * r, 5)


	def getLatLongList(self, table_path , my_id ):

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


	def getLatLonDifference(self, arr1, arr2):

		answer_array = []

		print("arr1 = ", len(arr1))
		print("arr2 = ", len(arr2))
		for data in arr1:
		
			lon1, lat1 = data #73.5140221957832, 26.0940516031
			minimum = 100000
			for arr2_data in arr2:
				lon2, lat2 = arr2_data #73.5297678032012, 26.1286388987
				answer = self.distance(lat1, lat2, lon1, lon2)
				minimum = min(answer, minimum)
			answer_array.append(minimum)

		answer_array.sort() 
		print("answer array = ", answer_array)
		for data in answer_array[:3]:
			if data > 0.005:
				print("we are in false ")
				return False 
		print(answer_array)
		return True 


	def removeDuplicateRoads(self):

		sqlExecuter, connection = self.makeConnection() 

		
		start = int(input("Enter start = "))
		end = int(input("Enter end = "))
		
		try:


			for i in range(start, end):

				if i in [1, 392, 439]: continue

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

				if result:

					table_path = f"""line."{i}" """

				else:

					table_path = f""" edited_line.edited_{i} """


				query = f"""

					alter table if exists {table_path}
					add column if not exists checkduplicateflag_new integer default 0,
					add column if not exists my_id serial;

				"""

				sqlExecuter.execute(query)
				connection.commit() 

				#Adding my_id 
				
				query = f"""

					alter table if exists {table_path}
					add column if not exists my_id serial,
					add column if not exists duplicate_flag integer default 0;

				"""
				sqlExecuter.execute(query)
				connection.commit() 



				query = f"""

					select my_id, path, geom
					FROM {table_path}
					where path ilike 'facebook%' and checkduplicateflag_new = 0 and duplicate_flag = 0 and geom is not null
					
				"""
					# where my_id = 208590				

				print("query = ", query)
				sqlExecuter.execute(query)
				records = sqlExecuter.fetchall() 

				for data in records:
					
					my_id_facebook, path, geom_facebook = data 
					# if geom_facebook is None or len(geom_facebook) < 5 : continue  #ST_DWithin('None', geom, 1000) Edge Case

					#update Flag 
					query = f"""

							update  {table_path}
							set checkduplicateflag_new = 1
							where my_id = {my_id_facebook}
							
					"""
					sqlExecuter.execute(query)
					connection.commit() 
					print(query)

					query = f"""

						select my_id, path from {table_path}

					where path ilike 'os%' and 
						ST_DWithin('{geom_facebook}', geom, 200) 

					"""
					# where my_id = 154506

					print(query)

					sqlExecuter.execute(query)
					osm_records = sqlExecuter.fetchall() 

					arr1 = self.getLatLongList(table_path, my_id_facebook)
					


					for data in osm_records:
						my_id_osm, path = data

						print(my_id_osm, "file_name = ", i)

						arr2 =  self.getLatLongList(table_path, my_id_osm)
						is_duplicate = self.getLatLonDifference(arr1, arr2) 


						if is_duplicate:

							print("facebook = ", my_id_facebook,  " osm = ",my_id_osm) 

							query = f"""

								update {table_path}
								set duplicate_flag = 1
								where my_id = {my_id_facebook}
							"""
							print("We got duplicate ", query)
							
							sqlExecuter.execute(query)
							connection.commit() 
							break

						else:

							print("No Duplicates ", end = "")

			print("\n\nDone!!!! Ho gaya.")


		except Exception as e:

			print(start, end)
			
			print(str(e))
			slackAlert(f"remove_Duplicate_Road_name_Facebook {start}, {end} " + str(e))
		
		else:

			slackAlert(f"Successfully Found Duplicate data in {start}, {end} " )

			print(" Deleted Dupicate data.  -->>>  Done!!!", start, end)


	def countandDeleteDuplicateRoads(self):

		sqlExecuter, connection = self.makeConnection() 


		start = int(input("Enter starting table = "))
		end = int(input("Enter ending table = "))


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

			table_name = ''
			schema_name = ''

			if result:
				schema_name = 'line'
				table_name = f'"{i}"'

			else:

				schema_name = 'edited_line'
				table_name = f'edited_{i}'

			table_path = f""" {schema_name} .{table_name} """



			query = f"""

					SELECT column_name 
					FROM information_schema.columns 
					WHERE 
						table_schema  = '{schema_name}' 
						and table_name = '{table_name}' 
					and column_name='checkduplicateflag_new';

			"""

			sqlExecuter.execute(query)
			is_checkduplicateflag_new_present = sqlExecuter.fetchall() 


			if not is_checkduplicateflag_new_present: continue


			query = f"""

						
			update {table_path}
			set checkduplicateflag_new = 0
			where checkduplicateflag_new = 1

			"""
				# select count(*) from {table_path}
				# where duplicate_flag = 1
			print(query)
			sqlExecuter.execute(query)
			connection.commit() 
			print(table_path)
			
			# records = sqlExecuter.fetchall()[0][0]

			# print(table_path, " = ", records)


	def addRaodNamesfromNearByRoads(self):

		sqlExecuter, connection = self.makeConnection()

		start, end  = 110, 111
		

		for i in range(start, end):


			query = f"""

				SELECT EXISTS (

								SELECT FROM 

									information_schema.tables 
					
								WHERE 
					
									table_schema  = 'near_by_roads' AND 
									table_name   = 'edited_line_{i}'
					
								);

			"""

			sqlExecuter.execute(query)
			result = sqlExecuter.fetchall()[0][0]

			schema_name = "" #line or edited_line
			near_by_roads_path = ""

			if result:

				schema_name = f'edited_line.edited_{i}'
				near_by_roads_path = f"near_by_roads.edited_line_{i}"

			else:

				schema_name = f""" line."{i}" """
				near_by_roads_path = f"near_by_roads.line_{i}"
			

			query = f"""

				alter table {near_by_roads_path}
				add column if not exists added_road_name_flag integer default 0;

			"""
			
			sqlExecuter.execute(query)
			# connection.commit() 

			#add 3 columns in road data 

			query = f"""

				alter table {schema_name}
				add column if not exists gov_drrp_road_id character varying(100),
				add column if not exists gov_road_category character varying(100),
				add column if not exists gov_road_name character varying(100);

			"""

			sqlExecuter.execute(query)
			# connection.commit()


			query = f"""

				select old_my_id, my_id, near_by_gov_road_id from
				{near_by_roads_path}
				where near_by_gov_road_id is not null and
				added_road_name_flag = 0 
				limit 1

			"""
			sqlExecuter.execute(query)
			print(query)

			records = sqlExecuter.fetchall()

			for data in records:

				old_my_id, my_id, near_by_gov_road_array = data 

				arr1 = self.getLatLongList(schema_name, old_my_id)
				# state_name = self.pointInPolygon(latitude, longitude)
				state_name = 'rajasthan'


				for near_by_id in near_by_gov_road_array:

					#get data from gov 


					query = f"""

						select drrp_road_, roadcatego, roadname, lat_long_array   from public.{state_name}
						where gid = {near_by_id} 

					"""

					sqlExecuter.execute(query)

					gov_records = sqlExecuter.fetchall()[0]

					gov_drrp_road_id, gov_road_category, gov_road_name, arr2 = gov_records

					is_duplicate = self.getLatLonDifference(arr1, arr2)


					if is_duplicate:

						print("This road is nearby ")


						inserted_values =  tuple([gov_drrp_road_id, gov_road_category, gov_road_name])


						query = f"""

							UPDATE   {schema_name}
							SET 

								gov_drrp_road_id = '{gov_drrp_road_id}',
								gov_road_category = '{gov_road_category}',
								gov_road_name = '{gov_road_name}'
							WHERE
								my_id = {old_my_id}


						"""
					else:
						print("no nearbys")


						# sqlExecuter.execute(query)
					
				#update added_road_name_flag

				query = f"""


				update added_road_name_flag

				"""




if __name__ == '__main__':
		

	line_object = LineData() 
	# line_object.removeDuplicateRoads() 
	line_object.addRaodNamesfromNearByRoads() 
	




	




