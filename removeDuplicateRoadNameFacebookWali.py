import psycopg2
from math import radians, cos, sin, asin, sqrt


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


	def deleteDuplicateRoadName(self):


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

					delete  FROM {table_path}

					where 
				 duplicate_flag = 1;

				"""

				sqlExecuter.execute(query)
				connection.commit() 

				print("delted ", query)

				# records = sqlExecuter.fetchall()
				# print("duplicate = ", records)


				# slackAlert(f"Successfully Deleted Duplicate data in {start}, {end} " )
				

		except Exception as e:
			slackAlert(f" Exception in  {start}, {end} " )
			print("We got an error ", str(e))

		finally:
			slackAlert(f"Successfully Deleted duplicate road data all" )






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


if __name__ == '__main__':
		

	line_object = LineData() 
	# line_object.removeDuplicateRoads() 
	line_object.deleteDuplicateRoadName()





	




