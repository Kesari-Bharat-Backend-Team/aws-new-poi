import psycopg2
from collections import defaultdict 



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




class Building:

	def __init__(self):

		self.hostname =  'localhost'  
		self.username = 'postgres'
		self.password = '$Chintu02468'
		self.database = 'postgres'
		self.port = 5432

	

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection  

	
	def pointInPolygon(self):

		
		sqlExecuter, connection = self.makeConnection() 
		# aws_sqlExecuter, aws_connection = self.makeAWSConnection() 

		start = int(input("Enter the starting number = "))
		end = int(input("Enter the ending number = "))

		try:

			for i in range(start, end):

				schema_name = f"building_download3.merged_building{i}"


				query = f"""

					alter table if exists {schema_name}
					add column if not exists dt_code integer default 0;

				"""
				sqlExecuter.execute(query)
				connection.commit() 


				query = f"""

					select max(my_id) from {schema_name}

				"""
				sqlExecuter.execute(query)
				total_records = sqlExecuter.fetchall()[0][0] 


				query = f"""
							select count(*)
							from {schema_name}
							where  dt_code   = 0  and latitude is not null 

				"""
				sqlExecuter.execute(query)
				records_without_dt_code  = sqlExecuter.fetchall()[0][0] 
				if records_without_dt_code == 0:
					print("records_without_dt_code = 0 ")
					continue 
				
				print("total_count_without_dt_code = ", records_without_dt_code)
				


				batch_size = 1000
				for batch in range(1, total_records, batch_size):


					print("fetching records")
					query = f"""

							select my_id, latitude, longitude 
							from {schema_name}
							where  dt_code  = 0  and latitude is not null 
							and my_id between {batch} and {batch + batch_size}

					"""

					print(query)
					sqlExecuter.execute(query)
					records = sqlExecuter.fetchall() 

					for data in records:

						my_id, latitude, longitude = data 
						latitude = float(latitude)
						longitude = float(longitude)


						print("my_id = ", my_id )

						query = f"""

								select 

								
								public.district_with_id.kb_lev3_id  from public.district_with_id

								where 
								
									ST_intersects(
									public.district_with_id.geom,
									
										ST_MakePoint({longitude}, {latitude})
								) 

						"""
						print(query)
						sqlExecuter.execute(query)
						kb_lev3_id = sqlExecuter.fetchall()
						
						if kb_lev3_id:
							kb_lev3_id = kb_lev3_id[0][0]
						else:
							kb_lev3_id = -1
							print("We got null values", kb_lev3_id)

						
						query = f"""

							update {schema_name}
							set dt_code = {kb_lev3_id}
							where my_id = {my_id}

						"""
						sqlExecuter.execute(query)
						print(query)

						connection.commit() 


		except Exception as e:

			print(str(e))
			slackAlert(f"We got an Error !!! {start}, {end}  i = {i}" + str(e))
	
		else:
			slackAlert(f"Successfully Done Point In Polygon!!! {start}, {end}")
	
	 
	def getLatLongFromRoutePoint(self):

		sqlExecuter, connection = self.makeConnection() 

		start = int(input("Enter the starting number = "))
		end = int(input("Enter the ending number = "))

		try:

			for i in range(start, end):

				schema_name = f"building_download3.merged_building{i}"

				query = f"""
						select max(my_id) from {schema_name}
				"""
				sqlExecuter.execute(query)

				total = sqlExecuter.fetchall()[0][0]


				query = f"""

						select count(*) from {schema_name}
						where latitude is null
				"""
				print(query)

				sqlExecuter.execute(query)
				data_without_lat_long = sqlExecuter.fetchall() 
				print(data_without_lat_long)
				if not data_without_lat_long[0][0]:
					print("we are in continue")
					continue
				
				batch_size = 10000

				for batch in range(1, total, batch_size):

					print("curent batch = ", batch)

					query = f"""

						select routepoint, my_id from {schema_name}
						where latitude is null
						and my_id between {batch} and {batch + batch_size}
					"""
					sqlExecuter.execute(query)
					records = sqlExecuter.fetchall()


					# print("total data  without lat ", records)
					for data in records:

						routepoint, my_id = data 
						print(routepoint)
						# continue

						try:
							routepoint = eval(routepoint)

							print("type of ", type(routepoint))
							# print(routepoint)
							arr = routepoint.get('point').get('coordinates')[:2]
							print("id = ", my_id, " --->>> \t ", arr)
							longitude, latitude = arr 
							
							query = f"""

								update {schema_name}
								set latitude = {latitude}, 
								longitude = {longitude}
								where my_id = {my_id}

							"""

							print(query)
							sqlExecuter.execute(query)
							connection.commit() 


						except Exception as e:

							print("error = ", str(e))
							# return 
							continue


				print("Done!!!", i)


		except Exception as e:

			print(str(e))
			slackAlert(f"We got an Error !!! {start}, {end}  i = {i}" + str(e))
	
		else:
			slackAlert(f"Successfully Fetch Lat long from RoutePoint -> Building data!!! {start}, {end}")
	

	def InsertBuildingdataIntoAWSBuildingData(self):

		sqlExecuter, connection = self.makeConnection()

		aws_sqlExecuter, aws_connection = self.makeAWSConnection() 
		
		try:
			
			start = int(input("Enter starting table number 1-24 = "))
			end = int(input("Enter ending table number 1-24 = "))

			for i in range(start, end):

				schema_name = f"building_download3.merged_building{i}"


				query = f"""

					alter table if exists {schema_name}
					add column if not exists record_checked_flag integer default 0;

				"""

				sqlExecuter.execute(query)
				connection.commit() 


				query = f"""
						select max(my_id) from {schema_name}
				"""
				sqlExecuter.execute(query)

				total = sqlExecuter.fetchall()[0][0]


				batch_size = 100

				for batch in range(1, total, batch_size):


					query = f"""

						SELECT 
							 routepoint, housenumbers, addressroadid, buildingnames, latitude, longitude, dt_code, my_id
						FROM 
							{schema_name}

						WHERE 
								record_checked_flag = 0
						AND 
								dt_code > 0 
						AND 
								my_id 
						BETWEEN 
								{batch}
						AND 
								{batch + batch_size}

					"""
					print(query)

					sqlExecuter.execute(query)

					records = sqlExecuter.fetchall() 


					for data in records:

						routepoint, house_numbers, addressroadid, buildingnames, latitude, longitude, dt_code, my_id = data 
							
						house_numbers = house_numbers.replace("{", "").replace("}", "") if house_numbers else "None"   
						buildingnames = buildingnames.replace("{", "").replace("}", "").replace("'", "") if buildingnames else "None"   



						#NOw Fetching Records from AWS_new 


						query = f"""

						select my_id  from building_no.final_id_{dt_code}

						where routepoint = '{routepoint}'

						"""

						aws_sqlExecuter.execute(query)
						records_building_no = aws_sqlExecuter.fetchall() 

						if records_building_no == []:
							print("We got no data!!! Ingested One ")


							insert_values = tuple([routepoint, house_numbers, addressroadid, buildingnames, latitude, longitude])
							query = f"""

								insert into building_no.final_id_{dt_code}

								(routepoint, housenumbe, addressroa, buildingna, latitude, longitude)
								VALUES {insert_values}

							"""
							print(query)
							aws_sqlExecuter.execute(query)
							aws_connection.commit()


							#update flag 

							query = f"""

									update {schema_name}
									set record_checked_flag = 1
									where my_id = {my_id}

							"""

							sqlExecuter.execute(query)
							connection.commit() 
						

							

							



		except Exception as e:

			print(str(e))
			slackAlert(f"We got an Error !!! {start}, {end}  i = {i}" + str(e))
	
		else:
			slackAlert(f"Successfully Fetch Lat long from RoutePoint -> Building data!!! {start}, {end}")
	
					

if __name__ == '__main__': 

	building_object = Building() 


	# building_object.getLatLongFromRoutePoint()
	building_object.pointInPolygon() 
	# building_object.InsertBuildingdataIntoAWSBuildingData() #not complete 
	


	