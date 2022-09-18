import psycopg2
from collections import defaultdict 



import json
import sys
import random
import requests



class Navigation:

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

	
	def convertMulitiLIneToLineString(self):


		sqlExecuter, connection = self.makeConnection() 


		schema_name = "public.copy_of_final_id_109"
		query = f"""

		SELECT  (ST_Dump(geom)).geom, gid  FROM {schema_name}
		
		"""
	
		sqlExecuter.execute(query)

		records = sqlExecuter.fetchall() 


		for data in records:

			new_geom, gid = data 

			query = f"""

				update {schema_name}

				set new_geom = '{new_geom}'
				where gid = {gid}

			"""
			print(query)
			sqlExecuter.execute(query)
			connection.commit()

		print("Done!!!")


	def updateColumns(self):


		sqlExecuter, connection = self.makeConnection() 


		start, end = 1, 736

		for i in range(start, end):

			# query = f"""
			# 	ALTER TABLE IF EXISTS public.final_id_{i}
			# 	RENAME "fclass" TO "highway"
			# """

			query = f"""

				update public.final_id_{i}
				set bridge = 'no'
				where bridge = 'F'


			"""

			sqlExecuter.execute(query)
			connection.commit()
			print(i)

		print("Done!!!")





if __name__ == '__main__':

	navigation_object = Navigation() 
	navigation_object.updateColumns()
	
	# navigation_object.convertMulitiLIneToLineString() 
