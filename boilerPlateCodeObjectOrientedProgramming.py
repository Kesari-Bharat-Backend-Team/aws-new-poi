import psycopg2
from math import radians, cos, sin, asin, sqrt

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


	def updateColumns(self):

		sqlExecuter, connection = self.makeConnection() 

		query = f""" 
			
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'edited_line') and 
			table_name ilike 'edi%' 
			
		"""

		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()

		for i in all_tables:

			table_name = i[0]

			column_names = {'fclass': 'unknown',  'oneway': 'B', 'path' : 'self', 'bridge' : 'F'}

			for column_name, value in column_names.items():

				query = f"""
					update edited_line.{table_name} 
					set {column_name} = '{value}'
					where {column_name} is null
				"""
				sqlExecuter.execute(query)
				# connection.commit()
				print(query)
			# break 

			
	def checkDuplicateRoad(self):

		sqlExecuter, connection = self.makeConnection() 

		query = f""" 
			
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'edited_line') and 
			table_name ilike 'edi%' 
			
		"""

		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()

		hash = dict() 

		for i in all_tables:

			table_name = i[0]
			if table_name == 'edited_44': continue
			query = f"""
					select count(*) from edited_line.{table_name}
					where duplicate_flag = 0 and checkduplicateflag = 0 and path ilike 'facebook%'  
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()[0][0] 
			
			if records:
				hash[table_name] = records
				print(table_name, "-->> ", records)
			
		print(hash)


	def deleteDatawhereGeomisNULL(self):
		
		sqlExecuter, connection = self.makeConnection() 

		schema_name = 'edited_line'

		query = f"""
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = '{schema_name}')
		"""


		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall()


		for table_name in records:

			table_name = table_name[0]

			query = f"""

				delete from {schema_name}.{table_name}
				where geom is null 
								
			"""
			sqlExecuter.execute(query)
			connection.commit() 
			# print(query)

			print("Done!!! ", table_name)




	def countGeomNull(self, schema_name):

		sqlExecuter, connection = self.makeConnection() 

		query = f"""

					
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = '{schema_name}')
		"""


		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall()


		total = 0
		for table_name in records:

			table_name = table_name[0]

			query = f"""

				select count(*) from {schema_name}."{table_name}"
				where geom is null 
			"""
			sqlExecuter.execute(query)
			total_recorrds = sqlExecuter.fetchall()[0][0]
			total = total + total_recorrds

			print(total)

	def updateSRIDTo4326(self):


		sqlExecuter, connection = self.makeConnection() 

		query = f""" 
			
			SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA = 'edited_line') and 
			table_name ilike 'edi%' 
			
		"""

		sqlExecuter.execute(query)
		all_tables = sqlExecuter.fetchall()

		for i in all_tables:

			table_name = i[0]

		query = f"""

		select updategeometrysrid('edited_line','edited_110', 'geom', 4326);

		"""

	def dropColumns(self):

		sqlExecuter, connection = self.makeConnection() 

		start, end = 1, 736

		for i in range(start, end):
			
			
			if i in [392, 439, 593, 691, 679]: continue 

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

				table_path = f""" edited_line . edited_{i} """

			
			#drop query 

			query = f"""

				ALTER TABLE 
			
					IF EXISTS {table_path} 
				
					DROP COLUMN IF EXISTS __gid
					,DROP COLUMN IF EXISTS id_road
					,DROP COLUMN IF EXISTS road_flag
					,DROP COLUMN IF EXISTS drp_road_id
					,DROP COLUMN IF EXISTS drp_road_name
					,DROP COLUMN IF EXISTS drp_road_category;

			"""
			print("i = ", i)
			sqlExecuter.execute(query) 

			connection.commit()

	def dropColumnsInBuildingPolygons(self):

		sqlExecuter, connection = self.makeConnection() 

		start, end = 1, 736

		for i in range(start, end):
			
			
			if i in [392, 439, 593, 691, 679]: continue 

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

				table_path = f""" edited_line . edited_{i} """

			
			#drop query 

			query = f"""

				ALTER TABLE 
			
					IF EXISTS {table_path} 
				
					DROP COLUMN IF EXISTS __gid
					,DROP COLUMN IF EXISTS id_road
					,DROP COLUMN IF EXISTS road_flag
					,DROP COLUMN IF EXISTS drp_road_id
					,DROP COLUMN IF EXISTS drp_road_name
					,DROP COLUMN IF EXISTS drp_road_category;

			"""
			print("i = ", i)
			sqlExecuter.execute(query) 

			connection.commit()


	
	def countDuplicateFlag(self):

		sqlExecuter, connection = self.makeConnection() 


		start, end = 630, 736


		total = 0 
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

			if result:

				table_path = f"""line."{i}" """

			else:

				table_path = f""" edited_line . edited_{i} """

			

			query = f"""

			select count(*) from {table_path}
			where duplicate_flag = 1
			"""

			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()[0][0]

			print(table_path, " \t total_duplicates  = ", records)
			total += records
		
		print("Total Duplicates = ", total)


if __name__ == '__main__':

	line_object = LineData() 
	
	line_object.countDuplicateFlag()
	# line_object.dropColumns()

	# line_object.dropColumnsInBuildingPolygons()
	# line_objec


	# line_object.deleteDatawhereGeomisNULL() 
	# line_object.updateSRIDTo4326()
	
	# line_object.countGeomNull("edited_line")
	# line_object.countGeomNull("line")

	# line_object.checkDuplicateRoad() 
	# line_object.updateColumns()
	