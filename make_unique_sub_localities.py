import psycopg2
from collections import defaultdict 
from fuzzywuzzy import fuzz

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

	def makeUniqueSublocalities(self):

		sqlExecuter, connection = self.makeConnection() 

		sub_locality_hash = defaultdict(list)


		for i in range(1, 736):
			# 
			query = f"""
			SELECT new_google_sub_locality, latitude, longitude FROM jan_2022_poi.poi_{i}
			where new_google_sub_locality is not Null and sub_locality_name is not  Null and sub_locality_name not ilike 'null' and new_google_sub_locality not ilike 'null'
			
			"""
			sqlExecuter.execute(query)
			total_records = sqlExecuter.fetchall() 

			# print("total_i = ", total_records[0][0])
			for data in total_records:

				new_sub_loc, latitude, longitude = data 
				latitude, longitude = float(latitude), float(longitude)
				sub_locality_hash[new_sub_loc.strip()] = [latitude, longitude]
				# sub_locality_hash[old_sub_loc.strip()] = [latitude, longitude]

			for key, value in sub_locality_hash.items():

				print("Key = ", key)
				print("value = ", value)
				latitude, longitude = value
				query = f"""

				Insert into sub_locality_points.sub_locality_names_from_google

				(name , latitude, longitude, data_source) values('{key}', {latitude}, {longitude}, 'poi_{i}')

				"""
				sqlExecuter.execute(query)
				connection.commit() 

				# break 
			print("*" * 150)
			print("Done !!! ", i )

		# print(sub_locality_hash)



if __name__ == '__main__':

	poi_object = PoiData() 
	poi_object.makeUniqueSublocalities() 
	