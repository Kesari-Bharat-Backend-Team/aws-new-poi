import re 
import csv  
import psycopg2
from collections import defaultdict 
from math import radians, cos, sin, asin, sqrt


class AddressParser:

	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'postgres'
		self.password = 'EiOiJja3ZqaTlrcnQyNzh'
		self.database = 'kesari_bharat'
		self.port = 5432
		self.sub_locality_hash = defaultdict(list)
		self.state_hash = defaultdict(bool)
		self.district_hash = defaultdict(bool)
		self.sub_district_hash = defaultdict(bool)
		self.locality_hash = defaultdict(bool)


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


	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection
	
	def filterStatePincode(self,sqlExecuter, state_pincode_data):

		if len(self.state_hash) == 0:
		
			query = """
				select state from try_admin_point.state_capitals
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()

			for data in records:
				self.state_hash[data[0].strip().upper()] = True
		
		state_pincode_data = state_pincode_data.split(" ") if " " in state_pincode_data else state_pincode_data.split(",")

		for data in state_pincode_data:
			if (data.isdigit() and len(data) == 6 ) or self.state_hash[data.strip().upper()]:
				return True 
		return False 
	
	def filterDistrict(self, sqlExecuter, district_string):
	
	
		if len(self.district_hash) == 0:
		
			query = """
				select dtname from try_admin_point.district_capitals
				/*where  redundent_data_flag != 1 */
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()

			for data in records:
				self.district_hash[data[0].strip().upper()] = True

		return self.district_hash[district_string.strip().upper()]

	def filterSubDistrict(self, sqlExecuter, sub_district_string):
	
	
		if len(self.sub_district_hash) == 0:
		
			query = """
				select name11 from try_admin_point.subdistrict_capitals
				where  redundent_data_flag != 1
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()

			for data in records:
				self.sub_district_hash[data[0].strip().upper()] = True

		return self.sub_district_hash[sub_district_string.strip().upper()]
		
	def filterLocality(self, sqlExecuter, locality_string):
	
		if len(self.locality_hash) == 0:
		
			query = """
				SELECT officename FROM point.locality_point
				where  redundent_data_flag != 1
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()

			for data in records:
				self.locality_hash[data[0].strip().upper()] = True

		return self.locality_hash[locality_string.strip().upper()]

	def filterSubLocality(self, sqlExecuter, sub_locality_string):
	
		if len(self.sub_locality_hash) == 0:
		
			query = """
				SELECT name FROM point.sub_locality_point_2
				where  redundent_data_flag != 1
			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall()

			for data in records:
				self.sub_locality_hash[data[0].strip().upper()] = True

		return self.sub_locality_hash[sub_locality_string.strip().upper()]
		
	def filterLandmarks(self, landmark_string):
		
		if (re.search ("Near-|Near|near|ke pass|pass|ke paas|ke samne|ke bahar|Inside|Infront|behind|beside|Opp.|opposite|apposite|opposite to|above ", landmark_string.strip(), flags=re.IGNORECASE)):
			return True 
		return False 

	def filterRoadname(self, raod_name_string):

		# if (re.search ("Road|Road$|Rd$|Marg|Lane$|Ln$|^NH|^SH|^N.H|^S.H|^National Highway|^State Highway|^Road Number|^Rd Number|^Gali Number|^Gali No.|^Gali No|^Street Number|^Street No.|^Street No|^Street Number|^Lane Number|^Lane No|ward Number|ward|word no|path|gali", raod_name_string.strip() ,flags=re.IGNORECASE)):


		if (re.search ("Road|Rd|Marg|Lane|National Highway|State Highway|Road Number|Rd Number|Gali Number|Gali No.|Gali No|Street Number|Street No.|Street No|Street Number|Lane Number|Lane No|ward Number|ward|word no|path|gali|marg|rastha|ka rastha|rasta", raod_name_string.strip() ,flags=re.IGNORECASE)):

			return True 
		return False
	
	def filterSubLocalityRegEx(self, sub_locality_string):

		if (re.search ("Nagar|colony|vihar|society|residency|block|sector|complex|market|industrial area|mandi|chowk| patiya|chokadi|choraha|chauraha|sabji|ganj|basti|model town|area|garden|club|plaza|police lines|estate|hills|mohalla|trade center|civil line|line|civil lines|lines|bagh|scheme|fort|enclave|extention|campus|bajar|bazar|camp|park", sub_locality_string.strip(), flags=re.IGNORECASE)):

		# if (re.search ("Nagar|colony|vihar|society|residency|complex|market|industrial area|gardan|basti|sector|block|area|plaza", sub_locality_string.strip(), flags=re.IGNORECASE)):

			return True 
		return False
			
	def filterData(self, sub_locality_string):

		sub_locality = sub_locality_string.split(" ")

		name = sub_locality[0]
		name = name.replace("#", "").replace(")", "").replace("(", "").replace(":", "")

		if len(name) >= 15: return sub_locality_string

		if '/' in name or '\\' in name or ("zone" not in name.lower() and "sector" not in name.lower
		() and  'block' not in name.lower() and  '-' in name) or (sum([1 for ch in name if ch.isdigit() ]) >= 3) : 
		
			return " ".join(sub_locality[1:])
		
		return sub_locality_string
		

	def sendCSVdata(self, schema_name, start = 109, end = 110):

		sqlExecuter, connection = self.makeConnection()

		for i in range(start, end):

			file = open(f'Sub-locality-list-building-address/sub_locality_list_{i}.csv')

			csvreader = csv.reader(file)

			for row in csvreader:

				name = row[0]
				latitude, longitude = eval(row[2])

				query  = f"""

					insert into  sub_locality_points.sub_locality_points_round_1

					(name, latitude, longitude, geom, data_source) 
					values ('{name}', {latitude}, {longitude}, st_makepoint({longitude}, {latitude}), '{f'final_id_{i}'}')
				"""

				sqlExecuter.execute(query)
				connection.commit()
			file.close()
			print("DAta sent ", i)
		
		connection.close()



	def databaseQueries(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(1, 736):

			print(f"file {i} is running")

			# writer = csv.writer (open(f'sub-locality-list-poi-google/sub_locality_list_{i}.csv', 'a'))
			# query = f"""

			# 	select address, id, latitude, longitude from 
			# 	jan_2022_poi.poi_{i}
			# 	where address is not null
			# """
			file = open(f'Sub-locality-list-building-address/sub_locality_list_{i}.csv', 'a')
			writer = csv.writer(file)
			query = f"""

				select address, my_id, latitude, longitude 
				from building_no.final_id_{i}
				
			"""


			counter = 0 
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
			   
				address, my_id, latitude, longitude = data 

				address = address.split(",")
				counter += 1 
				# print(address)
				# print("Counter = ", counter, end = '\n')

				prev_block = None 
				prev_sector = None 
				block_flag = False 
				sector_flag = False
				block_block_flag = 0 
				sector_sector_flag = 0 
				for address_value in address:

					if self.filterLandmarks(address_value):
						# print("we got a landmark ", address_value)
						continue

					if self.filterRoadname(address_value):

							# print("We got a roadname ", address_value)
							continue
					

					if self.filterSubLocalityRegEx(address_value): # or block_flag or sector_flag:
						if 'block'.upper() in address_value.upper():


							if  len(address_value) < 10:
								block_flag = True 
								prev_block = address_value
								block_block_flag = 1 
								# print("we got a block ", address_value, address)
								# continue 
							
						if block_flag and block_block_flag == 0 :
							address_value = ",".join([prev_block.strip().upper(), address_value.strip().upper()])
							# print("our block flag = ", True, address_value)
						
						if block_flag and  block_block_flag == 1:
							block_block_flag = 0 


							
						
						if 'sector'.upper() in address_value.strip().upper():

							if len(address_value) < 15:
								sector_flag = True 
								prev_sector = address_value
								sector_sector_flag = 1 
								# print("we got the sector ", address_value, ' ->> ', address)
								# continue
							
						if sector_flag and not block_flag and  sector_sector_flag == 0:
							address_value = ','.join([prev_sector.strip().upper(), address_value.strip().upper()])

						if sector_flag and not block_flag and  sector_sector_flag == 1 :
							sector_sector_flag = 0 

						address_value = self.filterData(address_value) 
						address_value = self.filterData(address_value) #calling 2 times
						address_value = self.removeStartingdata(address_value)
						address_value = address_value.strip().upper()
						if len(address_value) < 4:
							#  and any ( [ True for word in ['zone', 'sector', 'block'] if word.upper() in address_value] ) :
							continue 

						if  address_value not in self.sub_locality_hash:

								self.sub_locality_hash[address_value.strip()].append([float(latitude), float(longitude)])

						else:
							
							min_distance = 1000

							for lat_long_pair in self.sub_locality_hash[address_value.strip()]:
								old_latitude, old_longitude = lat_long_pair
								total_distance = self.distance(old_latitude, latitude, old_longitude, longitude)
								min_distance = min(min_distance,  abs(total_distance - min_distance), total_distance) 

							if min_distance > 5: # and min_distance != 1000:
								self.sub_locality_hash[address_value.strip()].append([float(latitude), float(longitude)])
								

			for key, value in self.sub_locality_hash.items():
				print("Performing write operation in CSV file!!!")
				for data in value:
					writer.writerow([key, '\t\t', data])

			self.sub_locality_hash = defaultdict(list) 
			file.close()
			self.sendCSVdata("sub_locality_points.sub_locality_points", i, i + 1)
		#Last Line Don't remove it.
		connection.close()

	def removeStartingdata(self, string):

		s = string.split(" ")

		answer = ""
		flag = True 
		for word in s:

			if flag and len(word) <= 5 and sum([1 for ch in word if ch.isdigit()]) >= 1 and 'zone' not in word.lower() and 'st'.upper() not in word.upper() and 'nd'.upper() not in word.upper():
				pass 
			else:
				flag = False 
				answer = answer +  " " + word
		
		return answer if answer else ""



if __name__ == "__main__":

	parser = AddressParser() 
	parser.databaseQueries() 
	# for i in range(1, 736):
	# 	parser.sendCSVdata("sub_locality_points.sub_locality_points", i, i + 1)