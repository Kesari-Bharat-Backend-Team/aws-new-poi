import psycopg2
import re 
from collections import defaultdict 

class AddressParser:

	def __init__(self):

		self.hostname = 'localhost'  
		self.username = 'postgres'
		self.password = 'EiOiJja3ZqaTlrcnQyNzh'
		self.database = 'kesari_bharat'
		self.port = 5432
		self.state_hash = defaultdict(bool)
		self.district_hash = defaultdict(bool)
		self.sub_district_hash = defaultdict(bool)
		self.locality_hash = defaultdict(bool)
		self.sub_locality_hash = defaultdict(bool)

	def makeConnection(self):

		connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database, port=self.port)
		sqlExecuter = connection.cursor()
		return sqlExecuter, connection 
		
		return connection
	
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
				
				/*where  redundent_data_flag != 1*/
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
				/*where  redundent_data_flag != 1*/
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
		
		if (re.search ("Near-|Near|near|ke pass|pass|ke paas|ke samne|ke bahar|Inside|Infront|behind|beside|Opp.|opposite|apposite|opposite to|above|below|hospital|school ", landmark_string.strip(), flags=re.IGNORECASE)):
			return True 
		return False 

	def filterRoadname(self, raod_name_string):

		if (re.search ("Road|Road$|Rd$|Marg|Lane|National Highway|State Highway|Road Number|Rd Number|Gali Number|Gali No.|Gali No|Street Number|Street No.|Street No|Street Number|Lane Number|Lane No|ward Number|ward|word no|path|gali", raod_name_string.strip() ,flags=re.IGNORECASE)):

			return True 
		return False
	
	def filterSubLocalityRegEx(self, sub_locality_string):

		
		if (re.search ("Nagar|nager|colony|vihar|society|residency|block|sector|complex|market|industrial area|mandi|chowk| patiya|chokadi|choraha|chauraha|sabji|ganj|basti|model town|area|garden|club|plaza|police lines|estate|hills|mohalla|trade center|civil line|line|civil lines|lines|bagh|scheme|fort|enclave|extention|campus|bajar|bazar|Bazaar|camp|park|city", sub_locality_string.strip(), flags=re.IGNORECASE)):

		# if (re.search ("Nagar|colony|vihar|society", sub_locality_string.strip(), flags=re.IGNORECASE)):

			return True 
		return False
			
	def databaseQueries(self):

		sqlExecuter, connection = self.makeConnection() 

		for i in range(1, 736):


			query = f"""

				select address, id 
				from jan_2022_poi.poi_{i}
				where address is not Null and length(address) > 10 
				/*and data_source ilike '%sanjay_round_4%'
				limit 200;*/ 
			"""

			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
			   
				address, id = data 
				
				address = address.split(",")
				remaining_address_data = address.copy() 

				
				print("Address ->>>>    ",   ",".join(address).strip(), '\n')	

				road_name_set = set() 
				sub_locality_set = set() 
				landmark_set = set() 

				for address_value in address[::-1]:

					if self.filterStatePincode(sqlExecuter, address_value):
						# print("St_pincode \t\t", address_value)

						if address_value in remaining_address_data:
							# print("We got statPincode ", address_value)
							remaining_address_data.remove(address_value)
							continue 
					
					if  self.filterDistrict(sqlExecuter, address_value):
						# print("District \t\t", address_value)
						if address_value in  remaining_address_data:
							remaining_address_data.remove(address_value)
							continue

					if self.filterSubDistrict(sqlExecuter, address_value):

						# print("sub_District \t\t", address_value)

						if address_value in  remaining_address_data:
							remaining_address_data.remove(address_value)
							continue

					
					if self.filterLocality(sqlExecuter, address_value):
						
						if address_value in remaining_address_data:
							# print("locality \t\t", address_value)
							remaining_address_data.remove(address_value)
							continue

					if self.filterLandmarks(address_value):
					
						# print("Landmark \t\t", address_value)
						landmark_set.add(address_value.strip())
						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							continue
					

					if self.filterRoadname(address_value):

						# print("road \t\t", address_value)
						road_name_set.add(address_value.strip())
						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							continue

					# if self.filterSubLocality(sqlExecuter, address_value):
						
					# 	print("sub-locality \t\t", address_value)
					# 	if address_value in remaining_address_data:
					# 		remaining_address_data.remove(address_value)
					# 		continue
							

					elif self.filterSubLocality(sqlExecuter, address_value) or self.filterSubLocalityRegEx(address_value):


						sub_locality_set.add(address_value.strip())

						# print("sub-locality \t\t", address_value)
						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							continue
					
					
				if remaining_address_data:
					# print(address)

					print("\n\n")
					print("landmark set = ", landmark_set)
					print("sub_locality_set = ", sub_locality_set)
					print("road_set = ", road_name_set)
					print("\n\n")
					print("Remaining Extras = ", remaining_address_data, '\tfinal_id_',i)
					# print("Final Address = ", " ".join(address), '\tfinal_id_',i)
					print( '\n\n\n\n')
					print("*" * 150)
				
				query = f"""

						update jan_2022_poi.poi_{i}
set
						new_google_landmark = '{str(landmark_set).replace("'", "").replace("{", "").replace("}", "") if landmark_set else 'Null'}',
						new_google_road_name = '{str(road_name_set).replace("'", "").replace("{", "").replace("}", "") if road_name_set else 'Null'}',
						new_google_sub_locality = '{str(sub_locality_set).replace("'", "").replace("{", "").replace("}", "") if sub_locality_set else 'Null'}',
						 new_google_extra = '{",".join(remaining_address_data).replace("'", "") if remaining_address_data else 'Null'}'
						where id = {id}
				"""
				print(query)
				sqlExecuter.execute(query)
				# connection.commit()
				print("*" * 150)					
		#Last Line Don't remove it.
		connection.close()

	def pointInPolygonDtCode(self, schema_name, primary_key = 'id'):

		sqlExecuter, connection = self.makeConnection() 
		query = f""" 
			select 
			polygon.pincode_or_locality.pincode, polygon.pincode_or_locality. officename,
			polygon.pincode_or_locality.stname,
			polygon.pincode_or_locality.dtname,
			polygon.pincode_or_locality.sdtname,
			 {schema_name}.{primary_key}
			from {schema_name}, polygon.pincode_or_locality

			where 
				
			ST_intersects(
				polygon.pincode_or_locality.geom,
				ST_SetSRID(
					ST_MakePoint(longitude, latitude), 4326)
				
			) and {schema_name}.pincode is null
		"""
		print(query)
		sqlExecuter.execute(query)
		records = sqlExecuter.fetchall() 
		# print(records)
		for data in records:

			pincode, locality, state_name, district_name, sub_district_name, id = data 
		
			# print(data)

			query = f"""

			update {schema_name}

			set pincode = '{pincode}',
			locality_name = '{locality}',
			state_name = '{state_name}',
			district_name = '{district_name}',
			sub_district_name = '{sub_district_name}'
			where id = {id}

			"""

			print(query)
			sqlExecuter.execute(query)
			connection.commit() 
		print("Done!!! 😊")
			



if __name__ == "__main__":

	parser = AddressParser() 
	# parser.databaseQueries() 
	for i in range(1, 736):

		parser.pointInPolygonDtCode(f"jan_2022_poi.poi_{i}", 'id')

