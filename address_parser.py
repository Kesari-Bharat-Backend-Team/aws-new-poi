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

		return self.locality_hashlocality_string[.strip().upper()]

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
		
		if (re.search ("Near-|Near|near|ke pass|pass|ke paas|ke samne|ke bahar|Inside|Infront|behind|beside|Opp.|opposite|apposite|^opposite to|above ", landmark_string.strip(), flags=re.IGNORECASE)):
			return True 
		return False 

	def filterRoadname(self, raod_name_string):

		if (re.search ("Road|Road$|Rd$|Marg|Lane|Ln|NH|SH|N.H|S.H|National Highway|State Highway|Road Number|Rd Number|Gali Number|Gali No.|Gali No|Street Number|Street No.|Street No|Street Number|Lane Number|Lane No|ward Number|ward|word no|path|gali", raod_name_string.strip() ,flags=re.IGNORECASE)):

			return True 
		return False
	
	def filterSubLocalityRegEx(self, sub_locality_string):

		# if (re.search ("Nagar|colony|vihar|society|residency|block|sector|complex|market|industrial area|mandi|chowk| patiya|chokadi|choraha|chauraha|sabji|ganj|basti|model town|area|garden|club|plaza|police lines|estate|hills|mohalla|trade center", sub_locality_string.strip(), flags=re.IGNORECASE)):

		if (re.search ("Nagar|colony|vihar|society", sub_locality_string.strip(), flags=re.IGNORECASE)):

			return True 
		return False
			
	def databaseQueries(self):

		connection = self.makeConnection() 
		sqlExecuter = connection.cursor() 

		for i in range(109, 110):


			query = f"""

				select address, my_id, latitude, longitude 
				from building_no.final_id_{i}
				limit 50;
			"""

			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for data in records:
			   
				address, id, latitude, longitude = data 

				# print("address = > ", address, '\t', id)
				address = address.split(",")
				remaining_address_data = address.copy() 

				for address_value in address[::-1]:
					
					if False and  self.filterStatePincode(sqlExecuter, address_value):
						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							continue 
					
					if False and self.filterDistrict(sqlExecuter, address_value):
						if address_value in  remaining_address_data:
							remaining_address_data.remove(address_value)
							continue

					if False and self.filterSubDistrict(sqlExecuter, address_value):
						if address_value in  remaining_address_data:
							remaining_address_data.remove(address_value)
							continue

					if False and self.filterLandmarks(address_value):
					
						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							continue

					if False and self.filterRoadname(address_value):

						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							continue
					
					if False and self.filterLocality(sqlExecuter, address_value):
						
						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							continue

					if False and self.filterSubLocality(sqlExecuter, address_value):
						
						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							print(address)
							print(address_value, latitude, longitude, 'poi_address_parser')
							print("\n" * 2)

							query = f""" 

								insert into public.sub_locality_points
								(name, latitude, longitude, data_source, pincode) 
								values('{address_value.strip()}', {latitude}, {longitude}, 'poi_address_parser', {pincode})

							"""
							sqlExecuter.execute(query)
							# connection.commit()
							continue

					elif self.filterSubLocalityRegEx(address_value):

						if address_value in remaining_address_data:
							remaining_address_data.remove(address_value)
							print(address)
							print(address_value, latitude, longitude, 'building_no')

							query = f""" 

								insert into public.sub_locality_points
								(name, latitude, longitude, data_source) 
								values('{address_value.strip()}', {latitude}, {longitude}, 'building_no')

							"""
							sqlExecuter.execute(query)
							# connection.commit()
							continue

				if remaining_address_data:
					# print(address)
					print("Remaining Address = ", remaining_address_data, '\tfinal_id_',i)
					print( '\n\n\n\n')
				
		#Last Line Don't remove it.
		connection.close()

if __name__ == "__main__":

	parser = AddressParser() 
	parser.databaseQueries() 