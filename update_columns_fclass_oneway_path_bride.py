import psycopg2
from math import radians, cos, sin, asin, sqrt
from collections import defaultdict 

hash = {
	None : "unknown",
	'path\\' : "unknown",
	'facebook_output/171.shp' : "unknown",
	"track\\" : "unknown",
	"service\\" : "unknown",
1 : "unknown",
0 : "unknown",
	2 : "unknown",
	3 : "unknown",
	4 : "unknown",
	5 : "unknown",
	6 : "unknown",
	"" : "unknown",
	'-path': 'path',
	'.': 'unknown',
	']': 'unknown',
	'8920219.56,1460746.34': 'unknown',
	'9 +': 'unknown',
	'a': 'unknown',
	'B D Desai Marg': 'unknown',
	'Bajghera Rd': 'unknown',
	'bike': 'unknown',
	'Bombay Port Trust Road': 'unknown',
	'colony Rd': 'unknown',
	'Cooch Bihari Rajbari Rd': 'unknown',
	'done stellite': 'unknown',
	'ervice': 'service',
	'F': 'unknown',
	'f ootway': 'footway',
	"facebook_output/171.shp'": 'unknown',
	'foot': 'footway',
	'Foot path': 'footway',
	'footbridge': 'footway',
	'footpath': 'footway',
	'Footway': 'footway',
	'FOOTWAY': 'footway',
	'footway': 'footway',
	"footway'": 'footway',
	'fooyway': 'footway',
	'Formula Car Racing Track': 'unknown',
	'fottway': 'footway',
	'fotway': 'footway',
	'fpptway': 'footway',
	'Gali No. 7': 'residential',
	'Gauri Sankar marg': 'residential',
	'Hare Krishna Road': 'residential',
	'Jangpura Railway Under Bridge': 'residential',
	'Khizrabad Village Rd': 'residential',
	'kuti Rd': 'residential',
	'Malviya Nagar Rd': 'residential',
	'Mysore Rd': 'residential',
	'Nawab Tank Road': 'residential',
	'NH344': 'trunk',
	'NpathULL': 'residential',
	'null': 'unknown',
	'NULLATH': 'path',
	'NULLath': 'path',
	'NULLck': 'unknown',
	'NULLh': 'unknown',
	'NULLpath': 'path',
	'NULLresidential_bike': 'residential_bike',
	'NULLservice': 'service',
	'NULLth': 'unknown',
	'NULLtrack': 'track',
	'NULLtrck': 'track',
	'NULLunknown_bike': 'unknown_bike',
	'NULpath': 'path',
	'NULTRACK': 'track',
	'oath': 'path',
	'Off New Link Road': 'residential',
	'p[ath': 'path',
	'pa5th': 'path',
	'pah': 'path',
	'parh': 'path',
	'PARH': 'path',
	'park': 'path',
	'parth': 'path',
	'pass': 'path',
	'pat': 'path',
	'patg': 'path',
	'path': 'path',
	'Path': 'path',
	'PATH': 'path',
	'PAth': 'path',
	'pATH': 'path',
	'path  path': 'path',
	'path .': 'path',
	"path '": 'path',
	'path;': 'path',
	'path.': 'path',
	'path.3': 'path',
	"path'": 'path',
	"PATH'": 'path',
	'path]': 'path',
	'path2': 'path',
	'path20': 'path',
	'path3': 'path',
	'path6': 'path',
	'patha': 'path',
	'pathAC': 'path',
	'pathb': 'path',
	'pathf': 'path',
	'pathg': 'path',
	'pathj': 'path',
	'pathL': 'path',
	'pathv': 'path',
	'pathy': 'path',
	'patj': 'path',
	'patjh': 'path',
	'patn': 'path',
	'patrh': 'path',
	'patth': 'path',
	'patyh': 'path',
	'payh': 'path',
	'pri mary': 'primary',
	'primarry': 'primary',
	'PRIMARY': 'primary',
	'primary': 'primary',
	'Primary': 'primary',
	'primaryprimary': 'primary',
	'psth': 'path',
	'Pth': 'path',
	'pth': 'path',
	'rack': 'track',
	'RACK': 'track',
	'Rd No. 4': 'unknown',
	'Rd No. 8': 'unknown',
	'resdential': 'resdential',
	'residenresidential_biketial_bike': 'residential_bike',
	'RESIDENTIAL': 'residential',
	'residential': 'residential',
	'residential bike': 'residential_bike',
	'residential_ auto': 'residential_auto',
	'residential_ bike': 'residential_bike',
	'residential_ bike2': 'residential_bike',
	'residential_)bike': 'residential_bike',
	'residential_auto': 'residential_auto',
	'residential_bike': 'residential_bike',
	'residential_Bike': 'residential_bike',
	'residential_car': 'residential_car',
	'residential_path': 'residential_bike',
	'residential_Path': 'residential_bike',
	'residential_truck': 'residential_truck',
	'residential-bike': 'residential_bike',
	'residentisl_bike': 'residential_bike',
	'restricted': 'restricted',
	'road': 'unknown',
	'Road To Village Surebardi': 'unknown',
	'runway': 'runway',
	'sarvice': 'service',
	'SECONDARY': 'secondary',
	'Secondary': 'secondary',
	'secondary link': 'secondary',
	'secondarysecondary': 'secondary',
	'sercvis': 'service',
	'serrvice': 'service',
	'servce': 'service',
	'serves': 'service',
	'servi ce': 'service',
	'servic e': 'service',
	'servicce': 'service',
	'SERVICE': 'service',
	'service': 'service',
	'Service': 'service',
	'service  road': 'service',
	'service line': 'service',
	'Service line': 'service',
	'service Rd': 'service',
	'service rd': 'service',
	'Service Rd': 'service',
	'service road': 'service',
	'Service Service': 'service',
	'service.': 'service',
	"service'": 'service',
	'service2': 'service',
	'servicee': 'service',
	'servicef': 'service',
	'serviceRd': 'service',
	'servie': 'service',
	'servise': 'service',
	'servive': 'service',
	'sevice': 'service',
	'SH6 Westbound Bachau': 'residential',
	'shivaji nagar cir': 'residential',
	'Shraddhanand Road': 'residential',
	'step': 'steps',
	'STEPS': 'steps',
	'stpes': 'unknown',
	'Street No. 7': 'unknown',
	'T': 'unknown',
	'tack': 'track',
	'tarackl': 'track',
	'tarck': 'track',
	'TERACK': 'track',
	'tertairy': 'tertiary',
	'TERTIARY': 'tertiary',
	'tertiary': 'tertiary',
	'tertiary link': 'tertiary_link',
	'tertiary_auto': 'tertiary_auto',
	'tertiary_bike': 'tertiary_bike',
	'tertiary_car': 'tertiary_car',
	'tertiary_truck': 'tertiary_truck',
	'trac': 'track',
	'TRACK': 'track',
	'Track': 'track',
	'track': 'track',
	"track'": 'track',
	'trackj': 'track',
	'trackl': 'track',
	'tracl': 'track',
	'trck': 'track',
	'tritary': 'tertiary',
	'Trunk': 'trunk',
	'trunk': 'trunk',
	'trunk link': 'trunk_link',
	'trunk_bypass': 'trunk',
	'Turnk': 'trunk',
	'turnk': 'trunk',
	'TURNK': 'trunk',
	'uater Para Rd': 'unknown',
	'uknown_bike': 'unknown_bike',
	'unclassified_bike': 'unknown_bike',
	'unclassified_path': 'path',
	'Under Pass': 'residential',
	'under Pass': 'residential',
	'Underpass': 'residential',
	'underpass': 'residential',
	'Unkhown_Path': 'path',
	'unknoown_bike': 'unknown_bike',
	'unknow_auto': 'unknown_auto',
	'unknown bike': 'unknown_bike',
	'unknown__ bike': 'unknown_bike',
	'unknown_auto': 'unknown_auto',
	'unknown_bike': 'unknown_bike',
	'unknown_Bike': 'unknown_bike',
	'Unknown_Bike': 'unknown_bike',
	'UNKNOWN_BIKE': 'unknown_bike',
	"unknown_bike'": 'unknown_bike',
	'unknown_boke': 'unknown_bike',
	'unknown_car': 'unknown_car',
	'unknown_path': 'path',
	'unknown_Path': 'path',
	'Unknown_Path': 'path',
	'unknown_truck': 'unknown_truck',
	'unknownbike': 'unknown_bike',
	'unknownpath': 'path',
	'Unkown_Bike': 'unknown_bike',
	'Unkown_Path': 'path',
	'v': 'unknown',
	'V': 'unknown',
	'walking  track': 'path',
	'walking path': 'path',
	'wkt_geom': 'gid',
	'yrack': 'track'
}

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

			

	def updateOneWay(self):


		onewayHash = {
			'T1': 'T',
			'TT': 'T',
			'b': 'B',
			'3': 'B',
			'T22': 'T',
			'm': 'B',
			'1': 'B',
			'path': 'B',
			'2': 'B',
			'T4': 'T',
			'//': 'B',
			'f': 'B',
			'service': 'B',
			'T0': 'T',
			'None': 'B',
			'T2': 'T',
			't': 'T',
			'T6': 'T',
			'T\\': 'T', 
			'liSahebAmirSahebFaki': 'B'
		}
							
		sqlExecuter, connection = self.makeConnection() 


		start = int(input("Enter starting table = "))
		end = int(input("Enter ending table = "))

		my_set = set() 


		for i in range(start, end):

			print("i = ", i)
			
			if i in [392, 439, 593, 679, 691]: continue 
		
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

				select distinct oneway from {table_path}
				where oneway is not null;

			"""
			sqlExecuter.execute(query)
			records = sqlExecuter.fetchall() 

			for oneway in records:

				oneway = oneway[0]
				
				if oneway:

					value = onewayHash.get(oneway)

					if value:


						print("value = ", value)
						query = f"""

							update {table_path}

							set oneway = '{value}'

							where oneway = '{oneway}'


						"""
						print(query)
						sqlExecuter.execute(query)
						connection.commit()

				
			
		print(my_set)


	def updateFclass(self):

		sqlExecuter, connection = self.makeConnection() 


		start = int(input("Enter starting table = "))
		end = int(input("Enter ending table = "))

		my_set = set() 


		for i in range(start, end):
			
			if i in [392, 439, 593, 679, 691]: continue 
		
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


			#step 1 #update Null Values 

			column_names = {'fclass': 'unknown',  'oneway': 'B', 'path' : 'self', 'bridge' : 'F'}

			for column_name, value in column_names.items():

				query = f"""
					update {table_path}
					set {column_name} = '{value}'
					where {column_name} is null or {column_name} = ''
				"""

				print(query)
				sqlExecuter.execute(query)
				connection.commit()


			query = f"""

					select distinct fclass 
					from {table_path}

			"""
			print(query)
			sqlExecuter.execute(query)

			records = sqlExecuter.fetchall() 




			for fclass in records:

				fclass = fclass[0]

				my_set.add(fclass)


				if fclass:

					update_value = hash.get(fclass)

					if update_value:

						
						fclass = fclass.replace("'", "''") # path '


						query = f"""

							update {table_path}

							set fclass = '{update_value}'
							where fclass = '{fclass}'

						"""
						sqlExecuter.execute(query)
						connection.commit() 

		print(my_set)




if __name__ == '__main__':

	line_object = LineData() 
	input_value = int(input("Enter 1 for Fclass and 2 for oneway"))
	if input_value == 1:
		line_object.updateFclass()
	elif input_value == 2:
		line_object.updateOneWay() 