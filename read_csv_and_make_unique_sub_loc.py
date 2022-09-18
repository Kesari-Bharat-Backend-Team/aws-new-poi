import psycopg2
import csv 
from collections import defaultdict 


def myQuery(connection):

    for i in range(1, 736):

        hash = defaultdict(list)

        file = open(f'Sub-locality-list-building-address/sub_locality_list_{i}.csv')
        csvreader = csv.reader(file)

        for row in csvreader:

            hash[row[0]] = eval(row[2])

        print("building_data")
        # print(hash)

        file = open(f'sub-locality-list-poi-google/sub_locality_list_{i}.csv')
        csvreader = csv.reader(file)

        for row in csvreader:

            hash[row[0]] = eval(row[2])

        print("poi_data")
        # print(hash)

        writer = csv.writer (open(f'final_merged_sub_locality/sub_locality_list_{i}.csv', 'a'))
        
        for key, value in hash.items():

            writer.writerow([key, '\t\t', value])
        
        print("Done!!!", i)



def connect_to_Database():
    
    hostname = 'localhost'  
    username = 'postgres'
    password = 'EiOiJja3ZqaTlrcnQyNzh'
    database = 'kesari_bharat'
    port = 5432

    myConnection = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
    myQuery(myConnection)
    myConnection.close()

connect_to_Database()
