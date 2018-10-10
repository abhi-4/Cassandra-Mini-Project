#!/usr/bin/env python
import ast
import logging
import os
import json
from cassandra.cluster import Cluster

KEYSPACE = "cassandra_assignment"#the keyspace created for assignment
path = '/home/abhi/CS345/Cassandra/workshop_dataset1'#path to json files

def main():
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()#connecting to cassandra

	print("creating keyspace...")
	session.execute("""
	CREATE KEYSPACE IF NOT EXISTS %s
	WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
	""" % KEYSPACE) #creating keyspace

	print("setting keyspace...")
	session.set_keyspace(KEYSPACE)#using the keyspace

	print("creating table query3...")#table created for query2
	session.execute("""
	CREATE TABLE IF NOT EXISTS table1 ( 
			location text, 
			mention text,
			PRIMARY KEY (location, mention) ) WITH CLUSTERING ORDER BY (mention DESC);
	
	""")
	print("table created for query3...")

	print("creating table query8...")#table created for query2
	session.execute("""
	CREATE TABLE IF NOT EXISTS table2 ( 
			day date,
			hashtags text,
			mention text,
			PRIMARY KEY (mention, hashtags) ) WITH CLUSTERING ORDER BY (hashtags DESC);
	
	""")
	print("table created for query8...")


	for root, dirs, files in os.walk(path, topdown=False):#use os.walk to traverse directory
		for name in files:#for each file
			filename = os.path.join(root, name)
			with open(filename) as data_file:	
				data = json.load(data_file)

				for key, value in data.items() :
					val = value

					if(val['mentions']!= None):#for each keyword the tweet iteratively
						for z in val['mentions']:
							if(z!= None):
								if not val['location']:
									val['location'] = "NA"
								session.execute("""
								INSERT INTO cassandra_assignment.table1(
								location, 
								mention
								)
								VALUES (%s, %s)
								""",
								(
								 val['location'],
								 z)
								)



	for root, dirs, files in os.walk(path, topdown=False):#use os.walk to traverse directory
		for name in files:#for each file
			filename = os.path.join(root, name)
			with open(filename) as data_file:	
				data = json.load(data_file)

				for key, value in data.items() :
					val = value

					if(val['hashtags']!= None):#for each keyword the tweet iteratively
						for y in val['hashtags']:
							if(y!= None):

								if(val['mentions']!= None):#for each keyword the tweet iteratively
									for z in val['mentions']:
										if(z!= None):
											session.execute("""
											INSERT INTO cassandra_assignment.table2(
											day, 
											hashtags,
											mention
											)
											VALUES (%s, %s, %s)
											""",
											(
											 val['date'], 
											 y,
											 z)
											)


if __name__ == "__main__":
	main()