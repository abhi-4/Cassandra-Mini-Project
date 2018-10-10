from flask import Flask
from flask_cassandra import CassandraCluster
from cassandra.cluster import Cluster
from flask import render_template, redirect, request
from datetime import datetime, timedelta

app = Flask(__name__)
cassandra = CassandraCluster()

app.config['CASSANDRA_NODES'] = ['cassandra-c1.terbiumlabs.com']  # can be a string or list of nodes


@app.route('/')
def query_1():
	cluster = Cluster(['127.0.0.1'])
	session = cluster.connect()
	session.set_keyspace("cassandra_assignment")
	cql1 = "SELECT location, mention, COUNT(*) AS freq FROM table1 WHERE location = 'Delhi' GROUP BY location, mention allow filtering;" 	
	r1 = session.execute(cql1)
	cql2 = "SELECT day, mention, hashtags, COUNT(*) AS freq FROM table2 WHERE day = '2018-01-15' GROUP BY mention, hashtags allow filtering;"
	r2 = session.execute(cql2)
	return render_template('query_output.html', output1 = r1, output2 = r2)




if __name__ == '__main__':
	app.run()