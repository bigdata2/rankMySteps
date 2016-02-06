from flask import jsonify 
from flask import render_template
from app import app
from cassandra.cluster import Cluster
import json
import operator
from datetime import datetime
from pytz import timezone

cluster = Cluster(['ec2-52-88-171-137.us-west-2.compute.amazonaws.com'])
session = cluster.connect("rank_steps")

@app.route('/')
@app.route('/index')
def index():
   return "Hello, World!"

@app.route('/realtime')
def realtime():
 return render_template("realtime.html")

@app.route('/api')
def get_email():
        top_walkers = {}
        cnt = 0
        todaysDate = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d')
        stmt = "SELECT * FROM top_walkers8 "\
               "where arrival_time=" + "'"+todaysDate+"'" + " limit 100"
        response = session.execute(stmt)
        response_list = []
        for val in response:
                 response_list.append(val)

        jsonresponse = [{"userid ": x.user, "steps_taken": x.num_steps}\
                          for x in response_list]

        return json.dumps(jsonresponse)
