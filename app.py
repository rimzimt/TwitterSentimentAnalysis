#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 12:52:46 2019


"""

from flask import Flask,jsonify,request
from flask import render_template
import ast
import argparse
app = Flask(__name__)
labels = []
values = []
ip='localhost'

def argsStuff():
    parser = argparse.ArgumentParser(description = "Publish sentiments")
    parser.add_argument("-V", "--version", help="show program version", \
            action="store_true")
    parser.add_argument("-i", "--ip", help="ip address to publish chart",\
            type=str)
    args = parser.parse_args()
    if args.version:
        print("V1.1")
        exit(0)
    if not args.ip:
        print("The following arguments are required: -i/--ip")
        exit(1)
    return args.ip

@app.route("/")
def get_chart_page():
    global labels,values
    labels = []
    values = []
    return render_template('chart.html', values=values, labels=labels)
@app.route('/refreshData')
def refresh_graph_data():
    global labels, values
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    return jsonify(sLabel=labels, sData=values)
@app.route('/updateData', methods=['POST'])
def update_data():
    global labels, values
    if not request.form or 'data' not in request.form:
        return "error",400
    labels = ast.literal_eval(request.form['label'])
    values = ast.literal_eval(request.form['data'])
    print("labels received: " + str(labels))
    print("data received: " + str(values))
    return "success",201
if __name__ == "__main__":
    #app.run(host='localhost', port=5001)
    ip=argsStuff()
    app.run(host=ip, port=5001)
