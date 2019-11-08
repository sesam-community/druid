from functools import wraps

import copy
from time import sleep

import cherrypy
from flask import Flask, request, Response, abort, stream_with_context
import datetime
import requests
import io
import os, os.path

import json
import logging
from pathlib import Path

app = Flask(__name__)

index_template = {
    "type": "index",
    "spec": {
      "dataSchema": {
        "dataSource": "<FILL IN SOURCE>",
        "parser": {
          "type": "string",
          "parseSpec": {
            "format": "json",
            "timestampSpec": {
              "column": "<FILL IN TIME DIM>",
              "format": "iso"
            },
            "dimensionsSpec" : {
                "dimensionExclusion" : [
                ]
            }
          }
        },
        "granularitySpec": {
          "type": "uniform",
          "segmentGranularity": "MONTH",
          "rollup": True,
          "queryGranularity": "none"
        },
        "metricsSpec": []
      },
      "ioConfig": {
        "type": "index",
        "firehose": {
          "fetchTimeout": 300000,
          "type": "http",
          "uris": [
            "<FILL IN URL>"
          ]
        },
        "appendToExisting": True
      },
      "tuningConfig": {
        "type": "index",
        "forceExtendableShardSpecs": True,
        "maxRowsInMemory": 75000,
        "reportParseExceptions": True
      }
    }
  }

def get_env(var):
    envvar = None
    if var.upper() in os.environ:
        envvar = os.environ[var.upper()]
    return envvar


druid_server = get_env("druid_server") or "druid:8082"
druid_indexer = get_env("druid_indexer") or "druid:8081"
druid_sink = get_env("druid_sink") or "http://druid-sink:5000"

logger = None

bulk_expose_data = {}

data_location = ""

def modification_date(filename):
    t = os.path.getmtime(filename)
    return datetime.datetime.fromtimestamp(t)

def datetime_format(dt):
    return '%04d' % dt.year + dt.strftime("-%m-%dT%H:%M:%SZ")

def clean_folder(mypath):
    for root, dirs, files in os.walk(mypath):
        for file in files:
            if modification_date(os.path.join(root, file)) > datetime.datetime.now() - datetime.timedelta(days=1):
                os.remove(os.path.join(root, file))

def to_transit_datetime(dt_int):
    return "~t" + datetime_format(dt_int)

def get_var(var):
    envvar = None
    if var.upper() in os.environ:
        envvar = os.environ[var.upper()]
    elif request:
        envvar = request.args.get(var)
    logger.info("Setting %s = %s" % (var, envvar))
    if isinstance(envvar, str):
        if envvar.lower() == "true":
            envvar = True
        elif envvar.lower() == "false":
            envvar = False

    return envvar

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
        'Could not verify your access level for that URL.\n'
        'You have to login with proper credentials', 401,
        {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth:
            return authenticate()
        return f(*args, **kwargs)

    return decorated


@app.route('/', methods=['GET'])
def bulk_read():
    datatype = get_var("datatype")
    app.logger.info("Delivering data to Druid: %s" % (datatype))
    if datatype not in bulk_expose_data:
        app.logger.info("Cant find: %s" % (datatype))
        app.logger.info("wee have the following data: %s" % (bulk_expose_data))
        abort(404)

    def get_data():
        data = bulk_expose_data[datatype]
        if data_location != "RAM":
            if os.path.isfile(data):
                with open(data, "r") as f:
                    for line in f:
                        yield line
        else:
            for line in data:
                yield line
                
    return Response(stream_with_context(get_data()), mimetype='text/plain')


@app.route('/<datatype>', methods=['POST'])

def receiver(datatype):
    # get entities from request and write each of them to a file
    #data = request.get_data(True,True).replace("false","0").replace("true","1")
    global data_location
    app.logger.info("Got request with args: %s" % (request.args))
    time_dim = get_var("timestamp") or "_ts"
    data_location = get_var("datastore") or "/data/"
    is_full = get_var("is_full") or False
    is_first = get_var("is_first") or False
    kill = get_var("kill") or False
    write_data = get_var("write_data") or True
    is_last = get_var("is_last") or False
    sequence_id = get_var("sequence_id") or False
    aggregate = get_var("aggregate") or "none"
    segment = get_var("segment") or "YEAR"

    if not data_location.endswith("/"):
        data_location += "/"
    data_location += "druid_tempfiles/"
    entities = request.get_json()
    app.logger.info("Updating entity of type %s" % (datatype))
    #app.logger.debug(json.dumps(entities))
    #auth = request.authorization
    #token, username = auth.username.split('\\', 1)
    segments = []
    intervals = []

    if kill and is_first and is_full:
        r = requests.delete('http://%s/druid/coordinator/v1/datasources/%s' % (druid_indexer, datatype))
        if r.status_code < 400:
            app.logger.info("Disable datatype %s in Druid. Response: %s - %s" % (datatype, r.status_code, r.reason))
            r = requests.get('http://%s/druid/coordinator/v1/datasources/%s/segments' % (druid_indexer, datatype))
            if r.status_code == 200:
                app.logger.info("Got segments for datatype %s in Druid. Response:  %s - %s" % (datatype, r.status_code, r.reason))
                result = r.json()
                for i in result:
                    r = requests.delete('http://%s/druid/coordinator/v1/datasources/%s/segments/%s' % (druid_indexer, datatype, i.replace("/","_")))
                    app.logger.info("Disable segment %s in Druid. Response: %s" % (i, r.content))
            r = requests.get('http://%s/druid/coordinator/v1/datasources/%s/intervals' % (druid_indexer, datatype))
            if r.status_code == 200:
                app.logger.info("Got intervals for datatype %s in Druid. Response:  %s - %s" % (datatype, r.status_code, r.reason))
                result = r.json()
                for i in result:
                    r = requests.delete('http://%s/druid/coordinator/v1/datasources/%s/intervals/%s' % (druid_indexer, datatype, i.replace("/","_")))
                    app.logger.info("Disable interval %s in Druid. Response: %s" % (i, r.content))
                while  len(result) > 0:
                    result = []
                    sleep(30)
                    r = requests.get(
                        'http://%s/druid/coordinator/v1/datasources/%s/segments' % (druid_indexer, datatype))
                    if r.status_code == 200:
                        result = r.json()
                        app.logger.info("Waiting for deletion of all segments for datatype %s in Druid. Current number of segments:  %s" % (
                             datatype, len(result)))

        else:
            app.logger.info("Datatype %s does not exist in Druid. Response: %s - %s" % (datatype, r.status_code, r.reason))
    elif not is_full:
        r = requests.get('http://%s/druid/coordinator/v1/datasources/%s/segments?full' % (druid_indexer, datatype))
        if r.status_code == 200:
            app.logger.info(
                "Got segments for datatype %s in Druid. Response:  %s - %s" % (datatype, r.status_code, r.reason))
            result = r.json()
            segments = result
        r = requests.get('http://%s/druid/coordinator/v1/datasources/%s/intervals' % (druid_indexer, datatype))
        if r.status_code == 200:
            app.logger.info("Got intervals for datatype %s in Druid. Response:  %s - %s" % (datatype, r.status_code, r.reason))
            result = r.json()
            intervals = result

    return Response(transform(datatype, entities, time_dim, is_full, is_first, is_last, sequence_id, aggregate, segment, segments, intervals, write_data), mimetype='text/plain')

def transform(datatype, entities, time_dim, is_full, is_first, is_last, sequence_id, aggregate, segment, segments, intervals, write_data):
    global ids
    global data_location




    datatype_name = "%s-%s" % (datatype, sequence_id)
    file_name = data_location + datatype_name + "-float_sum"
    os.makedirs(data_location, exist_ok=True)
    my_file = Path(file_name)

    float_sum = []
    if my_file.is_file():
        with open(file_name, "r") as f:
            for line in f:
                float_sum.append(line.strip())

    c = None
    listing = []
    if not isinstance(entities, (list)):
        listing.append(entities)
    else:
        listing = entities


    bulk_data = []
    stream_data = []
    c = []
    ct = {}
    #app.logger.info("Updating schema from type %s" % (datatype))

    for ent in listing:
        if "_deleted" in ent and ent["_deleted"] is True:
            continue
        new = {k: v for k, v in ent.items() if type(v) is not dict}

        if "_ts" in new:
            new["_ts"] = datetime_format(datetime.datetime.fromtimestamp(new["_ts"] // 1000000))

        if time_dim in new:
            if new[time_dim]:
                new[time_dim] = new[time_dim].replace("~t","")
            elif "_ts" in new:
                new[time_dim] = new["_ts"]
        elif "_ts" in new:
            new[time_dim] = new["_ts"]

        if "$ids" not in new or new["$ids"] == "":
            new["$ids"] = "~:%s" % new["_id"]

        for k,v in new.items():
            if type(v) is str:
                if v.startswith("~f"):
                    if k not in float_sum:
                        float_sum.append(k)
                    new[k] = float(v.replace("~f",""))
                if v.startswith("~t"):
                    new[k] = v.replace("~t", "")
            elif type(v) is int:
                if k not in float_sum:
                    float_sum.append(k)

        bulk_data.append(new)

    with open(file_name, 'w') as f:
        for item in float_sum:
            f.write("%s\n" % item)

    #store_db(clean, datatype)
    return store_file(bulk_data, datatype,time_dim,float_sum, is_full, is_first, is_last, sequence_id, aggregate, segment, segments, intervals, write_data)


def store_db(data, datatype):
    r = requests.post('http://%s/v1/post/%s' % (druid_server,datatype), json=data)
    app.logger.info("Sucsessfully posted data to Druid: %s" % (r.content))
    return r.content

def store_file(data, datatype, time_dim, float_sum, is_full, is_first, is_last, sequence_id, aggregate, segment, segments, intervals, write_data):
    global bulk_expose_data
    global data_location
    datatype_name = "%s-%s" % (datatype, sequence_id)
    if is_first:
        filemode = "w"
    else:
        filemode = "a"

    if data_location != "RAM":
        with open(data_location + datatype_name, filemode) as f:
            for line in data:
                json.dump(line, f, ensure_ascii=True)
                f.write("\n")
            bulk_expose_data[datatype_name] = data_location + datatype_name
    else:
        with io.StringIO() as f:
            for line in data:
                json.dump(line, f, ensure_ascii=True)
                f.write("\n")
            bulk_expose_data[datatype_name] = f.getvalue()



    if (not is_full or is_last) and write_data:
        index_task = copy.deepcopy(index_template)
        index_task["spec"]["dataSchema"]["dataSource"] = datatype
        index_task["spec"]["dataSchema"]["parser"]["parseSpec"]["timestampSpec"]["column"] = time_dim
        index_task["spec"]["dataSchema"]["granularitySpec"]["queryGranularity"] = aggregate
        index_task["spec"]["dataSchema"]["granularitySpec"]["segmentGranularity"] = segment
        index_task["spec"]["ioConfig"]["firehose"]["uris"]= ["%s?datatype=%s" % (druid_sink, datatype_name)]
        app.logger.info("Setting up firehose to read from: %s" % (json.dumps(index_task["spec"]["ioConfig"]["firehose"]["uris"])))
        for k in float_sum:
            index_task["spec"]["dataSchema"]["metricsSpec"].append({ "type":"doubleSum", "name": k, "fieldName": k })
        if is_full:
            index_task["spec"]["ioConfig"]["appendToExisting"] = False
        elif is_full:
            firehouse = {
                "type"  :   "combining",
                "delegates" : []
            }

            ingest = {
                            "type"    : "ingestSegment",
                            "dataSource"   : datatype,
                            "interval" : "%s/%s" % (intervals[-1].split("/")[0],intervals[0].split("/")[-1])
            }
            firehouse["delegates"].append(ingest)

            firehouse["delegates"].append(index_task["spec"]["ioConfig"]["firehose"])
            index_task["spec"]["ioConfig"]["firehose"] = firehouse


        app.logger.info("Task to send to to Druid: %s" % (json.dumps(index_task)))
        r = requests.post('http://%s/druid/indexer/v1/task' %(druid_indexer), json=index_task)
        result = r.json()
        if "task" in result:
            app.logger.info("Success in getting Task for request sent to Druid. Responce: %s" % (r.content))
        else:
            app.logger.error("Problem with request sent to Druid. Responce: %s" % (r.content))
            abort()

    return datatype_name

if __name__ == '__main__':
    # Set up logging
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('druid-microservice')

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.setLevel(logging.DEBUG)

    data_location = get_var("datastore") or "/data/"
    if data_location != "RAM":
        if not data_location.endswith("/"):
            data_location += "/"
        data_location += "druid_tempfiles/"
        clean_folder(data_location)

    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.socket_port': 5000,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
