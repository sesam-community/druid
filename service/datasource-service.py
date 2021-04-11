from functools import wraps

import copy
from time import sleep

import cherrypy
import logging
import paste.translogger
from flask import Flask, request, Response, abort, stream_with_context
import datetime
import requests
import io
import os, os.path

import json
import logging
from pathlib import Path

app = Flask(__name__)

logger = logging.getLogger('druid-microservice')

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
                "dimensions":[],
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
          "fetchTimeout": 21700000,
          "maxFetchRetry": 1,
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

bulk_expose_data = {}
bulk_expose_schema = {}
bulk_expose_schema = {}

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
    logger.debug("Setting %s = %s" % (var, envvar))
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
    global data_location
    datatype = get_var("datatype")

    if data_location != "RAM":
        data = data_location + datatype

        def get_data():
            with open(data, "r") as f:
                for line in f:
                    logger.debug(line)
                    yield line


        if os.path.isfile(data):
            logger.info("Delivering %s data from DISK: %s" % (datatype, data))
            return Response(stream_with_context(get_data()), mimetype='text/plain')

        else:
            NoData(datatype)

    else:
        if datatype not in bulk_expose_data:
            NoData(datatype)
        data = bulk_expose_data[datatype]
        logger.info("Delivering %s from RAM: %s bytes of type %s" % (datatype, len(data), type(data)))
        logger.debug(data)
        return Response(data, content_type="text/plain; charset=utf-8")


def NoData(datatype):
    app.logger.info("Cant find: %s" % (datatype))
    app.logger.info("wee have the following data: %s" % (bulk_expose_data))
    abort(404)


@app.route('/<datatype>', methods=['POST'])

def receiver(datatype):
    # get entities from request and write each of them to a file
    #data = request.get_data(True,True).replace("false","0").replace("true","1")
    global data_location
    logger.info("Got request with args: %s" % (request.args))
    time_dim = get_var("timestamp") or "_ts"
    data_location = get_var("datastore") or "/data/"
    if data_location != "RAM":
        if not data_location.endswith("/"):
            data_location += "/"
        data_location += "druid_tempfiles/"

    is_full = get_var("is_full") or False
    append = get_var("append") or False
    is_first = get_var("is_first") or False
    kill = get_var("kill") or False
    write_data = get_var("write_data") or True
    is_last = get_var("is_last") or False
    sequence_id = get_var("sequence_id") or False
    aggregate = get_var("aggregate") or "none"
    segment = get_var("segment") or "YEAR"
    loglevel = get_var("loglevel") or "INFO"

    if loglevel == "DEBUG":
        logger.setLevel(logging.DEBUG)
    if loglevel == "INFO":
        logger.setLevel(logging.INFO)
    if loglevel == "WARN":
        logger.setLevel(logging.WARNING)

    entities = request.get_json()
    app.logger.debug("Updating entity of type %s" % (datatype))
    #app.logger.debug(json.dumps(entities))
    #auth = request.authorization
    #token, username = auth.username.split('\\', 1)
    segments = []
    intervals = []

    if kill and is_first and is_full:
        r = requests.delete('http://%s/druid/coordinator/v1/datasources/%s' % (druid_indexer, datatype))
        if r.status_code < 400:
            logger.info("Disable datatype %s in Druid. Response: %s - %s" % (datatype, r.status_code, r.reason))
            r = requests.get('http://%s/druid/coordinator/v1/datasources/%s/segments' % (druid_indexer, datatype))
            if r.status_code == 200:
                logger.info("Got segments for datatype %s in Druid. Response:  %s - %s" % (datatype, r.status_code, r.reason))
                result = r.json()
                for i in result:
                    r = requests.delete('http://%s/druid/coordinator/v1/datasources/%s/segments/%s' % (druid_indexer, datatype, i.replace("/","_")))
                    logger.info("Disable segment %s in Druid. Response: %s" % (i, r.content))
            r = requests.get('http://%s/druid/coordinator/v1/datasources/%s/intervals' % (druid_indexer, datatype))
            if r.status_code == 200:
                logger.info("Got intervals for datatype %s in Druid. Response:  %s - %s" % (datatype, r.status_code, r.reason))
                result = r.json()
                for i in result:
                    r = requests.delete('http://%s/druid/coordinator/v1/datasources/%s/intervals/%s' % (druid_indexer, datatype, i.replace("/","_")))
                    logger.info("Disable interval %s in Druid. Response: %s" % (i, r.content))
                while  len(result) > 0:
                    result = []
                    sleep(30)
                    r = requests.get(
                        'http://%s/druid/coordinator/v1/datasources/%s/segments' % (druid_indexer, datatype))
                    if r.status_code == 200:
                        result = r.json()
                        logger.info("Waiting for deletion of all segments for datatype %s in Druid. Current number of segments:  %s" % (
                             datatype, len(result)))

        else:
            logger.info("Datatype %s does not exist in Druid. Response: %s - %s" % (datatype, r.status_code, r.reason))
    elif not is_full:
        r = requests.get('http://%s/druid/coordinator/v1/datasources/%s/segments?full' % (druid_indexer, datatype))
        if r.status_code == 200:
            logger.info(
                "Got segments for datatype %s in Druid. Response:  %s - %s" % (datatype, r.status_code, r.reason))
            result = r.json()
            segments = result
        r = requests.get('http://%s/druid/coordinator/v1/datasources/%s/intervals' % (druid_indexer, datatype))
        if r.status_code == 200:
            logger.info("Got intervals for datatype %s in Druid. Response:  %s - %s" % (datatype, r.status_code, r.reason))
            result = r.json()
            intervals = result

    return Response(transform(datatype, entities, time_dim, is_full, append, is_first, is_last, sequence_id, aggregate, segment, segments, intervals, write_data), mimetype='text/plain')

def transform(datatype, entities, time_dim, is_full, append, is_first, is_last, sequence_id, aggregate, segment, segments, intervals, write_data):
    global ids
    global data_location




    datatype_name = "%s-%s" % (datatype, sequence_id)
    file_name = data_location + datatype_name + "-float_sum"
    file_name_str = data_location + datatype_name + "-string"
    os.makedirs(data_location, exist_ok=True)
    my_file = Path(file_name)

    unhandeled_prop = set([])

    float_sum = set([])
    if not is_first and my_file.is_file():
        with open(file_name, "r") as f:
            for line in f:
                float_sum.add(line.strip())

    my_file = Path(file_name_str)

    str_field = set([])
    if not is_first and my_file.is_file():
        with open(file_name_str, "r") as f:
            for line in f:
                str_field.add(line.strip())

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
    #logger.info("Updating schema from type %s" % (datatype))

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

        for k,value in new.items():
            try:
                if type(value) is not list:
                    value = [value]
                nv = []
                for v in value:
                    if type(v) is str:
                        if v.startswith("~f"):
                            float_sum.add(k)
                            nv.append(float(v.replace("~f","")))
                        elif v.startswith("~t"):
                            str_field.add(k)
                            nv.append(v.replace("~t", ""))
                        else:
                            str_field.add(k)
                            nv.append(v)

                    elif type(v) is int or type(v) is float:
                        float_sum.add(k)
                        nv.append(float(v))
                    elif type(v) is bool:
                        str_field.add(k)
                        nv.append(str(v).lower())
                    elif type(v) is dict:
                        str_field.add(k)
                        nv.append(json.dumps(v))
                    elif type(v) is list:
                        str_field.add(k)
                        nv.append(json.dumps(v))
                    elif v is not None:
                        if k not in unhandeled_prop:
                            unhandeled_prop.add(k)
                            logger.warning("UNHANDLED data type in %s: %s" % (k, type(v)))


                if len(nv) == 1:
                    new[k] = nv[0]
                elif len(nv) > 1:
                    new[k] = nv
                    if type(nv[0]) is int or type(nv[0]) is float:
                        logger.info("MULTIVALUE in field %s: %s" % (k, str(nv)))
                    if k in float_sum and k not in str_field:
                        logger.warning("MULTIVALUE in number field %s. Suming numbers: %s" % (k, str(nv)))
                        new[k] = sum(nv)



            except Exception as e:
                logger.error("ERROR '%s' for %s: %s" % (str(e), k, type(v)))


        bulk_data.append(new)
        logger.debug(json.dumps(new))

    if is_first:
        filemode = "w"
    else:
        filemode = "a"

    with open(file_name, filemode) as f:
        for item in float_sum:
            f.write("%s\n" % item)
    with open(file_name_str, filemode) as f:
        for item in str_field:
            f.write("%s\n" % item)

    #store_db(clean, datatype)
    return store_file(bulk_data, datatype,time_dim, str_field, float_sum, is_full, append, is_first, is_last, sequence_id, aggregate, segment, segments, intervals, write_data)


def store_db(data, datatype):
    r = requests.post('http://%s/v1/post/%s' % (druid_server,datatype), json=data)
    logger.info("Sucsessfully posted data to Druid: %s" % (r.content))
    return r.content

def store_file(data, datatype, time_dim, str_field, float_sum, is_full, append, is_first, is_last, sequence_id, aggregate, segment, segments, intervals, write_data):
    global bulk_expose_data, bulk_expose_schema
    global data_location
    datatype_name = "%s-%s" % (datatype, sequence_id)
    datatype_name = datatype

    if data_location != "RAM":
        if is_first:
            filemode = "w"
        else:
            filemode = "a"
        with open(data_location + datatype_name, filemode) as f:
            for line in data:
                json.dump(line, f, ensure_ascii=True)
                f.write("\n")
            bulk_expose_data[datatype_name] = data_location + datatype_name
            logger.info("Stored data in FILE %s: %s lines" % (bulk_expose_data[datatype_name], len(data)))
    else:
        with io.StringIO() as f:
            datatype_name = datatype
            for line in data:
                json.dump(line, f, ensure_ascii=True)
                f.write("\n")
            if is_first:
                bulk_expose_data[datatype_name] = f.getvalue()
            else:
                bulk_expose_data[datatype_name] = bulk_expose_data[datatype_name] + f.getvalue()
            logger.info("Got data in RAM: %s bytes" % (len(bulk_expose_data[datatype_name])))
            logger.debug(bulk_expose_data[datatype_name])



    if (not is_full or is_last) and write_data:
        index_task = copy.deepcopy(index_template)
        index_task["spec"]["dataSchema"]["dataSource"] = datatype
        index_task["spec"]["dataSchema"]["parser"]["parseSpec"]["timestampSpec"]["column"] = time_dim
        index_task["spec"]["dataSchema"]["granularitySpec"]["queryGranularity"] = aggregate
        index_task["spec"]["dataSchema"]["granularitySpec"]["segmentGranularity"] = segment
        index_task["spec"]["ioConfig"]["firehose"]["uris"]= ["%s?datatype=%s" % (druid_sink, datatype_name)]
        app.logger.info("Setting up firehose to read from: %s" % (json.dumps(index_task["spec"]["ioConfig"]["firehose"]["uris"])))
        for k in str_field:
            #if k not in float_sum:
            index_task["spec"]["dataSchema"]["parser"]["parseSpec"]["dimensionsSpec"]["dimensions"].append(k)
        for k in float_sum:
            # index_task["spec"]["dataSchema"]["metricsSpec"].append({ "type":"doubleSum", "name": k, "fieldName": k })
            if k not in str_field:
                index_task["spec"]["dataSchema"]["parser"]["parseSpec"]["dimensionsSpec"]["dimensions"].append({ "type":"float", "name": k })
                index_task["spec"]["dataSchema"]["metricsSpec"].append({"type": "doubleSum", "name": "sum-" + k, "fieldName": k})
        if is_full or not append:
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

        r = requests.get(
            'http://%s/druid/indexer/v1/tasks' % (druid_indexer))
        result = r.json()
        app.logger.info("Tasks running: %s" % (json.dumps(result)))


        app.logger.info("Task to send to to Druid: %s" % (json.dumps(index_task)))
        app.logger.debug('Post to http://%s/druid/indexer/v1/task' %(druid_indexer))
        r = requests.post('http://%s/druid/indexer/v1/task' %(druid_indexer), json=index_task)
        app.logger.debug("Responce from Druid: %s" % (str(r)))
        result = r.json()
        if "task" in result:
            app.logger.info("Success in getting Task for request sent to Druid. Responce: %s" % (result))

            task = result["task"]

            while True:
                r = requests.get(
                    'http://%s/druid/indexer/v1/task/%s/status' % (druid_indexer, task))
                if r.status_code == 200:
                    result = r.json()
                    app.logger.info(
                        "Waiting for task completion for %s:  %s" % (
                            datatype, result["status"]["status"]))
                    sleep(30)
                    r = requests.get(
                        'http://%s/druid/indexer/v1/task/%s/reports' % (druid_indexer, task))
                    if r.status_code == 200:
                        result = r.json()
                        app.logger.info(
                            "Waiting for task completion for %s:  %s" % (
                                datatype, result))
                        if "errorMsg" in result["ingestionStatsAndErrors"]["payload"] and result["ingestionStatsAndErrors"]["payload"]["errorMsg"] is not None:
                            app.logger.error("Problem executing task in Druid. Error: %s" % (result["ingestionStatsAndErrors"]["payload"]["errorMsg"]))
                            abort(500, "Problem executing task in Druid. Error: %s" % (result["ingestionStatsAndErrors"]["payload"]["errorMsg"]))

                        if result["ingestionStatsAndErrors"]["payload"]["ingestionState"] == "COMPLETED":
                            break

                else:
                    app.logger.error("Problem geting task status from Druid. Responce: %s" % (r.content))

                    abort(500, r.content)

        else:
            app.logger.error("Problem with request sent to Druid. Responce: %s" % (r.content))
            abort(500, "Problem with request sent to Druid. Responce: %s" % (r.content))


    return datatype_name

if __name__ == '__main__':
    # Set up logging

    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Log to stdout, change to or add a (Rotating)FileHandler to log to a file
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.setLevel(logging.INFO)

    data_location = get_var("datastore") or "/data/"
    if data_location != "RAM":
        if not data_location.endswith("/"):
            data_location += "/"
        data_location += "druid_tempfiles/"


    logger.propagate = False
    logger.setLevel(logging.INFO)

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
