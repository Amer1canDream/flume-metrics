import requests
import json
import os
import graphyte
import time

cloudera_api_host = 'http://localhost'
cloudera_api_port = '7180'
cloudera_api_version = 'v16'
cloudera_api_url = cloudera_api_host + ':' + cloudera_api_port + '/api/' + cloudera_api_version + '/hosts/'

user = os.environ['user']
passwd = os.environ['passwd']
auth_values = (user, passwd)

channelMetrics = ['ChannelFillPercentage']
sinkMetrics = ['ConnectionFailedCount', 'EventDrainSuccessCount', 'EventDrainAttemptCount']
sourceMetrics = ['AppendBatchAcceptedCount', 'AppendBatchReceivedCount', 'EventAcceptedCount', 'EventReceivedCount', 'AppendAcceptedCount', 'AppendReceivedCount']

graphitePrefix = os.environ['graphitePrefix']
graphiteHost = os.environ['graphiteHost']
graphitePort = os.environ['graphitePort']

def getWorkersList():
    response = requests.get(cloudera_api_url, auth=auth_values)
    workers = []
    data = json.loads(response.text)
    for item in data['items']:
        if 'worker' in item['hostname']:
            workers.append(item['hostname'])
        else:
            pass
    return(workers)

def sendChannel(metricPrefix, channel):
    for i in channelMetrics:
        fullPrefix = metricPrefix + '.' + i
        graphyte.send(fullPrefix, float(channel[i]))

def sendSink(metricPrefix, sink):
    for i in sinkMetrics:
        fullPrefix = metricPrefix + '.' + i
        graphyte.send(fullPrefix, float(sink[i]))

def sendSource(metricPrefix, source):
    for i in sourceMetrics:
        fullPrefix = metricPrefix + '.' + i
        graphyte.send(fullPrefix, float(source[i]))

def parseMetrics(response, host, data):
    graphyte.init(graphiteHost, port=graphitePort, prefix=graphitePrefix)
    allMetrics = json.loads(response.text)

    worker = host.replace('.', '_')

    for metrics in allMetrics:

        record = {"{#WORKER}": host}

        if 'CHANNEL' in metrics:
            metricPrefix = worker + '.' + metrics
            sendChannel(metricPrefix, channel=allMetrics[metrics])
            record['{#CHANNEL}'] = metrics
        elif 'SINK' in metrics:
            metricPrefix = worker + '.' + metrics
            sendSink(metricPrefix, sink=allMetrics[metrics])
            record['{#SINK}'] = metrics
        elif 'SOURCE' in metrics:
            metricPrefix = worker + '.' + metrics
            sendSource(metricPrefix, source=allMetrics[metrics])
            record['{#SOURCE}'] = metrics

        data.append(record)

def takeJsons():
    workersList = getWorkersList()

    data = []

    for worker in workersList:
        response = requests.get('http://' + worker + ':' + '41414' + '/metrics')
        parseMetrics(response, host=worker, data=data)

    finalJson = {"data": data}

    with open('data.json', 'w') as outfile:
        outfile.write(json.dumps(finalJson))

    outfile.close()

while True:
    time.sleep(6)
    try:
        takeJsons()
    except:
        print('Error')
