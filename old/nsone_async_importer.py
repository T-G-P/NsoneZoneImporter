from nsone import NSONE, Config
from nsone.rest.errors import AuthException, ResourceException
from twisted.internet import defer, reactor, task
import os.path
import csv
import argparse


def isValidFile(parser, arg):
    arg = os.path.abspath(arg)
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return arg


def getArgs():
    parser = argparse.ArgumentParser(description='Import some Zone Data to NS1')
    parser.add_argument("-a", "--api_key",
                        dest="api_key",
                        type=str,
                        required=True,
                        metavar="API_KEY",
                        help="Your NS1 api key with this flag")
    parser.add_argument("-f", "--file",
                        dest="filename",
                        type=lambda x: isValidFile(parser, x),
                        required=True,
                        metavar="FILE",
                        help="Import Zone data from file with this flag")
    parser.add_argument("-d", "--delete",
                        dest="delete",
                        action='store_true',
                        help="Delete Zone data from file with this flag")
    args = parser.parse_args()
    return args


def readCsv(reader):
    """Lets csv data be evaluated lazily. Since file might be huge"""
    for row in reader:
        yield row


def readDataDict(dataDict):
    """Lets dictionary data be evaluated lazily. Since the file might be huge"""
    for k, v in dataDict.iteritems():
        yield k, v


def transformCsv(csvData):
    """
    Transforms the csv to a more easily processed dict to minimize
    rest api calls for creating and loading zones unecessarily.
    This is implemented since it is overkill to try to create or load
    the zones for each row using the api

    NOTE: Assumes Name,Zone,Type,TTL,Data as the header
    """

    data = {}
    for row in csvData:
        record = {
            'Data': row['Data'],
            'Type': row['Type'],
            'Name': row['Name'],
            'TTL': row['TTL']
        }
        if data.get(row['Zone']):
            data[row['Zone']].append(record)
        else:
            data[row['Zone']] = [record]
    return data


def transformJson(jsonData):
    """Not Implemented. Assuming json transformed similar to csv"""
    return jsonData


def loadZoneData(filename):
    extension = os.path.splitext(filename)[1]
    with open(filename, 'rb') as f:
        if extension == '.csv':
            reader = csv.DictReader(f)
            dataDict = transformCsv(readCsv(reader))
            data = readDataDict(dataDict)
        elif extension == '.json':
            dataDict = json.loads(f)
            data = transformJson(readDataDict(jsonDict))

    return data


def deleteZoneData(data, nsoneObj):
    dl = []
    for zoneKey, records in data:
        deleteZoneRes = deleteZonesAndRecords(zoneKey, records, nsoneObj)
        deleteZoneRes.addCallback(deleteZoneSuccess, zoneKey)
        deleteZoneRes.addErrback(deleteZoneFailure, zoneKey)
        dl.append(deleteZoneRes)
    return defer.DeferredList(dl, fireOnOneErrback=True)



@defer.inlineCallbacks
def deleteZonesAndRecords(zoneKey, records, nsoneObj):
    zone = yield nsoneObj.loadZone(zoneKey)
    yield zone.delete()


def deleteZoneSuccess(response, zone):
    print 'Successfully Deleted Zone: {}'.format(zone)


def deleteZoneFailure(failure, zone):
    print '{}: {}'.format(zone, failure.getErrorMessage())


def importZoneData(data, nsoneObj):
    dl = []
    for zoneKey, records in data:
        zone = createZone(zoneKey, nsoneObj)
        zone.addCallback(createZoneSuccess, zoneKey, records, nsoneObj)
        zone.addErrback(createZoneFailure, zoneKey, records, nsoneObj)
        dl.append(zone)
    return defer.DeferredList(dl, fireOnOneErrback=True)



@defer.inlineCallbacks
def createZone(zoneKey, nsoneObj):
    zone = yield nsoneObj.createZone(zoneKey)
    defer.returnValue(zone)


def createZoneSuccess(response, zoneKey, records, nsoneObj):
    dl = []
    zone = response
    print 'Successfully Created Zone: {}'.format(zone)
    for rec in records:
        answers = rec['Data'].split()
        methodName = 'add_{}'.format(rec['Type'])
        addMethod = getattr(zone, methodName)

        record = createRecord(addMethod, zoneKey, [answers], rec['TTL'])
        record.addCallback(createRecordSuccess)
        record.addErrback(createRecordFailure, zoneKey, rec['Type'], answers, nsoneObj)
        dl.append(record)
    return defer.DeferredList(dl, fireOnOneErrback=True)


@defer.inlineCallbacks
def createZoneFailure(failure, zoneKey, records, nsoneObj):
    # f = failure.trap(AuthException, ResourceException)
    f = failure.trap(ResourceException)
    print failure.getErrorMessage()

    # if f == AuthException:
    #     reactor.removeAll()
    #     reactor.stop()
    # elif f == ResourceException:
    zone = loadZone(zoneKey, nsoneObj)
    zone.addCallback(loadZoneSuccess, zoneKey, records, nsoneObj)
    zone.addErrback(loadZoneFailure, zoneKey)
    yield zone


@defer.inlineCallbacks
def loadZone(zoneKey, nsoneObj):
    zone = yield nsoneObj.loadZone(zoneKey)
    defer.returnValue(zone)

def loadZoneSuccess(response, zoneKey, records, nsoneObj):
    print 'Successfully Loaded Zone: {}'.format(zoneKey)
    dl = []
    zone = response
    for rec in records:
        answers = rec['Data'].split()
        methodName = 'add_{}'.format(rec['Type'])
        addMethod = getattr(zone, methodName)

        record = createRecord(addMethod, zoneKey, [answers], rec['TTL'])
        record.addCallback(createRecordSuccess)
        record.addErrback(createRecordFailure, zoneKey, rec['Type'], answers, nsoneObj)
        dl.append(record)
    return defer.DeferredList(dl, fireOnOneErrback=True)


def loadZoneFailure(failure, zoneKey):
    print '{}: {}'.format(zoneKey, failure.getErrorMessage())


@defer.inlineCallbacks
def createRecord(addMethod, zoneKey, answers, ttl):
    record = yield addMethod(zoneKey, answers, ttl=ttl)
    defer.returnValue(record)


def createRecordSuccess(response):
    print 'Created record: {}'.format(response)


@defer.inlineCallbacks
def createRecordFailure(failure, zoneKey, recType, answers, nsoneObj):
    f = failure.trap(ResourceException)
    if f == ResourceException:
        record = loadRecord(zoneKey, recType, nsoneObj)
        record.addCallback(loadRecordSuccess, answers)
        record.addErrback(loadRecordFailure)
        yield record


@defer.inlineCallbacks
def loadRecord(zoneKey, recType, nsoneObj):
    record = yield nsoneObj.loadRecord(zoneKey, recType, zoneKey)
    defer.returnValue(record)


@defer.inlineCallbacks
def loadRecordSuccess(response, answers):
    print 'Successfully loaded Record: {}'.format(response)
    record = response
    addRecordAnswersRes = addRecordAnswers(record, answers)
    addRecordAnswersRes.addCallback(addRecordAnswersSuccess, answers)
    addRecordAnswersRes.addErrback(addRecordAnswersFailure)
    yield addRecordAnswersRes


def loadRecordFailure(failure):
    print failure.getErrorMessage()


@defer.inlineCallbacks
def addRecordAnswers(record, answers):
    recordData = yield record.data
    recordAnswers = {answer['answer'][0] for answer in recordData['answers']}
    if recordAnswers.intersection(answers[0]):
        yield record.addAnswers(answers)


def addRecordAnswersSuccess(response, answers):
    print 'Successfully processed answers: {}'.format(answers)


def addRecordAnswersFailure(failure):
    print failure.getErrorMessage()


def main(reactor):
    args = getArgs()
    data = loadZoneData(args.filename)

    config = Config()
    config.createFromAPIKey(args.api_key)
    config['transport'] = 'twisted'

    nsoneObj = NSONE(config=config)

    if args.delete:
        return deleteZoneData(data, nsoneObj)

    else:
        return importZoneData(data, nsoneObj)


if __name__ == '__main__':
    task.react(main)
