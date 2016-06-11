from nsone import NSONE, Config
from nsone.rest.errors import AuthException, ResourceException
from twisted.internet import defer, reactor, threads
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
    """Lets csv data be evaluated lazily."""
    for row in reader:
        yield row


def readDataDict(dataDict):
    """Lets dictionary data be evaluated lazily."""
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
    for zoneKey, _ in data:
        deleteZoneRes = deleteZone(zoneKey, nsoneObj)
        deleteZoneRes.addCallback(deleteZoneSuccess, zoneKey)
        deleteZoneRes.addErrback(deleteZoneFailure, zoneKey)


@defer.inlineCallbacks
def deleteZone(zoneKey, nsoneObj):
    zone = yield nsoneObj.loadZone(zoneKey)
    yield zone.delete()


def deleteZoneSuccess(response, zone):
    print 'Successfully Deleted Zone: {}'.format(zone)


def deleteZoneFailure(failure, zone):
    print '{}: {}'.format(zone, failure.getErrorMessage())


def importZoneData(data, nsoneObj):
    for zoneKey, records in data:

        # could fail, on auth or existing zone
        zone = createZone(zoneKey, nsoneObj)
        zone.addCallback(createZoneSuccess, zoneKey)
        zone.addErrback(createZoneFailed, zoneKey, nsoneObj)

        for rec in records:
            answers = rec['Data'].split()

            methodName = 'add_{}'.format(rec['Type'])

            addMethod = threads.deferToThread(getZoneAddMethod, zone, methodName)
            addMethod.addCallback(getZoneAddMethodSuccess, methodName, zoneKey)
            addMethod.addErrback(getZoneAddMethodFailure, methodName, zoneKey)

            record = createRecord(addMethod, zoneKey, [answers], rec['TTL'])
            record.addCallback(createRecordSuccess)
            record.addErrback(createRecordFailure, zoneKey, rec['Type'], [answers], nsoneObj)

    # reactor.stop()


@defer.inlineCallbacks
def createZone(zoneKey, nsoneObj):
    yield nsoneObj.createZone(zoneKey)


def createZoneSuccess(response, zoneKey):
    print 'Successfully Created Zone: {}'.format(zoneKey)


def createZoneFailed(failure, zoneKey, nsoneObj):
    if isinstance(failure, AuthException):
        print failure.getErrorMessage()
        # reactor.stop()
    elif isinstance(failure, ResourceException):
        yield nsoneObj.loadZone(zoneKey)


# @defer.inlineCallbacks
def getZoneAddMethod(zone, methodName):
    z = yield zone
    defer.returnValue(getattr(z, methodName))


def getZoneAddMethodSuccess(response, methodName, zoneKey):
    print 'Successfully got {} method for Zone: {}'.format(methodName, zoneKey)


def getZoneAddMethodFailure(failure, methodName, zoneKey):
    print failure.getErrorMessage()


@defer.inlineCallbacks
def createRecord(addMethod, zoneKey, answers, ttl):
    am = yield addMethod
    yield am(zoneKey, answers, ttl=ttl)


def createRecordSuccess(response):
    print 'Successfully created a record'
    print response


def createRecordFailure(failure, zoneKey, recType, answers, nsoneObj):
    record = yield nsoneObj.loadRecord(zoneKey, recType, zoneKey)
    addAnswers = addRecordAnswers(record, answers)
    addAnswers.addCallback(addRecordAnswersSuccess)
    addAnswers.addErrback(addRecordAnswersFailure)


@defer.inlineCallbacks
def addRecordAnswers(record, answers):
    yield record.addAnswers(answers)


def addRecordAnswersSuccess(response):
    print response


def addRecordAnswersFailure(failure):
    print failure.getErrorMessage()
    # reactor.stop()


def main():
    args = getArgs()
    data = loadZoneData(args.filename)

    config = Config()
    config.createFromAPIKey(args.api_key)
    config['transport'] = 'twisted'

    nsoneObj = NSONE(config=config)

    if args.delete:
        deleteZoneData(data, nsoneObj)

    else:
        importZoneData(data, nsoneObj)

    reactor.run()


if __name__ == '__main__':
    main()
