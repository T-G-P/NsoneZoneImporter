from nsone import NSONE, Config
from nsone.rest.errors import AuthException, ResourceException
from twisted.internet import defer, reactor
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


def transformCsv(reader):
    """
    Transforms the csv to a more easily processed dict to minimize
    rest api calls for creating and loading zones unecessarily.
    This is implemented since it is overkill to try to create or load
    the zones for each row using the api

    NOTE: Assumes Name,Zone,Type,TTL,Data as the header
    """

    data = {}
    for row in reader:
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


def transformJson(jsonDict):
    """Not Implemented. Assuming json transformed similar to csv"""
    return jsonDict


def loadZoneData(filename):
    extension = os.path.splitext(filename)[1]
    with open(filename, 'rb') as f:
        if extension == '.csv':
            data = transformCsv(csv.DictReader(f))
        elif extension == '.json':
            data = transformJson(json.loads(f))
    return data


def deleteZoneData(nsoneObj, data):
    for key in data.keys():
        deleteZone = deleteZone(zoneKey, nsoneObj)
        deleteZone.addCallback(deleteZoneSuccess)
        deleteZone.addCallback(deleteZoneFailure)


@defer.inlineCallbacks
def deleteZone(zoneKey, nsoneObj):
    zone = yield nsoneObj.loadZone(key)
    yield zone.delete()


def deleteZoneSuccess(response):
    print response


def deleteZoneFailure(failure):
    print failure


def importZoneData(data, nsoneObj):
    # add auth exception error back
    # add existing zone callback
    for zoneKey, records in data.iteritems():
        # could fail, on auth or existing zone
        zone = createZone(zoneKey, nsoneObj)
        zone.addCallback(createZoneSucceeded)
        zone.addErrBack(createZoneFailed)

        for rec in records:
            answers = rec['Data'].split()
            methodName = 'add_{}'.format(rec['Type'])
            addMethod = getZoneAddMethod(zone, methodName)
            addMethod.addCallback(getZoneAddMethodSucceeded)
            addMethod.addErrback(getZoneAddMethodFailure)

            # record could already exist, add callback chain here
            # need to load record if record exists
            record = createRecord(addMethod, zoneKey, [answers], rec['TTL'])
            record.addCallback(createRecordSuccess)
            record.addErrBack(createRecordFailure, [answers])


@defer.inlineCallbacks
def createZone(zoneKey, nsoneObj):
    yield nsoneObj.createZone(zoneKey)


def createZoneSucceeded(response):
    print response


def createZoneFailed(failure, zoneKey, nsoneObj):
    if isinstance(failure, AuthException):
        print failure
        reactor.stop()
    elif isinstance(failure, ResourceException):
        yield nsoneObj.loadZone(zoneKey)


@defer.inlineCallbacks
def getZoneAddMethod(zone, methodName):
    yield getattr(zone, methodName)


def getAddZoneMethodSuccess(response):
    print response


def getAddZoneMethodFailure(failure):
    print failure
    reactor.stop()


@defer.inlineCallbacks
def createRecord(addMethod, zoneKey, answers, ttl):
    yield addMethod(zoneKey, [answers], ttl=ttl)


def createRecordSuccess(response):
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
    print failure
    reactor.stop()


def main():
    args = getArgs()
    data = loadZoneData(args.filename)

    config = Config()
    config.createFromAPIKey(args.api_key)
    config['transport'] = 'twisted'

    nsoneObj = NSONE(config=config)

    if args.delete:
        deleteZoneData(data, nsoneObj)


if __name__ == '__main__':
    main()
