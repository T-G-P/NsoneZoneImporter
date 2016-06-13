from nsone import NSONE, Config
from nsone.rest.errors import ResourceException
from twisted.internet import defer, reactor, task
from zonedataparser import ZoneDataParser


class NsoneImporter(object):


    config = Config()


    def __init__(self, apiKey, data, delete):
        self.config.createFromAPIKey(apiKey)
        self.config['transport'] = 'twisted'
        self.nsoneObj = NSONE(config=self.config)
        self.data = data
        self.deleteData = delete


    def deleteZoneData(self):
        dl = []
        for zoneKey, records in self.data:
            deleteZoneRes = self.deleteZonesAndRecords(zoneKey, records, self.nsoneObj)
            deleteZoneRes.addCallback(self.deleteZoneSuccess, zoneKey)
            deleteZoneRes.addErrback(self.deleteZoneFailure, zoneKey)
            dl.append(deleteZoneRes)
        return defer.DeferredList(dl, fireOnOneErrback=True)


    @defer.inlineCallbacks
    def deleteZonesAndRecords(self, zoneKey, records, nsoneObj):
        zone = yield nsoneObj.loadZone(zoneKey)
        yield zone.delete()


    def deleteZoneSuccess(self, response, zone):
        print 'Successfully Deleted Zone: {}'.format(zone)


    def deleteZoneFailure(self, failure, zone):
        print '{}: {}'.format(zone, failure.getErrorMessage())


    def importZoneData(self):
        dl = []
        for zoneKey, records in self.data:
            zone = self.createZone(zoneKey)
            zone.addCallback(self.createZoneSuccess, zoneKey, records, self.nsoneObj)
            zone.addErrback(self.createZoneFailure, zoneKey, records, self.nsoneObj)
            dl.append(zone)
        return defer.DeferredList(dl, fireOnOneErrback=True)


    @defer.inlineCallbacks
    def createZone(self, zoneKey):
        zone = yield self.nsoneObj.createZone(zoneKey)
        defer.returnValue(zone)


    def createZoneSuccess(self, response, zoneKey, records, nsoneObj):
        dl = []
        zone = response
        print 'Successfully Created Zone: {}'.format(zone)
        for rec in records:
            answers = rec['Data'].split()
            methodName = 'add_{}'.format(rec['Type'])
            addMethod = getattr(zone, methodName)

            record = self.createRecord(addMethod, zoneKey, [answers], rec['TTL'])
            record.addCallback(self.createRecordSuccess)
            record.addErrback(self.createRecordFailure, zoneKey, rec['Type'], answers, nsoneObj)
            dl.append(record)
        return defer.DeferredList(dl, fireOnOneErrback=True)


    @defer.inlineCallbacks
    def createZoneFailure(self, failure, zoneKey, records, nsoneObj):
        f = failure.trap(ResourceException)
        print failure.getErrorMessage()

        zone = self.loadZone(zoneKey, nsoneObj)
        zone.addCallback(self.loadZoneSuccess, zoneKey, records, nsoneObj)
        zone.addErrback(self.loadZoneFailure, zoneKey)
        yield zone


    @defer.inlineCallbacks
    def loadZone(self, zoneKey, nsoneObj):
        zone = yield nsoneObj.loadZone(zoneKey)
        defer.returnValue(zone)


    def loadZoneSuccess(self, response, zoneKey, records, nsoneObj):
        print 'Successfully Loaded Zone: {}'.format(zoneKey)
        dl = []
        zone = response
        for rec in records:
            answers = rec['Data'].split()
            methodName = 'add_{}'.format(rec['Type'])
            addMethod = getattr(zone, methodName)

            record = self.createRecord(addMethod, zoneKey, [answers], rec['TTL'])
            record.addCallback(self.createRecordSuccess)
            record.addErrback(self.createRecordFailure, zoneKey, rec['Type'], answers, nsoneObj)
            dl.append(record)
        return defer.DeferredList(dl, fireOnOneErrback=True)


    def loadZoneFailure(self, failure, zoneKey):
        print '{}: {}'.format(zoneKey, failure.getErrorMessage())


    @defer.inlineCallbacks
    def createRecord(self, addMethod, zoneKey, answers, ttl):
        record = yield addMethod(zoneKey, answers, ttl=ttl)
        defer.returnValue(record)


    def createRecordSuccess(self, response):
        print 'Created record: {}'.format(response)


    @defer.inlineCallbacks
    def createRecordFailure(self, failure, zoneKey, recType, answers, nsoneObj):
        f = failure.trap(ResourceException)
        if f == ResourceException:
            record = self.loadRecord(zoneKey, recType, nsoneObj)
            record.addCallback(self.loadRecordSuccess, answers)
            record.addErrback(self.loadRecordFailure)
            yield record


    @defer.inlineCallbacks
    def loadRecord(self, zoneKey, recType, nsoneObj):
        record = yield nsoneObj.loadRecord(zoneKey, recType, zoneKey)
        defer.returnValue(record)


    @defer.inlineCallbacks
    def loadRecordSuccess(self, response, answers):
        print 'Successfully loaded Record: {}'.format(response)
        record = response
        addRecordAnswersRes = self.addRecordAnswers(record, answers)
        addRecordAnswersRes.addCallback(self.addRecordAnswersSuccess, answers)
        addRecordAnswersRes.addErrback(self.addRecordAnswersFailure)
        yield addRecordAnswersRes


    def loadRecordFailure(self, failure):
        print failure.getErrorMessage()


    @defer.inlineCallbacks
    def addRecordAnswers(self, record, answers):
        recordData = yield record.data
        recordAnswers = {answer['answer'][0] for answer in recordData['answers']}
        if recordAnswers.intersection(answers[0]):
            print 'Adding answer: {}'.format(answers[0])
            yield record.addAnswers(answers)


    def addRecordAnswersSuccess(self, response, answers):
        print 'Successfully processed answers: {}'.format(answers)


    def addRecordAnswersFailure(self, failure):
        print failure.getErrorMessage()


    def startRequests(self, reactor):
        if self.deleteData:
            return self.deleteZoneData()
        else:
            return self.importZoneData()


    def run(self):
        task.react(self.startRequests)
