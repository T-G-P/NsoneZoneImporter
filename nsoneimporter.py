from nsone import NSONE, Config
from nsone.rest.errors import ResourceException
from twisted.internet import defer, reactor, task


class NsoneImporter(object):
    """
    Attributes:
        config (nsone.Config): The configuration for the nsone requests.
        nsoneObj (nsone.NSONE): Instance of the nsone object used for http requests
        data (dict): Dictionary containing the zone data used by all methods for importing
        deleteData (bool): Attribute used to call deletion endpoints instead of importing
    """

    config = Config()


    def __init__(self, apiKey, data, delete):
        """
        Args:
            apiKey (str):  The Nsone Api Key
            data (Dict):  Zone Data Dict
            delete (bool): Delete Flag, defaults to false from argument parser
        """

        self.config.createFromAPIKey(apiKey)
        self.config['transport'] = 'twisted'
        self.nsoneObj = NSONE(config=self.config)
        self.data = data
        self.deleteData = delete


    def _deleteZoneData(self):
        """
        Parent method that triggers callback chain for deleting all zones.

        Makes call to deleteZonesandRecords method and depending on the response,
        the success or error callbacks are fired.

        Each call to deleteZonesandRecords is an instance of a deferred object and
        each object is collected for graceful termination
        """

        dl = []
        for zoneName, records in self.data:
            deleteZoneRes = self._deleteZonesAndRecords(zoneName, records, self.nsoneObj)
            deleteZoneRes.addCallback(self._deleteZoneSuccess, zoneName)
            deleteZoneRes.addErrback(self._deleteZoneFailure, zoneName)
            dl.append(deleteZoneRes)
        return defer.DeferredList(dl, fireOnOneErrback=True)


    @defer.inlineCallbacks
    def _deleteZonesAndRecords(self, zoneName, records, nsoneObj):
        """
        Returns a deferred object that calls the delete method of the nsone object.

        Args:
            zoneName (str):  The zone name from the data dictionary
            records (Dict):  a list of records belonging to this zone
            nsoneObj (nsone.NSONE): Instance of the nsone object

        """

        zone = yield nsoneObj.loadZone(zoneName)
        yield zone.delete()


    def _deleteZoneSuccess(self, response, zoneName):
        """
        Success callback for deleteZonesAndRecords deferred object.
        Triggered if there are no errors when calling the api

        Args:
            response (None): Upon success, the delete method returns None
            zoneName (str):  The zone name
        """

        print 'Successfully Deleted Zone: {}'.format(zoneName)


    def _deleteZoneFailure(self, failure, zoneName):
        """
        Error callback for deleteZonesAndRecords deferred object.
        Triggered if there are errors when calling the api

        Args:
            failure (twisted.python.failure)
            zoneName (str):  The zone name
        """

        print '{}: {}'.format(zoneName, failure.getErrorMessage())


    def _importZoneData(self):
        """
        The parent method that triggers all of the callback chains for importing zone data.

        Loops through all of the zone data and creates a deferred object for every
        zone and adds Success and Error callbacks for each deferred instance.

        Tracks all of the deferreds by appending them to a deferred list.
        If no errors occur, the success callback is triggered. If even a single
        error occurs, the fireOnOneErrback parameter is set to true and
        the error callback is triggered and the program exits.

        NOTE: Auth exceptions are not explicitely checked since if an
        nsone.rest.errors.AuthException is raised, the deferred list error back
        will fire and the program will exit

        Returns:
            defer.DeferredList


        """
        dl = []
        for zoneName, records in self.data:
            zone = self._createZone(zoneName)
            zone.addCallback(self._createZoneSuccess, zoneName, records, self.nsoneObj)
            zone.addErrback(self._createZoneFailure, zoneName, records, self.nsoneObj)
            dl.append(zone)
        return defer.DeferredList(dl, fireOnOneErrback=True)


    @defer.inlineCallbacks
    def _createZone(self, zoneName):
        """
        Returns the result of the deferred zone api call when available.

        Args:
            zoneName (str): The zone name in the data dictionary

        Returns:
            nsone.zones.Zone
        """

        zone = yield self.nsoneObj.createZone(zoneName)
        defer.returnValue(zone)


    def _createZoneSuccess(self, response, zoneName, records, nsoneObj):
        """
        Success callback for _createZone deferred object.
        Triggered if there are no errors when calling the api.

        If a zone is created successfully, then the next logical step
        during the import process is to add all of the records for that zone

        This method calls _createRecords which creates
        deferred objects for all of the records in the record list and adde
        both success and error callbacks for each instance.

        Each deferred record is tracked with the DeferredList object
        and the error back is fired as soon as one error back is fired

        Args:
            response (nsone.zones.Zone): The zone returned from createZone
            zoneName (str):  The zone name
            records (list): The list of records belonging to the zone
            nsoneObj (nsone.NSONE): Instance of the nsone object

        Returns:
            defer.DeferredList
        """
        return self._createRecords(response, zoneName, records, nsoneObj)


    @defer.inlineCallbacks
    def _createZoneFailure(self, failure, zoneName, records, nsoneObj):
        """
        Failure Error callback if a zone cannot be created

        If a zone fails to be created, it is likely due to the fact that
        it exists already. The next logical step is to try to load the existing zone.

        A deferred zone is created with the _loadZone method. If a zone is loaded
        successfully, the _loadZoneSuccess callback is fired. Othewise, the error
        callback is fired

        A deferred zone object is yielded and the response from _loadZone is returned
        to the generator when it is available.


        Args:
            failure (twisted.python.failure)
            zoneName (str):  The zone name
            records (list): The list of records belonging to the zone
            nsoneObj (nsone.NSONE): Instance of the nsone object

        Yields:
            twisted.internet.defer
        """

        f = failure.trap(ResourceException)
        print failure.getErrorMessage()

        zone = self._loadZone(zoneName, nsoneObj)
        zone.addCallback(self._loadZoneSuccess, zoneName, records, nsoneObj)
        zone.addErrback(self._loadZoneFailure, zoneName)
        yield zone


    @defer.inlineCallbacks
    def _loadZone(self, zoneName, nsoneObj):
        """
        Gets the result of the deferred load zone api call when available.

        Args:
            zoneName (str): The zone name in the data dictionary
            nsoneObj (nsone.NSONE): Instance of the nsone object

        Returns:
            nsone.zones.Zone
        """

        zone = yield nsoneObj.loadZone(zoneName)
        defer.returnValue(zone)


    def _loadZoneSuccess(self, response, zoneName, records, nsoneObj):
        """
        Success callback for _loadZone deferred object.
        Triggered if there are no errors when calling the api.

        If a zone is loaded successfully, then the next logical step
        during the import process is to add all of the records for that zone
        similar to creating a zone

        This method calls a helper method that creates deferred objects
        for all of the records  in the record list and addes both success
        and error callbacks for each instance.

        Each deferred record is tracked with the DeferredList object
        and the error back is fired as soon as one error back is fired

        Args:
            response (nsone.zones.Zone): The zone returned from createZone
            zoneName (str):  The zone name
            records (list): The list of records belonging to the zone
            nsoneObj (nsone.NSONE): Instance of the nsone object

        Returns:
            defer.DeferredList
        """

        print 'Successfully Loaded Zone: {}'.format(zoneName)

        return self._createRecords(response, zoneName, records, nsoneObj)


    def _loadZoneFailure(self, failure, zoneName):
        """
        Triggered when a zone cannot be loaded
        Prints the error message from the failure

        Args:
            failure (twisted.python.failure):
            zoneName (str):  The zone name
        """

        print '{}: {}'.format(zoneName, failure.getErrorMessage())


    def _createRecords(self, response, zoneName, records, nsoneObj):
        """
        This method reates deferred objects
        for all of the records  in the record list and addes both success
        and error callbacks for each instance.

        Records are logically created either after creating a zone
        or loading a zone successfully which is why this functionality
        is modularized into this function to remove duplicate logic.

        Each deferred record is tracked with the DeferredList object
        and the error back is fired as soon as one error back is fired

        Args:
            response (nsone.zones.Zone): The zone returned from createZone
            zoneName (str):  The zone name
            records (list): The list of records belonging to the zone
            nsoneObj (nsone.NSONE): Instance of the nsone object

        Returns:
            defer.DeferredList
        """

        dl = []
        zone = response
        for rec in records:
            answers = rec['Data'].split()
            methodName = 'add_{}'.format(rec['Type'])
            addMethod = getattr(zone, methodName)

            record = self._createRecord(addMethod, zoneName, [answers], rec['TTL'])
            record.addCallback(self._createRecordSuccess)
            record.addErrback(self._createRecordFailure, zoneName, rec['Type'], answers, nsoneObj)
            dl.append(record)
        return defer.DeferredList(dl, fireOnOneErrback=True)


    @defer.inlineCallbacks
    def _createRecord(self, addMethod, zoneName, answers, ttl):
        """
        Calls the add_X method on zones for creating records and
        returns the value when it's available

        Args:
            addMethod (function) : add_X method of the zone where X is dynamic
            zoneName (str): The zone name from the data dict
            answers (list): The answers for the record in list form
            ttl (str): The TTL value for the record

        Return:
            nsone.records.Record
        """
        record = yield addMethod(zoneName, answers, ttl=ttl)
        defer.returnValue(record)


    def _createRecordSuccess(self, response):
        """
        Triggered when a record is created successfully

        If a record is created successfully, an nsone record instance is returned

        Args:
            response (nsone.records.Record): an instance of an nsone record object
        """
        print 'Created record: {}'.format(response)


    @defer.inlineCallbacks
    def _createRecordFailure(self, failure, zoneName, recType, answers, nsoneObj):
        """
        Triggered when a record cannot be created.

        Logically if a record can't be created, an attempt at loading it is made.
        A deferred record object is created and both success and error callbacks
        are chained onto it.


        Args:
            failure (twisted.python.failure): the failure object

        Yields:
            nsone.records.Record

        """
        f = failure.trap(ResourceException)
        if f == ResourceException:
            record = self._loadRecord(zoneName, recType, nsoneObj)
            record.addCallback(self._loadRecordSuccess, answers)
            record.addErrback(self._loadRecordFailure)
            yield record


    @defer.inlineCallbacks
    def _loadRecord(self, zoneName, recType, nsoneObj):
        """
        Calls the loadRecord  method on nsoneObj
        returns the value of the record when it's available

        Args:
            zoneName (str): The zone name from the data dict
            recType (str): The record type
            nsoneObj (nsone.NSONE): Instance of the nsone object

        Return:
            nsone.records.Record
        """

        record = yield nsoneObj.loadRecord(zoneName, recType, zoneName)
        defer.returnValue(record)


    @defer.inlineCallbacks
    def _loadRecordSuccess(self, response, answers):
        """
        Triggered when a record is successfully loaded

        Logically if a record is loaded the next step would be to
        try to add answers to it.

        A deferred object is yielded and the response from the
        addAnswers method on the record is returned to the generator
        when available. The response from the method triggers either
        the succes or error callback on the deferred object.

        Args:
            response (nsone.records.Record): the record instance
            answers (list): The answers to be added to the record

        Yields:
            twisted.internet.defer

        """

        print 'Successfully loaded Record: {}'.format(response)
        record = response
        addRecordAnswersRes = self._addRecordAnswers(record, answers)
        addRecordAnswersRes.addCallback(self._addRecordAnswersSuccess, answers)
        addRecordAnswersRes.addErrback(self._addRecordAnswersFailure)
        yield addRecordAnswersRes


    def _loadRecordFailure(self, failure):
        """
        Prints the failure message if a record fails to load

        Args:
            failure (twisted.python.failure): The failure object
        """
        print failure.getErrorMessage()


    @defer.inlineCallbacks
    def _addRecordAnswers(self, record, answers):
        """
        Calls the addAnswers method on the nsone.records.Record object

        If the record answers already contain the answers passed
        into this method, then nothing is done. Otherwise, the
        addAnswers method is called and the answers are added to
        the record

        Returns a deferred object with the response from the
        addAnswers method on the record object when it is available
        or returns None if the record answers intersect with
        what's passed in here.

        Args:
            record (nsone.records.Record): The nsone record object
            answers (list): The record answers

        Yields:
            None
        """

        recordData = yield record.data
        recordAnswers = {answer['answer'][0] for answer in recordData['answers']}
        if not recordAnswers.intersection(answers):
            print 'Adding answer: {}'.format(answers)
            yield record.addAnswers(answers)
        else:
            print 'Answer already exists: {}'.format(answers[0])


    def _addRecordAnswersSuccess(self, response, answers):
        """
        Prints a success message to show that the answers were added successfully.

        Args:
            response (None): the addAnswers method on the record returns None on success
            answers (list): The record answers

        """
        print 'Successfully processed answers: {}'.format(answers)


    def _addRecordAnswersFailure(self, failure):
        """
        Prints a failure message to show that the answers weren't added

        Args:
            failure (twisted.python.failure): the twisted failure object

        """
        print failure.getErrorMessage()


    def _startRequests(self, reactor):
        """
        This method initializes either the zone data import or deletion.

        All of the api requests are triggered here

        Args:
            reactor (twisted.internet.reactor)

        Return:
            function
        """

        if self.deleteData:
            return self._deleteZoneData()
        return self._importZoneData()


    def run(self):
        """
        Schedules the startRequests method and gracefully exits the program when either
        all of the deferred objects fire successfully or fail
        """

        task.react(self._startRequests)
