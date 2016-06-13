import os.path
import csv
import argparse


class ZoneDataParser(object):


    def _isValidFile(self, parser, arg):
        arg = os.path.abspath(arg)
        if not os.path.exists(arg):
            parser.error("The file %s does not exist!" % arg)
        else:
            return arg


    def getArgs(self):
        parser = argparse.ArgumentParser(description='Import some Zone Data to NS1')
        parser.add_argument("-a", "--apikey",
                            dest="apikey",
                            type=str,
                            required=True,
                            metavar="API_KEY",
                            help="Your NS1 api key with this flag")
        parser.add_argument("-f", "--file",
                            dest="filename",
                            type=lambda x: self._isValidFile(parser, x),
                            required=True,
                            metavar="FILE",
                            help="Import Zone data from file with this flag")
        parser.add_argument("-d", "--delete",
                            dest="delete",
                            action='store_true',
                            help="Delete Zone data from file with this flag")
        args = parser.parse_args()
        return args


    def _readCsv(self, reader):
        """Lets csv data be evaluated lazily. Since file might be huge"""
        for row in reader:
            yield row


    def _readDataDict(self, dataDict):
        """Lets dictionary data be evaluated lazily. Since the file might be huge"""
        for k, v in dataDict.iteritems():
            yield k, v


    def _transformCsv(self, csvData):
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


    def _transformJson(jsonData):
        """Not Implemented. Assuming json transformed similar to csv"""
        return jsonData


    def loadZoneData(self, filename):
        extension = os.path.splitext(filename)[1]
        with open(filename, 'rb') as f:
            if extension == '.csv':
                reader = csv.DictReader(f)
                dataDict = self._transformCsv(self._readCsv(reader))
                data = self._readDataDict(dataDict)
            elif extension == '.json':
                dataDict = json.loads(f)
                data = self._transformJson(self._readDataDict(jsonDict))

        return data
