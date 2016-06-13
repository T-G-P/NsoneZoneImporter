from zonedataparser import ZoneDataParser
from nsoneimporter import NsoneImporter

def main():
    zoneDataParser = ZoneDataParser()
    args = zoneDataParser.getArgs()
    data = zoneDataParser.loadZoneData(args.filename)

    nsoneImporter = NsoneImporter(args.apikey, data, args.delete)
    nsoneImporter.run()

if __name__ == '__main__':
    main()
