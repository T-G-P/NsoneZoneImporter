from nsone import NSONE
from nsone.rest.errors import AuthException, ResourceException
import os.path
import csv
import argparse


def is_valid_file(parser, arg):
    arg = os.path.abspath(arg)
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return arg


def transform_csv(reader):
    """
    Transforms the csv to a more easily processed dict to minimize
    rest api calls for creating and loading zones unecessarily.
    This is implemented since it is overkill to try to create or load
    the zones for each row using the api

    The csv data structure is transformed to look like this:
    {
        'example.com': [
            {
                'Data': '1.2.3.4',
                'Name': "@',
                'TTL': '86400',
                'Type': 'A'
            },
            {
                'Data': '5.2.3.4',
                'Name': "@',
                'TTL': '86400',
                'Type': 'A'
            },
        ],
        'example2.com': [
            {
                'Data': '2.3.4.5',
                'Name': "www',
                'TTL': '86400',
                'Type': 'CNAME'
            },
    }
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


def transform_json(json_dict):
    """Not Implemented. Assuming json formatted similar to csv"""
    return json_dict


def import_zone_data(filename, api_key, **kwargs):
    nsone_obj = NSONE(apiKey=api_key)
    extension = os.path.splitext(filename)[1]

    with open(filename, 'rb') as f:
        if extension == '.csv':
            reader = csv.DictReader(f)
            data = transform_csv(reader)
        elif extension == '.json':
            data = transform_json(json.loads(f))

    if kwargs.get('delete'):
        delete_zone_data(nsone_obj, data)
        return

    for k, v in data.iteritems():

        try:
            zone = nsone_obj.createZone(k)
            print 'Added Zone {}'.format(zone)
        except AuthException as e:
            # Invalid api key passed in
            print '{} {}'.format(api_key, e.message)
        except ResourceException as e:
            # zone already exists
            print '{} {}'.format(k, e.message)
            zone = nsone_obj.loadZone(k)
            print 'Loaded Zone {}'.format(zone)

        for rec in v:

            answers = rec['Data'].split()
            try:
                # determine which record type to add using types provided in the file
                method_name = 'add_{}'.format(rec['Type'])
                try:
                    add_method = getattr(zone, method_name)
                except AttributeError:
                    # Invalid type, skip this record
                    continue
                # data corresponds to the answer and it might have priority values
                record = add_method(k, [answers], ttl=rec['TTL'])
                print 'Added record {}'.format(record)
            except ResourceException as e:
                # record already exists, so add answers to it
                print '{} {}'.format(rec, e.message)
                record = nsone_obj.loadRecord(k, rec['Type'], k)
                print 'Loaded Record {}'.format(record)
                try:
                    record.addAnswers([answers])
                    print 'Added Answers {}'.format(answers)
                except ResourceException as e:
                    # Invalid format for answer, so ignore this answer
                    print 'Invalid Answers {}'.format(answers)
                    continue


def delete_zone_data(nsone_obj, data):
    for key in data.keys():
        try:
            nsone_obj.loadZone(key).delete()
            print 'Deleting Zone: {}'.format(key)
        except AuthException as e:
            # Invalid api key passed in
            print '{} {}'.format(nsone.config.getAPIKey(), e.message)
        except ResourceException as e:
            print '{} {}'.format(key, e.message)

def main():
    parser = argparse.ArgumentParser(description='Import some Zone Data to NS1')
    parser.add_argument("-a", "--api_key",
                        dest="api_key",
                        type=str,
                        required=True,
                        metavar="API_KEY",
                        help="Your NS1 api key with this flag")
    parser.add_argument("-f", "--file",
                        dest="filename",
                        type=lambda x: is_valid_file(parser, x),
                        required=True,
                        metavar="FILE",
                        help="Import Zone data from file with this flag")
    parser.add_argument("-d", "--delete",
                        dest="delete",
                        action='store_true',
                        help="Delete Zone data from file with this flag")
    args = parser.parse_args()

    import_zone_data(args.filename, args.api_key, delete=args.delete)


if __name__ == '__main__':
    main()
