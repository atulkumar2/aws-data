"""
# This will be used to find data about AWS S3 buckets
# Usage
#    <ScriptName> --profile <awsprofilename> --verbose
#    <ScriptName> -p <awsprofilename> -v
"""

from sys import argv
import boto3
import datetime


def getopts(argv):
    """ Parses arguments and puts them in a list

    :param argv: Arguments passed to program
    :return: List of arguments
    """

    opts = {}
    while argv:
        if (len(argv[0]) > 2) and argv[0][0:2] == '--':
            if len(argv) > 1 and argv[1][0] != '-':
                opts[argv[0]] = argv[1]
            else:
                opts[argv[0]] = ''
        elif argv[0][0] == '-':
            if len(argv) > 1 and argv[1][0] != '-':
                opts[argv[0]] = argv[1]
            else:
                opts[argv[0]] = ''
        argv = argv[1:]
    keys = list(opts.keys()).copy()
    for key in keys:
        opts[key.lstrip('-').lower()] = opts[key]
        del (opts[key])
    return opts


def help() -> object:
    """ Prints help string

    :return: nothing
    """

    print("Usage")
    print("<ScriptName> Options")
    print("OPTIONS")
    print('  [--profile or -p <profilename>] AWS profile name')
    print('  [--verbose or -v] verbose display')
    print('  [--help or -h] Help message')
    print('If options are not given, it will try to use default profile '
          'Install aws-cli and Run aws configure beforehands.')
    print("")


# Main module
def main():
    # Get Arguments
    opts = getopts(argv)
    print(opts)

    if 'help' in opts or 'h' in opts:
        help()
        exit()

    if len(opts) == 0:
        help()
        print('---------------------------')
        print('Going with default options')

    profile_name = None
    if 'profile' in opts:
        profile_name = opts['profile']
    elif 'p' in opts:
        profile_name = opts['p']

    verbose = False
    if 'verbose' in opts or 'v' in opts:
        verbose = True

    '''
    session = boto3.session.Session(profile_name=profile_name)
    client = boto3.client('pricing')
    client.describe_services(ServiceCode='S3')
    return
    '''

    session = boto3.session.Session(profile_name=profile_name)
    client = session.client('s3')
    buckets_data = get_s3_buckets_data(client, verbose=verbose)

    if verbose:
        for key in buckets_data:
            print(key, buckets_data[key])

    return buckets_data

def get_s3_buckets_data(s3_client: boto3.client, verbose:False) -> object:
    """ Collects S3 data for an AWS region
    :param client: AWS S3 session client
    :return: Dict object with bucket names as key and each entry has information
    on bucket size, total files, total file size, creation date and last modified date
    """

    response = s3_client.list_buckets()

    buckets = [bucket['Name'] for bucket in response['Buckets']]
    if len(buckets) == 0:
        print_v('No S3 buckets found', verbose)
        return None
    else:
        print_v('{0} S3 buckets found'.format(len(buckets)), verbose)

    buckets_data: dict = {bucket['Name']:bucket for bucket in response['Buckets']}

    for bucket in buckets:
        print_v('Working on bucket {0}'.format(bucket), verbose)

        tot_files = 0
        tot_file_size = 0
        g_last_modified_date = datetime.datetime(year=1970, month=1, day=1)

        kwargs = {'Bucket': bucket}
        while True:
            resp = s3_client.list_objects_v2(**kwargs)
            tot_files += resp['KeyCount']

            try:
                obj: dict
                for obj in resp['Contents']:
                    tot_file_size += obj['Size']
                    last_modified_date = obj['LastModified'].replace(tzinfo=None)
                    if last_modified_date > g_last_modified_date:
                        g_last_modified_date = last_modified_date
            except KeyError:
                break

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

        buckets_data[bucket]['Total Files'] = tot_files
        buckets_data[bucket]['Total File Size'] = tot_file_size
        buckets_data[bucket]['Last Modified Date'] = last_modified_date

    return buckets_data


def print_v(info, verbose:False):
    if verbose:
        print(info)


# Run main program
main()