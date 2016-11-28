from awslogparse import LogFileList, LogFileDownloader, UTC
from datetime import datetime
import boto3
import argparse
import warnings
from ruamel.yaml.error import UnsafeLoaderWarning
warnings.simplefilter('ignore', UnsafeLoaderWarning)
import dateparser

# these are here for the benefit of dateparser Russian dates. Maybe look for a safer dateparser somewhere

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("loadbalancer", help="The loadbalancer we want to download logfiles for")
    parser.add_argument("--targetfolder", help="The folder the files are downloaded to")
    parser.add_argument("--numfiles", type=int, help="Number of files to download")
    parser.add_argument("--endtime", help="The last time for which we want logfiles")
    parser.add_argument("--logfolder", help="Subfolder in S3 if different from load balancer name")
    args = parser.parse_args()

    s3 = boto3.resource('s3')

    lb = args.loadbalancer
    #  
    endtime = datetime.now(UTC()) if args.endtime == None else dateparser.parse(args.endtime, settings={'TIMEZONE': 'UTC'})
    targetfolder = "~/{0}-{1:%Y-%m-%d}".format(lb, endtime) if args.targetfolder == None else args.targetfolder
    numfiles = 5 if args.numfiles == None else args.numfiles
    lblogfolder = args.logfolder



    print "Downloading {3} logfiles for loadbalancer '{0}' to folder: '{2}'. time should be from the hour around {1} or earlier.".format(lb, endtime, targetfolder, numfiles)


#    print args.loadbalancer
#    print args.targetfolder

    # reftime = datetime(2016, 11, 23, 23, 30, 00, 0, UTC())
    # Set up to get recent logfiles
    loglistgetter = LogFileList(s3res = s3, minimumfiles = numfiles)
    # possible values are: adm, api, mainsites, simplesitecom, userdomains, usermainsites, usersimplesites
    recents = loglistgetter.get_recents(lb, refdate = endtime, lblogfolder = lblogfolder)
    # Set up object to read in the logfiles

    downloader = LogFileDownloader(folder = targetfolder, s3res = s3)
    downloader.download_logs(recents)


    # framegetter = LogDataFrame(s3res = s3)
    # Take filenames, download and make into a dataframe
    # df = framegetter.make_dataframe(recents)
    
    
    # print out names and timestamps of recents
    # printrecentssummary(s3, recents)

    # Print hottest IPs
    # ipsummary = df[['remoteip', 'remoteport']].groupby('remoteip').agg('count').sort_values('remoteport', ascending=False)
    # print ipsummary.head(10)
    # Print most expensive paths
    # urltimetaken = df[['path', 'servertime']].groupby('path').sum().sort_values('servertime', ascending=False) #.agg({'servertime', 'sum'})
    # print urltimetaken.head(10)
    # Print most expensive hosts
    # hosttimetaken = df[['hostname', 'servertime']].groupby('hostname').sum().sort_values('servertime', ascending=False)
    # print hosttimetaken.head(10)


def printrecentssummary(s3res, s3items):
    for s3objsummary in s3items:
        s3obj = s3res.Object(s3objsummary.bucket_name, s3objsummary.key)
        print "{0.last_modified} {0.key}".format(s3obj)




if __name__ == "__main__":
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UnsafeLoaderWarning)
        warnings.filterwarnings('ignore')
        main()


