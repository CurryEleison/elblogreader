from AwsElbLogUtil import LogFileList, LogDataFrame
import boto3
import pandas as pd

def main():

    s3 = boto3.resource('s3')
    # Set up to get recent logfiles
    loglistgetter = LogFileList(s3res = s3)
    # possible values are: adm, api, mainsites, simplesitecom, userdomains, usermainsites, usersimplesites
    recents = loglistgetter.get_recents_elb("userdomains")
    # Set up object to read in the logfiles
    framegetter = LogDataFrame(s3res = s3)
    # Take filenames, download and make into a dataframe
    df = framegetter.make_dataframe(recents)
    
    
    # print out names and timestamps of recents
    printrecentssummary(s3, recents)

    # Print hottest IPs
    ipsummary = df[['remoteip', 'remoteport']].groupby('remoteip').agg('count').sort_values('remoteport', ascending=False)
    print ipsummary.head(10)
    # Print most expensive paths
    urltimetaken = df[['path', 'servertime']].groupby('path').sum().sort_values('servertime', ascending=False) #.agg({'servertime', 'sum'})
    print urltimetaken.head(10)
    # Print most expensive hosts
    hosttimetaken = df[['hostname', 'servertime']].groupby('hostname').sum().sort_values('servertime', ascending=False)
    print hosttimetaken.head(10)


def printrecentssummary(s3res, s3items):
    for s3objsummary in s3items:
        s3obj = s3res.Object(s3objsummary.bucket_name, s3objsummary.key)
        print "{0.last_modified} {0.key}".format(s3obj)




if __name__ == "__main__":
    main()


