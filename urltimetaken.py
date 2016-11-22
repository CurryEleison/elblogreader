from awslogparse import LogFileList, LogDataFrame, UTC
from datetime import datetime
import boto3
import pandas as pd

def main():

    reftime = datetime(2016, 11, 22, 9, 30, 00, 0, UTC())
    print reftime

    s3 = boto3.resource('s3')
    # Set up to get recent logfiles
    loglistgetter = LogFileList(s3res = s3, strictreftime = True)
    # possible values are: adm, api, mainsites, simplesitecom, userdomains, usermainsites, usersimplesites
    recents = loglistgetter.get_recents("mainsites", refdate = reftime)
    # Set up object to read in the logfiles
    framegetter = LogDataFrame(s3res = s3)
    # Take filenames, download and make into a dataframe
    df = framegetter.make_dataframe(recents, lambda l: hasattr(l, 'path') and l.path.startswith('/tpltest'))
    
    
    # print out names and timestamps of recents
    # printrecentssummary(s3, recents)
    print df.sort_values(by = 'utctime', ascending=False).head(10)['servertime'].mean()




def printrecentssummary(s3res, s3items):
    for s3objsummary in s3items:
        s3obj = s3res.Object(s3objsummary.bucket_name, s3objsummary.key)
        print "{0.last_modified} {0.key}".format(s3obj)




if __name__ == "__main__":
    main()


