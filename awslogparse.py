import boto3
import StringIO
from datetime import tzinfo, datetime, timedelta
import csv
import pandas as pd
import numpy as np
from urlparse import urlparse

ZERO = timedelta(0)


#A class to fetch a list of logfilesd from S3 object summaries and put them into a dataframe
class LogDataFrame:
    """LogDataFrame"""

    def __init__(self, s3res ):
        self.s3res = s3res

    # Maybe add an argument to exclude lines. Perhaps a lambda that takes 
    # a LogLine object and returns a bool or something to scan a string and return a bool
    def make_dataframe(self, s3items):
        loglines = []
        for s3objsummary in s3items:
            s3obj = self.s3res.Object(s3objsummary.bucket_name, s3objsummary.key)
            buf = StringIO.StringIO()
            s3obj.download_fileobj(buf)
            buf.seek(0)
            csvreader = csv.reader(buf, delimiter = ' ', quotechar = '"')
            for row in csvreader:
                loglines.append(LogLine(row))
            buf.close()
        variables = loglines[0].__dict__.keys()
        return pd.DataFrame([[getattr(i,j) for j in variables] for i in loglines], columns = variables)


#A class to get recent files from S3
class LogFileList:
    """LogFileList"""

    def __init__(self, s3res, account = '377243189808', region = 'eu-west-1', minimumfiles = 5):
        self.account = account
        self.region = region
        self.minimumfiles = minimumfiles
        self.s3res = s3res

    def get_recents(self, lbname):
        utc = UTC()
        allitems = []
        iterations = 0
        maxiterations = 15
        tenminspast = timedelta(minutes=-10)
        mytime = datetime.now(utc)
        s3foldertemplate = "loadbalancers/{loadbalancer}/AWSLogs/{account}/elasticloadbalancing/{region}/{dt.year:0>4}/{dt.month:0>2}/{dt.day:0>2}/"
        s3filekeyroottemplate = "{account}_elasticloadbalancing_{region}_{loadbalancer}_{dt.year:0>4}{dt.month:0>2}{dt.day:0>2}T{dt.hour:0>2}"
        while (len(allitems) <= self.minimumfiles and iterations < maxiterations):
            folderprefix = s3foldertemplate.format(dt = mytime, loadbalancer = lbname, account = self.account, region = self.region) 
            itemprefix = s3filekeyroottemplate.format(dt = mytime, loadbalancer = lbname, account = self.account, region = self.region )
            bucket = self.s3res.Bucket("123logging")
            allitems.extend(sorted(bucket.objects.filter(Prefix=folderprefix + itemprefix), key = lambda item: item.last_modified, reverse=True))
            iterations += 1
            mytime += tenminspast
            

        recents = [x for ind, x in enumerate(allitems) if self.minimumfiles > ind >= 0 ]
        return recents



# A UTC class. From tzinfo docs
class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

# A class to hold a log line
class LogLine:
    """LogLine"""

    def __init__(self, fields):
        self.utctime = datetime.strptime(fields[0], '%Y-%m-%dT%H:%M:%S.%fZ')
        self.loadbalancer = fields[1]
        (self.remoteip, self.remoteport) = fields[2].split(':')
        if (fields[3] != '-'):
            (self.hostip, self.hostport) = fields[3].split(':')
        else:
            (self.hostip, self.hostport) = ("-", "-1")
        self.time1 = float(fields[4])
        self.servertime = float(fields[5])
        self.time2 = float(fields[6])
        self.responsecode = int(fields[7])
        reqfields = fields[11].split(' ')
        if len(reqfields) >= 2:
            self.method = reqfields[0]
            self.url = reqfields[1]
            if (len(reqfields) >=3):
                self.protocol = reqfields[2]
            else:
                self.protocol = "unknown"
        else:
            print reqfields
        u = urlparse(self.url)
        self.hostname = u.hostname
        self.path = u.path
        self.useragent = fields[12]
        self.encryption = fields[13]




