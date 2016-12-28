import pandas as pd
from AwsElbLogUtil import LogDataFrame
import boto3
import datetime






def main():
    foldername = "~/junk"
    s3 = boto3.resource('s3')
    dfmaker = LogDataFrame(s3)
    df = dfmaker.make_dataframe_fromfolder(foldername, lambda l: hasattr(l, 'path') and l.path.startswith('/tpltest'))
    meantime = df.sort_values(by = 'utctime', ascending=False).head(10)['servertime'].mean()
    totallines = df.shape
    print "Mean time of most recent 10 lines was {0} and shape of linelines was {1}".format(meantime, totallines)
    df['roundedtime'] = df['utctime'].apply(lambda dt: datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute))
    df = df.assign(mintime = df.servertime).assign(maxtime = df.servertime).assign(sumtime = df.servertime).assign(reccount = df.method)
    # print df
    summary = df.groupby('roundedtime').agg(
            {
                'maxtime': 'max', 
                'mintime': 'min', 
                'sumtime': 'sum', 
                'reccount': 'count'
                }
            )
    for index, item in summary.iterrows():
        print "At {4} Min: {0}, Max: {1}, Sum: {2}, Count: {3}".format(item['mintime'], item['maxtime'], item['sumtime'], item['reccount'], index)
#         resp = sender.senddataaggregate(datatime = index, datalength = item['reccount'], 
#                datasum = item['sumtime'], datamin = item['mintime'], datamax = item['maxtime'])
#        logging.info(resp)








if __name__ == '__main__':
    main()


