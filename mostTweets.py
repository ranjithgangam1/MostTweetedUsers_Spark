# To run, do: spark-submit --master yarn-client -num-executors 17 avgTweetLength.py hdfs://hadoop2-0-0/data/twitter/part-03212

from pyspark import SparkContext
import HTMLParser
import json

#sc=SparkContext("local", "App Name")
sc = SparkContext(appName="Q5")
#tf = sc.textFile("/user/bollara/twitter_input/")
tf = sc.textFile("/data/twitter/")
input_list = tf.map(lambda x: json.loads(x))

textList = input_list.map(lambda x: [x['user']['screen_name'], HTMLParser.HTMLParser().unescape(x['text'])])


totaldays = textList.map(lambda a: (a[0],(len(a[1]),1))).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))

maxTweetUser = totaldays.takeOrdered(1, key=lambda x: -x[1][1])

print maxTweetUser
outList = totaldays.map(lambda x: (x[0],float(x[1][0])/x[1][1]))

minTweetAvgCnt = outList.takeOrdered(5, key=lambda x: x[1])
maxTweetAvgCnt = outList.takeOrdered(5, key=lambda x: -x[1])
print minTweetAvgCnt
print maxTweetAvgCnt
sc.parallelize(['Max Tweeted Twitter:'] + maxTweetUser + ['Min Average Tweet Length Users:'] + minTweetAvgCnt + ['Max Average Tweet Length Users:'] + maxTweetAvgCnt).coalesce(1).saveAsTextFile("MaxUserTweet")
  
sc.stop()
