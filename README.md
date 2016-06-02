# MostTweetedUser_mapReduce

####What twitter user tweeted the most?  What is the top 5 longest tweeters over each’s average tweet length?  Bottom 5?

#####Aim:

Generate an output file using spark that has below information from the given input data.

a. Twitter user who tweeted the most.

b. Top 5 Twitter users per tweet text average length.

c. Bottom 5 Twitter users per tweet text average length.

#####Approach:

 Read given twitter data and create RDD with each line in json format.

 Create RDD for the required data from input data using map. In this case we need Screen name and Tweet Text to calculate required statistics.

 Calculate Tweet count and Tweet Length as tuple for each mapper in map reduce and output to total days RDD.

 Find the user that has highest Tweet count using takeOrdered action for the above created total days RDD.

 Create spark transformation to use above calculated tweets Length and Tweet Count

to find average tweet length per each user.

 Now use takeOrdered of 5 with key x[1] to find top 5 twitter users and

 Use takeOrdered of 5 with key –x[1] to find bottom 5 twitter users

 Add these results in to single list and save as text file to HDFS home directory output. 

#####Output

Max Tweeted Twitter: (u&#39;marilyn9743&#39;, (462238, 3419))

Min Avg. Tweet Length Users: ShakeIt4Rome, Im_Lil_Wanie, Abby_Palmiter,

HannahGarwood, Oliviacouss

Max Average Tweet Length Users: sonsuztrk, nevsegirli, Dua_Vakti07, ebrubosluk, TevhideAlyanak

ALSO tested to few outputs to confirm if the min average tweet length is correct. For one user tweet text is shown as ‘U0001f629’ which is nothing but Weary smiley face as shown in link. Hence users who tweeted single tweet as smileys are all showed as bottom 5 min avg tweet length.
