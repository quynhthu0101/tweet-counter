from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import window
import socket

# Tạo SparkContext và StreamingContext
sc = SparkContext("local[2]", "TweetCountDemo")
ssc = StreamingContext(sc, 5)  # batch interval 5 giây

ssc.checkpoint("/tmp/spark-checkpoint")

# Kết nối đến socket giả lập tweet
tweets = ssc.socketTextStream("localhost", 9999)

# Tách tweet ra hashtag
hashtags = tweets.flatMap(lambda tweet: [tag for tag in tweet.split() if tag.startswith("#")])

# Tạo cặp (hashtag, 1)
hashtag_pairs = hashtags.map(lambda h: (h.lower(), 1))

# Đếm hashtag trong cửa sổ 30s, cập nhật mỗi 10s
hashtag_counts = hashtag_pairs.reduceByKeyAndWindow(lambda a,b: a+b, lambda a,b: a-b, 30, 10)

hashtag_counts.pprint()

ssc.start()
ssc.awaitTermination()