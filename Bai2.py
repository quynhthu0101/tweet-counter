from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Tạo SparkContext và StreamingContext
sc = SparkContext("local[2]", "FilterTweetWindowApp")
ssc = StreamingContext(sc, 5)  # batch interval = 5s

# Kết nối tới ncat (tweet giả lập được gửi qua socket)
lines = ssc.socketTextStream("localhost", 9999)

# Danh sách hashtag cần lọc
target_hashtags = {"#ai", "#bigdata"}

# Hàm kiểm tra tweet có chứa hashtag được chỉ định
def contains_target_hashtag(tweet):
    words = tweet.split()
    hashtags = [w.lower() for w in words if w.startswith("#")]
    return any(h in target_hashtags for h in hashtags)

# Lọc tweet có hashtag chỉ định
filtered_tweets = lines.filter(contains_target_hashtag)

# Áp dụng window: gom tweet trong 30s, trượt 10s
windowed_tweets = filtered_tweets.window(windowDuration=30, slideDuration=10)

# In ra các tweet trong cửa sổ
windowed_tweets.pprint()

# Chạy streaming
ssc.start()
ssc.awaitTermination()
