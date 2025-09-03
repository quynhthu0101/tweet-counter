from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Tạo SparkContext và StreamingContext
sc = SparkContext("local[2]", "HashtagCountApp")
ssc = StreamingContext(sc, 5)  # batch interval = 5s

# Cho phép checkpoint để hỗ trợ window operations
ssc.checkpoint("checkpoint_dir")

# Kết nối tới ncat (tweet giả lập được gửi qua socket)
tweets = ssc.socketTextStream("localhost", 9999)

# Lấy hashtag từ mỗi tweet
hashtags = ...

# Đếm hashtag bằng reduceByKeyAndWindow
hashtag_counts = hashtags.map(...) \
    .reduceByKeyAndWindow(lambda a, b: ...,
                          lambda a, b: ...,
                          windowDuration=...,  # 45s window
                          slideDuration=...)   # update mỗi 15s

# Sắp xếp theo số lượng giảm dần
sorted_counts = ...

# In ra top 5 hashtag
sorted_counts.pprint(5)

# Chạy
ssc.start()
ssc.awaitTermination()
