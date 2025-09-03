from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Tạo SparkContext và StreamingContext
sc = SparkContext("local[2]", "HashtagCountApp")
ssc = StreamingContext(sc, 5)  # batch interval = 5s

# Cho phép checkpoint để hỗ trợ window operations
ssc.checkpoint("checkpoint_dir")

# Kết nối tới ncat (tweet giả lập được gửi qua socket)
lines = ssc.socketTextStream("localhost", 9999)

# Lấy hashtag từ mỗi tweet
hashtags = lines.flatMap(lambda line: [word for word in line.split() if word.startswith("#")])

# Đếm hashtag bằng reduceByKeyAndWindow
hashtag_counts = hashtags.map(lambda tag: (tag, 1)) \
    .reduceByKeyAndWindow(lambda a, b: a + b,
                          lambda a, b: a - b,
                          windowDuration=45,  # 45s window
                          slideDuration=15)   # update mỗi 15s

# Sắp xếp theo số lượng giảm dần
sorted_counts = hashtag_counts.transform(lambda rdd: rdd.sortBy(lambda x: -x[1]))

# In ra top 5 hashtag
sorted_counts.pprint(5)

# Chạy
ssc.start()
ssc.awaitTermination()
