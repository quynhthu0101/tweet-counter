from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# 1. Tạo SparkContext
sc = SparkContext("local[2]", "TweetCountDemo")
ssc = StreamingContext(sc, 5)   # Batch interval = 5s

ssc.checkpoint("/tmp/spark-checkpoint")

# 2. Giả lập luồng dữ liệu tweet (socket text stream)
lines = ssc.socketTextStream("localhost", 9999)

# 3. Đếm số lượng tweet trong cửa sổ trượt
# windowLength = 30s, slideInterval = 10s
tweet_count = lines.countByWindow(windowDuration=30, slideDuration=10)

# 4. In kết quả ra màn hình
tweet_count.pprint()

# 5. Start streaming
ssc.start()
ssc.awaitTermination()