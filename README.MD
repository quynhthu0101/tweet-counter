# TweetCountDemo - Spark Streaming với Socket

Ứng dụng này minh họa cách sử dụng **Spark Streaming** để đếm số lượng "tweet" (dòng text) trong một **cửa sổ trượt** (sliding window) từ luồng dữ liệu nhận qua **socket TCP** (sử dụng `netcat`).

---

## 1. Kiểm tra công cụ cần thiết

Trước khi chạy chương trình, hãy kiểm tra các công cụ sau:

# Kiểm tra Java
java -version

# Kiểm tra Spark
spark-shell --version

# Kiểm tra Python
python --version

# Kiểm tra PySpark
python -m pip show pyspark

# Kiểm tra netcat
nc -h


### 2. Các bước chạy chương trình

# Mở terminal 1 → chạy server netcat để phát dữ liệu:

nc -lk 9999


# Sau đó gõ text bất kỳ (giả lập tweet), ví dụ:

hello world
spark streaming demo
another tweet


# Mở terminal 2 → chạy chương trình Spark Streaming:

spark-submit TweetCountDemo.py


# Quan sát kết quả in ra mỗi 10 giây:

----- Time: 2025-09-02 19:00:10 -----
5
----- Time: 2025-09-02 19:00:20 -----
12

### 3. Giải thích ngắn gọn

Batch interval: 5 giây → gom dữ liệu mỗi 5 giây.

Window length: 30 giây → đếm số tweet trong 30 giây gần nhất.

Slide interval: 10 giây → cập nhật kết quả sau mỗi 10 giây.

### 4. Lỗi thường gặp và cách fix