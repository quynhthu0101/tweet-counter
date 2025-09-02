# TweetCountDemo - Spark Streaming với Socket

Ứng dụng này minh họa cách sử dụng **Spark Streaming** để đếm số lượng "tweet" (dòng text) trong một **cửa sổ trượt** (sliding window) từ luồng dữ liệu nhận qua **socket TCP** (sử dụng `netcat`).

---

## 1. Kiểm tra công cụ cần thiết

Trước khi chạy chương trình, hãy kiểm tra các công cụ sau:

```bash
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
```

---

## 2. Các bước chạy chương trình

### Bước 1: Mở terminal 1 → chạy server netcat để phát dữ liệu
```bash
nc -lk 9999
```

Sau đó gõ text bất kỳ (giả lập tweet), ví dụ:
```
hello world
spark streaming demo
another tweet
```

### Bước 2: Mở terminal 2 → chạy chương trình Spark Streaming
```bash
spark-submit TweetCountDemo.py
```

### Bước 3: Quan sát kết quả in ra mỗi 5 giây
Ví dụ:
```
----- Time: 2025-09-02 19:00:10 -----
3
----- Time: 2025-09-02 19:00:15 -----
3
```

---

## 3. Giải thích ngắn gọn

- **Batch interval**: 5 giây → gom dữ liệu mỗi 5 giây.  
- **Window length**: 30 giây → đếm số tweet trong 30 giây gần nhất.  
- **Slide interval**: 10 giây → cập nhật kết quả sau mỗi 10 giây.  

---

## 4. Lỗi thường gặp và cách fix

| Lỗi | Nguyên nhân | Cách fix |
|-----|-------------|----------|
| `Exception in thread "streaming-start" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'` | Thiếu hadoop.dll trong C:\hadoop\bin và C:\Windows\System32 | Tải và đặt vào 2 folder |

---
