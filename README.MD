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
ncat -h
```

---

## 2. Các bước chạy chương trình

### Bước 1: Mở terminal 1 → chạy server netcat để phát dữ liệu
```bash
ncat -lk 9999
```

Sau đó gõ text bất kỳ (giả lập tweet), ví dụ:
```
hello world
spark streaming demo
another tweet
```

### Bước 2: Mở terminal 2 → chạy chương trình Spark Streaming
c
spark-submit TweetCountDemo.py
```

### Bước 3: Quan sát kết quả in ra mỗi 10 giây
Ví dụ:
```
----- Time: 2025-09-02 19:00:10 -----
5
----- Time: 2025-09-02 19:00:20 -----
12
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
| `java.lang.ClassNotFoundException` | Thiếu cài đặt Java hoặc biến môi trường `JAVA_HOME` | Cài Java và cấu hình `JAVA_HOME` |
| `pyspark not found` | Chưa cài PySpark trong Python | Cài bằng: `pip install pyspark` |
| `nc: command not found` | Hệ thống chưa có netcat | Cài bằng: `sudo apt install netcat` (Linux) hoặc dùng bản thay thế trên Windows (ncat trong Nmap) |
| Chương trình không in kết quả | Netcat chưa gửi dữ liệu hoặc chưa đúng port | Kiểm tra lại lệnh `nc -lk 9999` và chắc chắn đã nhập text vào terminal 1 |
| Spark log quá nhiều | Spark mặc định log WARN/INFO nhiều | Thêm `sc.setLogLevel("ERROR")` trong code để giảm log |
| `Exception in thread "streaming-start" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'` | Thiếu `hadoop.dll` trong `C:\hadoop\bin` và `C:\Windows\System32` | - Tải file từ [winutils](https://github.com/steveloughran/winutils) và copy vào 2 folder trên.<br>- Cấu hình biến môi trường (nếu chưa có):<br>```bash<br>set HADOOP_HOME=c:/hadoop<br>set PATH=%PATH%;%HADOOP_HOME%/bin;<br>``` |
