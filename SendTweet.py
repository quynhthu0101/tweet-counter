# =============================================================================
# TWEET SERVER CHO SPARK DSTREAMS DEMO
# Mục đích: Gửi tweet ngẫu nhiên qua socket TCP để Spark DStreams xử lý
# Cách hoạt động: Đọc file sample_tweets.txt → Thêm timestamp → Gửi cho Spark
# =============================================================================

import socket
import time
import random
import threading
from pathlib import Path
from datetime import datetime

# Cấu hình server
HOST = "127.0.0.1"     # Địa chỉ localhost
PORT = 9999            # Cổng để Spark kết nối
INTERVAL_SEC = 2       # Gửi tweet mỗi 2 giây

# Đọc tất cả tweet mẫu từ file
TWEETS = Path("sample_tweets_with_hashtags.txt").read_text(encoding="utf-8", errors="ignore").splitlines()
print(f"Đã đọc {len(TWEETS)} dòng tweet mẫu từ sample_tweets.txt")

def handle_client(conn, addr):
    """Xử lý khi Spark kết nối: gửi tweet liên tục"""
    print(f"Spark DStreams connected from {addr}")
    print(f"Bắt đầu streaming tweets mỗi {INTERVAL_SEC}s...")
    
    try:
        count = 0
        while True:
            # Chọn 1 tweet ngẫu nhiên
            original_tweet = random.choice(TWEETS)
            
            # Thêm timestamp hiện tại
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = f"{timestamp} - {original_tweet}"
            
            # Gửi cho Spark
            conn.sendall((msg + "\n").encode("utf-8"))
            
            # In ra console để kiểm tra
            count += 1
            print(f"[{count}] {msg}")
            
            # Dừng 2 giây trước khi gửi tiếp
            time.sleep(INTERVAL_SEC)
    except (BrokenPipeError, ConnectionResetError):
        print(f"Spark client {addr} disconnected.")
    finally:
        conn.close()
        print(f"Connection to {addr} closed.")

def main():
    """Khởi tạo server và lắng nghe kết nối từ Spark"""
    print("TWEET SERVER FOR SPARK DSTREAMS DEMO")
    print("=" * 40)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(1)
        
        print(f"Tweet server listening on {HOST}:{PORT}")
        print("Waiting for Spark DStreams connection...")
        print("Run your Spark Streaming job to connect")
        
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()
