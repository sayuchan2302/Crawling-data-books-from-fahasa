# HƯỚNG DẪN THU THẬP DỮ LIỆU SÁCH TỪ FAHASA.COM

## 🎯 Tổng quan dự án

Dự án này giúp bạn thu thập dữ liệu tất cả sách từ website Fahasa.com một cách tự động và hiệu quả.

## 📁 Cấu trúc dự án

```
d:\script\
├── scipt.py                    # Script chính (requests + BeautifulSoup)
├── fahasa_selenium_scraper.py  # Script nâng cao (Selenium)
├── test_connection.py          # Test kết nối đơn giản
├── advanced_test.py           # Test nâng cao
├── selenium_example.py        # Ví dụ Selenium
├── requirements.txt           # Danh sách thư viện
└── README_FULL.md            # Hướng dẫn này
```

## 🚀 Cách sử dụng

### Phương án 1: Selenium (Khuyến nghị) ⭐

```bash
# Chạy script Selenium
python fahasa_selenium_scraper.py
```

**Ưu điểm:**
- ✅ Bypass được hệ thống chống bot
- ✅ Mô phỏng hành vi người dùng thật
- ✅ Xử lý được JavaScript
- ✅ Tự động xử lý CloudFlare

**Nhược điểm:**
- ⚠️ Chậm hơn requests
- ⚠️ Tốn tài nguyên hơn

### Phương án 2: Requests + BeautifulSoup

```bash
# Chạy script cơ bản
python scipt.py
```

**Ưu điểm:**
- ✅ Nhanh
- ✅ Ít tài nguyên

**Nhược điểm:**
- ❌ Dễ bị chặn bởi CloudFlare
- ❌ Không xử lý được JavaScript

## 📊 Dữ liệu thu thập được

Mỗi cuốn sách sẽ có các thông tin:

```json
{
  "url": "Link sản phẩm",
  "title": "Tên sách",
  "author": "Tác giả",
  "price": "Giá hiện tại",
  "original_price": "Giá gốc",
  "publisher": "Nhà xuất bản",
  "description": "Mô tả sách",
  "image_url": "Link hình ảnh",
  "category": "Danh mục",
  "rating": "Đánh giá",
  "availability": "Tình trạng kho"
}
```

## 🔧 Tùy chỉnh

### Điều chỉnh số lượng

```python
# Trong file fahasa_selenium_scraper.py
scraper.run_scraping(
    max_categories=3,        # Số danh mục tối đa
    max_books_per_category=10  # Số sách mỗi danh mục
)
```

### Điều chỉnh delay

```python
# Tăng delay để tránh bị chặn
time.sleep(random.uniform(5, 10))  # 5-10 giây
```

### Thêm proxy

```python
chrome_options.add_argument('--proxy-server=http://proxy-server:port')
```

## 📈 Chiến lược thu thập hiệu quả

### 1. Chia nhỏ công việc
- Thu thập từng danh mục một
- Giới hạn số sách mỗi lần chạy
- Chạy vào giờ ít người dùng (2-6h sáng)

### 2. Tránh bị chặn
- Sử dụng delay ngẫu nhiên
- Thay đổi User-Agent
- Sử dụng proxy rotation
- Mô phỏng hành vi người dùng

### 3. Xử lý lỗi
- Retry mechanism
- Lưu checkpoint
- Log chi tiết

## 🛡️ Tuân thủ đạo đức

### ✅ Nên làm:
- Đọc và tuân thủ robots.txt
- Giới hạn tốc độ request
- Sử dụng dữ liệu có trách nhiệm
- Liên hệ website nếu cần thu thập lớn

### ❌ Không nên:
- Spam request liên tục
- Làm quá tải server
- Bán dữ liệu trái phép
- Vi phạm bản quyền

## 🐛 Xử lý lỗi thường gặp

### Lỗi 403 Forbidden
```
Nguyên nhân: Bị chặn bởi CloudFlare
Giải pháp: Sử dụng Selenium + delay cao
```

### ChromeDriver error
```bash
# Cài đặt webdriver-manager
pip install webdriver-manager
```

### Encoding error
```python
# Lưu file với encoding UTF-8
df.to_csv(filename, encoding='utf-8-sig')
```

## 🔄 Nâng cấp script

### Thêm database
```python
import sqlite3

conn = sqlite3.connect('fahasa_books.db')
df.to_sql('books', conn, if_exists='append')
```

### Thêm GUI
```python
import tkinter as tk
# Tạo giao diện đồ họa
```

### Thêm scheduling
```python
import schedule

schedule.every().day.at("02:00").do(scrape_books)
```

## 📞 Liên hệ và hỗ trợ

Nếu gặp vấn đề:
1. Kiểm tra kết nối internet
2. Cập nhật ChromeDriver
3. Điều chỉnh delay time
4. Thử chạy không headless để debug

## 📚 Tài liệu tham khảo

- [Selenium Documentation](https://selenium-python.readthedocs.io/)
- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

## 🏆 Kết quả mong đợi

Sau khi hoàn thành, bạn sẽ có:
- File CSV chứa dữ liệu sách
- File JSON backup
- Thống kê chi tiết
- Kinh nghiệm web scraping

---

**Chúc bạn thu thập dữ liệu thành công! 🎉**