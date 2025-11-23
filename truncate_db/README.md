# 🗑️ Database Truncate Scripts

Thư mục này chứa các scripts để truncate (xóa tất cả dữ liệu) từ các databases trong hệ thống Fahasa Data Warehouse.

## 📋 Danh sách Scripts

### 1. `truncate_staging.py`
**Mục đích**: Truncate chuyên biệt cho fahasa_staging database
- ✅ Kiểm tra dữ liệu trước khi xóa
- 💾 Tùy chọn backup 
- 🔧 Tự động tắt/bật Foreign Key Checks
- 🔄 Reset AUTO_INCREMENT counters
- ✅ Verify kết quả sau khi truncate

**Sử dụng**:
```bash
python truncate_staging.py
```

### 2. `universal_truncate.py` 
**Mục đích**: Script tổng quát có thể truncate bất kỳ database nào
- 🎯 Hỗ trợ nhiều database
- 🚫 Exclude/Include tables cụ thể
- ⚡ Command line arguments
- 📊 Báo cáo chi tiết

**Sử dụng**:
```bash
# Interactive mode
python universal_truncate.py

# Command line mode
python universal_truncate.py fahasa_staging
python universal_truncate.py fahasa_dw --exclude dim_books author_dim
python universal_truncate.py fahasa_datamart --include mart_temp_table
python universal_truncate.py fahasa_staging --confirm  # Bỏ qua xác nhận
```

### 3. `quick_truncate.py`
**Mục đích**: Truncate nhanh với menu đơn giản
- ⚡ Truncate nhanh chóng
- 📋 Menu lựa chọn database
- 🎯 Phù hợp cho việc dọn dẹp hàng ngày

**Sử dụng**:
```bash
python quick_truncate.py
```

## ⚠️ Cảnh báo quan trọng

1. **KHÔNG THỂ HOÀN TÁC**: Tất cả dữ liệu sẽ bị xóa vĩnh viễn
2. **BACKUP**: Luôn backup trước khi truncate database production
3. **FOREIGN KEYS**: Scripts tự động xử lý Foreign Key constraints
4. **AUTO_INCREMENT**: Tự động reset về 1 sau khi truncate

## 🎯 Các trường hợp sử dụng phổ biến

### Trước khi chạy ETL mới
```bash
python truncate_staging.py
# Sau đó chạy ETL để load dữ liệu mới
```

### Dọn dẹp datamart
```bash
python quick_truncate.py
# Chọn option 3 (fahasa_datamart)
```

### Reset development environment
```bash
python universal_truncate.py fahasa_dw --exclude dim_categories dim_time
# Giữ lại master data, xóa fact tables
```

### Dọn dẹp tables tạm thời
```bash
python universal_truncate.py fahasa_staging --include temp_table staging_errors
# Chỉ xóa các tables cụ thể
```

## 📊 Database Structure

```
fahasa_staging      - Raw data từ crawling/scraping
fahasa_dw          - Data warehouse (facts, dimensions)  
fahasa_datamart    - Business intelligence layer
fahasa_control     - ETL logs và control tables
```

## 🔧 Database Connection

Tất cả scripts sử dụng connection settings:
```python
host='localhost'
user='root' 
password='123456'
charset='utf8mb4'
```

Nếu cần thay đổi, edit trong từng script tại hàm `connect_db()`.

## ✅ Best Practices

1. **Luôn kiểm tra** dữ liệu trước khi truncate
2. **Backup production** data trước khi thao tác
3. **Test trên development** environment trước  
4. **Document** các thay đổi quan trọng
5. **Coordinate** với team trước khi truncate shared databases

## 🆘 Troubleshooting

### Lỗi Foreign Key Constraint
Scripts tự động tắt FK checks, nhưng nếu gặp lỗi:
```sql
SET FOREIGN_KEY_CHECKS = 0;
-- Run truncate commands
SET FOREIGN_KEY_CHECKS = 1;
```

### Lỗi kết nối database
- Kiểm tra MySQL service đang chạy
- Verify username/password
- Confirm database tồn tại

### Table không thể truncate
- Check permissions
- Verify table không bị lock
- Try DELETE FROM table instead of TRUNCATE

---

**📞 Support**: Contact Data Team nếu cần hỗ trợ thêm!