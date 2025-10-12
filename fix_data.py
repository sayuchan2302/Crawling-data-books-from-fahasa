import json
import re
from fahasa_database import FahasaDatabase

def parse_description_data(description):
    """Parse thông tin từ trường description"""
    data = {
        'author': '',
        'publisher': '',
        'supplier': '',
        'publish_year': 0,
        'page_count': 0,
        'weight': 0.0,
        'dimensions': '',
        'original_price': 0.0,
        'discount_price': 0.0
    }
    
    if not description:
        return data
    
    # Tách các dòng
    lines = description.split('\n')
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Tác giả
        if 'Tác giả' in line and i + 1 < len(lines):
            data['author'] = lines[i + 1].strip()
        
        # Nhà xuất bản
        if 'NXB' in line and i + 1 < len(lines):
            data['publisher'] = lines[i + 1].strip()
        
        # Nhà cung cấp
        if 'Nhà Cung Cấp' in line and i + 1 < len(lines):
            data['supplier'] = lines[i + 1].strip()
        elif 'Tên Nhà Cung Cấp' in line and i + 1 < len(lines):
            data['supplier'] = lines[i + 1].strip()
        
        # Năm xuất bản
        if 'Năm XB' in line and i + 1 < len(lines):
            try:
                year_text = lines[i + 1].strip()
                data['publish_year'] = int(re.search(r'\d{4}', year_text).group()) if re.search(r'\d{4}', year_text) else 0
            except:
                pass
        
        # Số trang
        if 'Số trang' in line and i + 1 < len(lines):
            try:
                page_text = lines[i + 1].strip()
                data['page_count'] = int(re.search(r'\d+', page_text).group()) if re.search(r'\d+', page_text) else 0
            except:
                pass
        
        # Trọng lượng
        if 'Trọng lượng' in line and i + 1 < len(lines):
            try:
                weight_text = lines[i + 1].strip()
                weight_match = re.search(r'(\d+)', weight_text)
                if weight_match:
                    data['weight'] = float(weight_match.group()) / 1000  # Convert gram to kg
            except:
                pass
        
        # Kích thước
        if 'Kích Thước' in line and i + 1 < len(lines):
            data['dimensions'] = lines[i + 1].strip()
    
    return data

def fix_json_data(json_file_path):
    """Fix dữ liệu trong file JSON"""
    print(f"🔧 FIXING DỮ LIỆU JSON: {json_file_path}")
    print("=" * 50)
    
    # Đọc file JSON
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            books_data = json.load(f)
        
        print(f"📚 Đã đọc {len(books_data)} sách từ file JSON")
        
        # Fix từng sách
        fixed_count = 0
        for i, book in enumerate(books_data):
            print(f"\n📖 Đang fix sách {i+1}: {book['title']}")
            
            # Parse description
            parsed_data = parse_description_data(book['description'])
            
            # Cập nhật các trường trống
            updates = []
            
            if not book['author'] and parsed_data['author']:
                book['author'] = parsed_data['author']
                updates.append(f"Tác giả: {parsed_data['author']}")
            
            if not book['publisher'] and parsed_data['publisher']:
                book['publisher'] = parsed_data['publisher']
                updates.append(f"NXB: {parsed_data['publisher']}")
            
            if not book['supplier'] or book['supplier'] == 'Fahasa':
                if parsed_data['supplier']:
                    book['supplier'] = parsed_data['supplier']
                    updates.append(f"Nhà cung cấp: {parsed_data['supplier']}")
            
            if book['publish_year'] == 0 and parsed_data['publish_year'] > 0:
                book['publish_year'] = parsed_data['publish_year']
                updates.append(f"Năm XB: {parsed_data['publish_year']}")
            
            if book['page_count'] == 0 and parsed_data['page_count'] > 0:
                book['page_count'] = parsed_data['page_count']
                updates.append(f"Số trang: {parsed_data['page_count']}")
            
            if book['weight'] == 0.0 and parsed_data['weight'] > 0:
                book['weight'] = parsed_data['weight']
                updates.append(f"Trọng lượng: {parsed_data['weight']} kg")
            
            if not book['dimensions'] and parsed_data['dimensions']:
                book['dimensions'] = parsed_data['dimensions']
                updates.append(f"Kích thước: {parsed_data['dimensions']}")
            
            if updates:
                fixed_count += 1
                print(f"  ✅ Đã cập nhật: {', '.join(updates)}")
            else:
                print(f"  ⚠️ Không có dữ liệu để cập nhật")
        
        print(f"\n🎉 Đã fix {fixed_count}/{len(books_data)} sách")
        
        # Lưu file mới
        fixed_file = json_file_path.replace('.json', '_fixed.json')
        with open(fixed_file, 'w', encoding='utf-8') as f:
            json.dump(books_data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Đã lưu file đã fix: {fixed_file}")
        
        # Hiển thị kết quả
        print(f"\n📊 KẾT QUẢ SAU KHI FIX:")
        for i, book in enumerate(books_data, 1):
            print(f"{i}. {book['title']}")
            print(f"   - Tác giả: {book['author'] or 'Chưa có'}")
            print(f"   - NXB: {book['publisher'] or 'Chưa có'}")
            print(f"   - Năm XB: {book['publish_year'] or 'Chưa có'}")
            print(f"   - Số trang: {book['page_count'] or 'Chưa có'}")
            print(f"   - Trọng lượng: {book['weight']} kg" if book['weight'] > 0 else "   - Trọng lượng: Chưa có")
            print()
        
        return books_data, fixed_file
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return None, None

def update_database_with_fixed_data(books_data):
    """Cập nhật database với dữ liệu đã fix"""
    print(f"\n🗄️ CẬP NHẬT DATABASE")
    print("=" * 30)
    
    try:
        db = FahasaDatabase()
        
        # Xóa dữ liệu cũ và thêm dữ liệu mới
        import sqlite3
        conn = sqlite3.connect('fahasa_books.db')
        cursor = conn.cursor()
        
        # Xóa các sách có URL trùng (từ lần thu thập trước)
        urls_to_delete = [book['url'] for book in books_data]
        
        for url in urls_to_delete:
            cursor.execute('DELETE FROM books WHERE url = ?', (url,))
        
        conn.commit()
        conn.close()
        
        print(f"🗑️ Đã xóa dữ liệu cũ")
        
        # Thêm dữ liệu mới
        inserted = db.insert_books(books_data)
        print(f"✅ Đã thêm {inserted} sách đã fix vào database")
        
        # Xuất Excel mới
        import time
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        excel_file = db.export_to_excel(f"fahasa_fixed_{timestamp}.xlsx")
        print(f"📊 Đã tạo file Excel mới: {excel_file}")
        
        # Thống kê
        stats = db.get_statistics()
        print(f"\n📈 THỐNG KÊ DATABASE SAU KHI FIX:")
        print(f"- Tổng số sách: {stats['total_books']}")
        print(f"- Sách có tác giả: {stats['books_with_author']}")
        print(f"- Sách có NXB: {stats['books_with_publisher']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Lỗi cập nhật database: {e}")
        return False

def fix_price_issue():
    """Giải thích và hướng dẫn fix vấn đề giá"""
    print(f"\n💰 VẤN ĐỀ VỀ GIÁ SÁCH")
    print("=" * 30)
    
    print("""
🔍 NGUYÊN NHÂN GIÁ = 0:
  • Các sách trong flashsale không hiển thị giá rõ ràng
  • Giá có thể được load bằng JavaScript
  • Cần đăng nhập hoặc có session đặc biệt
  • Selector CSS không chính xác

🛠️ CÁCH KHẮC PHỤC:
  1. Thử truy cập sách không phải flashsale
  2. Cải thiện selector CSS cho giá
  3. Thêm delay để chờ JavaScript load
  4. Sử dụng API nếu có
  5. Thu thập từ trang danh mục thay vì chi tiết

🎯 THỰC HIỆN:
  • Chạy script vào giờ khác (không flashsale)
  • Thử URL khác: /sach-trong-nuoc, /van-hoc
  • Kiểm tra DevTools để tìm selector đúng
""")

def main():
    """Hàm chính"""
    print("🔧 FIX DỮ LIỆU FAHASA")
    print("=" * 40)
    
    # File JSON cần fix
    json_file = "fahasa_limited_20251012_183401.json"
    
    if not os.path.exists(json_file):
        print(f"❌ Không tìm thấy file {json_file}")
        return
    
    # Fix dữ liệu JSON
    fixed_books, fixed_file = fix_json_data(json_file)
    
    if fixed_books:
        # Cập nhật database
        success = update_database_with_fixed_data(fixed_books)
        
        if success:
            print(f"\n✅ HOÀN THÀNH FIX DỮ LIỆU!")
            print(f"📁 File đã fix: {fixed_file}")
            print(f"🗄️ Database đã được cập nhật")
        
        # Giải thích vấn đề giá
        fix_price_issue()
    
    print(f"\n🚀 TIẾP THEO:")
    print("1. Thử chạy script thu thập không flashsale")
    print("2. Cải thiện selector trong fahasa_limited_scraper.py")
    print("3. Thu thập từ các trang danh mục khác")

if __name__ == "__main__":
    import os
    main()