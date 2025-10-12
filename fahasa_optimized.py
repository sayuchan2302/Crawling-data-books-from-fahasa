from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from fahasa_database import FahasaDatabase
import time
import random
import json
import re
import os

def extract_price_smart(price_text):
    """Trích xuất giá thông minh"""
    try:
        if not price_text:
            return 0.0
        
        # Loại bỏ ký tự không cần thiết
        clean_text = re.sub(r'[^\d,.]', '', str(price_text))
        clean_text = clean_text.replace(',', '').replace('.', '')
        
        if clean_text and clean_text.isdigit():
            price = float(clean_text)
            # Điều chỉnh giá nếu cần
            if price < 1000:
                price *= 1000
            return price if price >= 1000 else 0.0
        return 0.0
    except:
        return 0.0

def scrape_fahasa_optimized():
    """Thu thập Fahasa tối ưu - 3 sách có giá"""
    print("🚀 THU THẬP FAHASA TỐI ƯU - 3 SÁCH")
    print("=" * 50)
    
    # Chrome setup
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(20)
    
    books_data = []
    
    try:
        print("🌐 Truy cập Fahasa...")
        driver.get('https://www.fahasa.com/sach-trong-nuoc.html')
        time.sleep(3)
        
        print("🔍 Tìm sản phẩm...")
        products = driver.find_elements(By.CSS_SELECTOR, '.item-inner')[:5]  # Chỉ lấy 5 sản phẩm đầu
        print(f"📚 Tìm thấy {len(products)} sản phẩm")
        
        # Lấy URL sản phẩm
        product_urls = []
        for product in products:
            try:
                link = product.find_element(By.TAG_NAME, 'a')
                url = link.get_attribute('href')
                if url and 'flashsale' not in url.lower():  # Tránh flashsale
                    product_urls.append(url)
                    if len(product_urls) >= 3:  # Chỉ cần 3 sách
                        break
            except:
                continue
        
        print(f"🔗 Sẽ thu thập {len(product_urls)} sách")
        
        # Thu thập từng sách
        for i, url in enumerate(product_urls, 1):
            print(f"\n📖 Sách {i}/3:")
            print(f"    URL: {url}")
            
            try:
                driver.get(url)
                time.sleep(3)
                
                # Khởi tạo dữ liệu
                book = {
                    'title': '',
                    'author': '',
                    'publisher': '',
                    'supplier': '',
                    'category_1': 'Sách trong nước',
                    'category_2': '',
                    'category_3': '',
                    'original_price': 0.0,
                    'discount_price': 0.0,
                    'discount_percent': 0.0,
                    'rating': 0.0,
                    'rating_count': 0,
                    'sold_count': '',
                    'sold_count_numeric': 0,
                    'publish_year': 0,
                    'language': 'Tiếng Việt',
                    'page_count': 0,
                    'weight': 0.0,
                    'dimensions': '',
                    'url': url,
                    'url_img': '',
                    'description': ''
                }
                
                # Lấy title
                try:
                    title_elem = driver.find_element(By.TAG_NAME, 'h1')
                    book['title'] = title_elem.text.strip()
                    print(f"    ✅ Tên: {book['title']}")
                except:
                    print("    ❌ Không lấy được title")
                    continue
                
                # Lấy giá - thử nhiều cách
                price_found = False
                
                # Cách 1: Tìm trong element có chữ "đ" hoặc "VND"
                all_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'đ') or contains(text(), 'VND')]")
                for elem in all_elements:
                    text = elem.text.strip()
                    if re.search(r'\d{2,}', text):  # Có ít nhất 2 chữ số
                        price = extract_price_smart(text)
                        if price > 0:
                            book['discount_price'] = price
                            book['original_price'] = price
                            print(f"    💰 Giá: {price:,.0f} VNĐ")
                            price_found = True
                            break
                
                # Cách 2: Tìm trong data attributes
                if not price_found:
                    try:
                        price_elements = driver.find_elements(By.CSS_SELECTOR, '[data-price], .price, .current-price')
                        for elem in price_elements:
                            # Thử data-price attribute
                            data_price = elem.get_attribute('data-price')
                            if data_price:
                                price = extract_price_smart(data_price)
                                if price > 0:
                                    book['discount_price'] = price
                                    book['original_price'] = price
                                    print(f"    💰 Giá (data): {price:,.0f} VNĐ")
                                    price_found = True
                                    break
                            
                            # Thử text content
                            text = elem.text.strip()
                            if text and ('đ' in text or 'vnd' in text.lower()):
                                price = extract_price_smart(text)
                                if price > 0:
                                    book['discount_price'] = price
                                    book['original_price'] = price
                                    print(f"    💰 Giá (text): {price:,.0f} VNĐ")
                                    price_found = True
                                    break
                    except:
                        pass
                
                # Cách 3: Tìm trong JavaScript/source
                if not price_found:
                    try:
                        page_source = driver.page_source
                        price_patterns = [
                            r'"price"[:\s]*"?(\d+)"?',
                            r'"current_price"[:\s]*"?(\d+)"?',
                            r'data-price="(\d+)"',
                            r'price["\']?\s*:\s*["\']?(\d+)'
                        ]
                        
                        for pattern in price_patterns:
                            matches = re.findall(pattern, page_source, re.IGNORECASE)
                            for match in matches:
                                price = extract_price_smart(match)
                                if price > 1000:  # Giá hợp lý
                                    book['discount_price'] = price
                                    book['original_price'] = price
                                    print(f"    💰 Giá (source): {price:,.0f} VNĐ")
                                    price_found = True
                                    break
                            if price_found:
                                break
                    except:
                        pass
                
                if not price_found:
                    print("    ⚠️ Không tìm thấy giá")
                
                # Lấy breadcrumb
                try:
                    breadcrumb = driver.find_element(By.CSS_SELECTOR, '.breadcrumb, .breadcrumbs')
                    links = breadcrumb.find_elements(By.TAG_NAME, 'a')
                    categories = [link.text.strip() for link in links if link.text.strip() and link.text.strip().lower() not in ['trang chủ', 'home']]
                    
                    if len(categories) >= 2:
                        book['category_2'] = categories[1]
                    if len(categories) >= 3:
                        book['category_3'] = categories[2]
                        
                    print(f"    📂 Danh mục: {' > '.join(categories[:3])}")
                except:
                    pass
                
                # Lấy thông tin chi tiết
                try:
                    # Tìm tất cả text chứa thông tin sách
                    page_text = driver.page_source
                    
                    # Tác giả
                    author_match = re.search(r'Tác giả[:\s]*</?\w*>?\s*([^<\n]+)', page_text, re.IGNORECASE)
                    if author_match:
                        book['author'] = author_match.group(1).strip()
                        print(f"    👤 Tác giả: {book['author']}")
                    
                    # NXB
                    pub_match = re.search(r'NXB[:\s]*</?\w*>?\s*([^<\n]+)', page_text, re.IGNORECASE)
                    if pub_match:
                        book['publisher'] = pub_match.group(1).strip()
                        print(f"    🏢 NXB: {book['publisher']}")
                    
                    # Năm xuất bản
                    year_match = re.search(r'Năm XB[:\s]*</?\w*>?\s*(\d{4})', page_text, re.IGNORECASE)
                    if year_match:
                        book['publish_year'] = int(year_match.group(1))
                        print(f"    📅 Năm XB: {book['publish_year']}")
                
                except:
                    pass
                
                # Thêm vào danh sách
                books_data.append(book)
                print(f"    ✅ Đã thu thập thành công")
                
            except Exception as e:
                print(f"    ❌ Lỗi: {e}")
                continue
        
        print(f"\n🎉 Hoàn thành! Thu thập được {len(books_data)} sách")
        
        # Thống kê
        books_with_price = [b for b in books_data if b['discount_price'] > 0]
        print(f"📊 Có {len(books_with_price)}/{len(books_data)} sách có giá")
        
        # Lưu dữ liệu
        if books_data:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            
            # JSON - Gộp vào file chung
            json_file = "fahasa_all_books.json"
            try:
                # Đọc dữ liệu cũ nếu có
                if os.path.exists(json_file):
                    with open(json_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                else:
                    existing_data = []
                
                # Gộp dữ liệu mới (chỉ thêm sách chưa có)
                existing_titles = {book['title'] for book in existing_data}
                new_books = [book for book in books_data if book['title'] not in existing_titles]
                
                if new_books:
                    existing_data.extend(new_books)
                    with open(json_file, 'w', encoding='utf-8') as f:
                        json.dump(existing_data, f, ensure_ascii=False, indent=2)
                    print(f"💾 Đã cập nhật JSON: {json_file} (+{len(new_books)} sách mới)")
                else:
                    print(f"💾 JSON: {json_file} (không có sách mới)")
                    
            except Exception as e:
                print(f"❌ Lỗi JSON: {e}")
            
            # Database
            try:
                db = FahasaDatabase()
                inserted = db.insert_books(books_data)
                print(f"🗄️ Đã lưu {inserted} sách vào database")
                
                # Excel - File chung từ database
                excel_file = "fahasa_all_books.xlsx"
                excel_file = db.export_to_excel(excel_file)
                print(f"📊 Đã cập nhật Excel: {excel_file}")
                
            except Exception as e:
                print(f"❌ Lỗi database: {e}")
            
            # Hiển thị kết quả
            print(f"\n📚 KẾT QUẢ:")
            for i, book in enumerate(books_data, 1):
                price_text = f"{book['discount_price']:,.0f} VNĐ" if book['discount_price'] > 0 else "Chưa có giá"
                print(f"{i}. {book['title']}")
                print(f"   - Tác giả: {book['author'] or 'Chưa có'}")
                print(f"   - Giá: {price_text}")
                print(f"   - NXB: {book['publisher'] or 'Chưa có'}")
                print()
        
    except Exception as e:
        print(f"❌ Lỗi chung: {e}")
    
    finally:
        input("⏸️ Nhấn Enter để đóng browser...")
        driver.quit()
        print("🔚 Đã đóng browser")

if __name__ == "__main__":
    scrape_fahasa_optimized()