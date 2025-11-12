from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import json
import pandas as pd
from datetime import datetime
import json
import re
import os
import psycopg2
from insert_staging_book import insert_book_staging

def extract_price_smart(price_text):
    
    try:
        if not price_text:
            return 0.0
        
        clean_text = re.sub(r'[^\d,.]', '', str(price_text))
        clean_text = clean_text.replace(',', '').replace('.', '')
        
        if clean_text and clean_text.isdigit():
            price = float(clean_text)
            if price < 1000:
                price *= 1000
            return price if price >= 1000 else 0.0
        return 0.0
    except:
        return 0.0

def get_book_details(driver, url):
    
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "h1"))
        )
        
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
            'time_collect': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        try:
            year_elem = driver.find_element(By.XPATH, "//th[contains(text(), 'Năm XB')]/following-sibling::td")
            divs = year_elem.find_elements(By.TAG_NAME, 'div')
            year_text = ''
            if divs and divs[0].text.strip():
                year_text = divs[0].text.strip()
            elif year_elem.text.strip():
                year_text = year_elem.text.strip()
            if year_text.isdigit():
                book['publish_year'] = int(year_text)
        except:
            pass
        
        try:
            weight_elem = driver.find_element(By.XPATH, "//th[contains(text(), 'Trọng lượng')]/following-sibling::td")
            divs = weight_elem.find_elements(By.TAG_NAME, 'div')
            weight_text = ''
            if divs and divs[0].text.strip():
                weight_text = divs[0].text.strip()
            elif weight_elem.text.strip():
                weight_text = weight_elem.text.strip()
            weight_val = re.sub(r'[^\d.]', '', weight_text)
            if weight_val:
                weight_gram = float(weight_val)
                if weight_gram > 10:
                    book['weight'] = round(weight_gram / 1000, 3)
                else:
                    book['weight'] = weight_gram
        except:
            pass
        
        try:
            dim_elem = driver.find_element(By.XPATH, "//th[contains(text(), 'Kích Thước Bao Bì')]/following-sibling::td")
            divs = dim_elem.find_elements(By.TAG_NAME, 'div')
            if divs and divs[0].text.strip():
                book['dimensions'] = divs[0].text.strip()
            elif dim_elem.text.strip():
                book['dimensions'] = dim_elem.text.strip()
        except:
            pass
        
        try:
            page_count_elem = driver.find_element(By.XPATH, "//th[contains(text(), 'Số trang')]/following-sibling::td")
            divs = page_count_elem.find_elements(By.TAG_NAME, 'div')
            if divs and divs[0].text.strip().isdigit():
                book['page_count'] = int(divs[0].text.strip())
            elif page_count_elem.text.strip().isdigit():
                book['page_count'] = int(page_count_elem.text.strip())
        except:
            pass
        
        
        try:
            breadcrumbs = driver.find_elements(By.CSS_SELECTOR, '.breadcrumb li a')
            if len(breadcrumbs) >= 2:
                book['category_2'] = breadcrumbs[1].text.strip() if len(breadcrumbs) > 1 else ''
                if len(breadcrumbs) == 4:
                    # Ghép mục 3 và 4
                    book['category_3'] = f"{breadcrumbs[2].text.strip()} - {breadcrumbs[3].text.strip()}"
                elif len(breadcrumbs) > 2:
                    book['category_3'] = breadcrumbs[2].text.strip()
        except:
            pass
        
        
        try:
            title_elem = driver.find_element(By.TAG_NAME, 'h1')
            book['title'] = title_elem.text.strip()
        except:
            return None
        
        
        price_found = False
        
        
        try:
            price_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'đ')]")
            for elem in price_elements:
                text = elem.text.strip()
                if re.search(r'\d{2,}', text):  # Có ít nhất 2 chữ số
                    price = extract_price_smart(text)
                    if price > 0:
                        book['discount_price'] = price
                        book['original_price'] = price
                        price_found = True
                        break
        except:
            pass
        
        
        if not price_found:
            selectors = [
                '.price-original .price',
                '.price .current-price', 
                '.product-price .price',
                '[data-price]',
                '.price-box .price'
            ]
            
            for selector in selectors:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, selector)
                    text = elem.text.strip() or elem.get_attribute('data-price') or ''
                    price = extract_price_smart(text)
                    if price > 0:
                        book['discount_price'] = price
                        book['original_price'] = price
                        price_found = True
                        break
                except:
                    continue
        
        
        try:
            # Giá hiện tại
            price_elem = driver.find_element(By.CSS_SELECTOR, 'span.price[id^="product-price-"]')
            price_text = price_elem.text.strip()
            price_val = re.sub(r'[^\d.]', '', price_text)
            if price_val:
                book['discount_price'] = float(price_val.replace('.', ''))
        except:
            pass
        try:
            # Giá gốc
            old_price_elem = driver.find_element(By.CSS_SELECTOR, 'span.price[id^="old-price-"]')
            old_price_text = old_price_elem.text.strip()
            old_price_val = re.sub(r'[^\d.]', '', old_price_text)
            if old_price_val:
                book['original_price'] = float(old_price_val.replace('.', ''))
        except:
            pass
        try:
            # Phần trăm giảm giá
            percent_elem = driver.find_element(By.CSS_SELECTOR, 'span.discount-percent')
            percent_text = percent_elem.text.strip()
            percent_val = re.sub(r'[^\d-]', '', percent_text)
            if percent_val:
                book['discount_percent'] = float(percent_val)
        except:
            pass

        
        try:
            
            author_elem = driver.find_element(By.XPATH, "//th[contains(text(), 'Tác giả')]/following-sibling::td")
            book['author'] = author_elem.text.strip()
        except:
            pass

            
        try:
            pub_elem = driver.find_element(By.XPATH, "//th[contains(text(), 'Nhà xuất bản')]/following-sibling::td")
            book['publisher'] = pub_elem.text.strip()
        except:
            try:
                pub_div = driver.find_element(By.CSS_SELECTOR, 'div.product-view-sa-supplier')
                spans = pub_div.find_elements(By.TAG_NAME, 'span')
                if len(spans) >= 2 and 'Nhà xuất bản' in spans[0].text:
                    book['publisher'] = spans[1].text.strip()
            except:
                pass

            
        try:
            sup_div = driver.find_element(By.CSS_SELECTOR, 'div.product-view-sa-supplier')
            # Ưu tiên lấy supplier từ thẻ <a>
            a_tags = sup_div.find_elements(By.TAG_NAME, 'a')
            if a_tags:
                supplier_text = a_tags[0].text.strip()
                book['supplier'] = supplier_text
            else:
                # Nếu không có thẻ <a>, lấy text sau span
                spans = sup_div.find_elements(By.TAG_NAME, 'span')
                if len(spans) >= 2 and 'Nhà cung cấp' in spans[0].text:
                    book['supplier'] = spans[1].text.strip()
                else:
                    # fallback: lấy toàn bộ text trừ label
                    text = sup_div.text.replace('Nhà cung cấp:', '').strip()
                    book['supplier'] = text
        except:
            pass
        
        
        try:
            sup_divs = driver.find_elements(By.CSS_SELECTOR, 'div.product-view-sa-supplier')
            for div in sup_divs:
                spans = div.find_elements(By.TAG_NAME, 'span')
                if len(spans) >= 2:
                    label = spans[0].text.strip().lower()
                    value = spans[1].text.strip()
                    if 'nhà cung cấp' in label:
                        book['supplier'] = value
                    elif 'nhà xuất bản' in label:
                        book['publisher'] = value
        except:
            pass

        
        try:
            img_elem = driver.find_element(By.CSS_SELECTOR, 'img.fhs-p-img')
            img_url = img_elem.get_attribute('src')
            if not img_url or 'placeholder' in img_url:
                img_url = img_elem.get_attribute('data-src')
            book['url_img'] = img_url
        except:
            pass
        
        
        try:
            
            rating_elem = driver.find_element(By.XPATH, "//div[./span[contains(text(), '/5')]]")
            rating_text = rating_elem.text.strip()
            match = re.search(r'(\d+(?:[.,]\d+)?)(?=\s*/\s*5)', rating_text)
            if match:
                book['rating'] = float(match.group(1).replace(',', '.'))
            else:
                # Nếu không có text, thử lấy width style trong .rating
                rating_box = driver.find_element(By.CSS_SELECTOR, '.rating-box .rating')
                style = rating_box.get_attribute('style')
                width_match = re.search(r'width:\s*(\d+)%', style)
                if width_match:
                    percent = int(width_match.group(1))
                    book['rating'] = round(percent / 20, 2)  # 100% = 5.0
        except:
            pass
        
        
        try:
            sold_elem = driver.find_element(By.CSS_SELECTOR, 'div.product-view-qty-num')
            sold_text = sold_elem.text.strip()
            
            match = re.search(r'Đã bán\s*([\d.,]+)(k\+)?', sold_text, re.IGNORECASE)
            if match:
                book['sold_count'] = match.group(1) + (match.group(2) if match.group(2) else '')
                
                if match.group(2):
                    
                    num = float(match.group(1).replace(',', '.')) * 1000
                    book['sold_count_numeric'] = int(num)
                else:
                    num = match.group(1).replace('.', '').replace(',', '')
                    if num.isdigit():
                        book['sold_count_numeric'] = int(num)
        except:
            pass

        
        if price_found:
            return book
        else:
            return None
            
    except Exception as e:
        print(f"    Lỗi khi lấy chi tiết: {e}")
        return None

def scrape_fahasa_bulk(max_pages=1, books_per_page=3):
    
    print("FAHASA BULK SCRAPER - THU THẬP QUY MÔ LỚN")
    print("=" * 60)
    print(f"Mục tiêu: {max_pages} trang x {books_per_page} sách = tối đa {max_pages * books_per_page} sách")
    print("=" * 60)
    
    
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    
    driver = None
    try:
        print("Đang setup ChromeDriver...")
        
        
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e1:
            print(f"Method 1 failed: {str(e1)[:100]}...")
            
            
            try:
                driver = webdriver.Chrome(options=chrome_options)
            except Exception as e2:
                print(f"Method 2 failed: {str(e2)[:100]}...")
                raise Exception("Không thể khởi tạo ChromeDriver")
        
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(30)
        print("ChromeDriver setup thành công!")
        
    except Exception as e:
        print(f"Lỗi ChromeDriver: {e}")
        print("Giải pháp:")
        print("   1. Chạy: python quick_test.py (test không cần Chrome)")
        print("   2. Restart máy tính và thử lại")
        print("   3. Cập nhật Chrome browser")
        print("   4. Kiểm tra antivirus không block chromedriver")
        return
    
    books_data = []
    total_collected = 0
    
    try:
        for page in range(1, max_pages + 1):
            print(f"\nTRANG {page}/{max_pages}")
            print("-" * 40)
            
            
            url = f"https://www.fahasa.com/sach-trong-nuoc.html?order=num_orders&limit={books_per_page}&p={page}"
            print(f"Truy cập: {url}")
            
            driver.get(url)
            time.sleep(random.uniform(3, 5))  # Random delay
            
            
            try:
                products = WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.item-inner'))
                )
                print(f"Tìm thấy {len(products)} sản phẩm trong trang")
            except:
                print("Không tìm thấy sản phẩm, bỏ qua trang này")
                continue
            
            
            product_urls = []
            for product in products:
                try:
                    link = product.find_element(By.TAG_NAME, 'a')
                    url_product = link.get_attribute('href')
                    if url_product and 'flashsale' not in url_product.lower():
                        product_urls.append(url_product)
                except:
                    continue
            
            print(f"Sẽ thu thập {len(product_urls)} sách từ trang {page}")
            
            
            page_success = 0
            for i, book_url in enumerate(product_urls, 1):
                print(f"\nSách {i}/{len(product_urls)} (Trang {page}):")
                
                book_data = get_book_details(driver, book_url)
                if book_data:
                    print(f"    {book_data['title'][:50]}...")
                    print(f"    Giá: {book_data['discount_price']:,.0f} VNĐ")
                    try:
                        insert_book_staging(book_data)
                        print("    Đã insert vào staging_books (PostgreSQL)")
                    except Exception as e:
                        print(f"    Lỗi insert staging_books: {e}")
                    books_data.append(book_data)
                    total_collected += 1
                    page_success += 1
                    time.sleep(random.uniform(2, 4))
                else:
                    print(f"    Không lấy được dữ liệu hoặc không có giá")
            
            print(f"\nKẾT QUẢ TRANG {page}: {page_success}/{len(product_urls)} sách thành công")
            print(f"TỔNG CỘNG: {total_collected} sách")
            
            
            if page_success == 0:
                print("Không thu thập được sách nào, có thể hết dữ liệu")
                break
            
            
            if page < max_pages:
                delay = random.uniform(5, 8)
                print(f"Chờ {delay:.1f}s trước trang tiếp theo...")
                time.sleep(delay)
        
        
        if books_data:
            
            data_dir = os.path.join(os.path.dirname(__file__), '../../data')
            data_dir = os.path.abspath(data_dir)
            os.makedirs(data_dir, exist_ok=True)

            output_file_json = os.path.join(data_dir, 'fahasa_all_books.json')
            output_file_csv = os.path.join(data_dir, 'fahasa_all_books.csv')

            existing_data = []
            if os.path.exists(output_file_json):
                try:
                    with open(output_file_json, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                    print(f"Đã có {len(existing_data)} sách cũ")
                except:
                    pass

            
            existing_urls = {book.get('url', '') for book in existing_data}
            new_books = [book for book in books_data if book.get('url', '') not in existing_urls]

            all_books = existing_data + new_books

            
            with open(output_file_json, 'w', encoding='utf-8') as f:
                json.dump(all_books, f, ensure_ascii=False, indent=2)

            
            import pandas as pd
            df = pd.DataFrame(all_books)
            df.to_csv(output_file_csv, index=False, encoding='utf-8')

            print(f"\nHOÀN TẤT!")
            print(f"Thu thập mới: {len(new_books)} sách")
            print(f"Tổng cộng: {len(all_books)} sách")
            print(f"Đã lưu: {output_file_json}, {output_file_csv}")
        
    except KeyboardInterrupt:
        print("\nNgười dùng dừng chương trình")
    except Exception as e:
        print(f"\nLỗi: {e}")
    finally:
        driver.quit()
        print("Đóng trình duyệt")


if __name__ == "__main__":
    MAX_PAGES = 1
    BOOKS_PER_PAGE = 3
    print("CẤU HÌNH:")
    print(f"   Số trang: {MAX_PAGES}")
    print(f"   Sách/trang: {BOOKS_PER_PAGE}")
    print(f"   Tối đa: {MAX_PAGES * BOOKS_PER_PAGE} sách")
    print()
    choice = input("Bắt đầu test thu thập? (y/n): ").lower()
    if choice == 'y':
        scrape_fahasa_bulk(MAX_PAGES, BOOKS_PER_PAGE)
    else:
        print("Hủy bỏ")