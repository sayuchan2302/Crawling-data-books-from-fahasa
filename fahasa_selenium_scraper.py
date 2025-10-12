from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import json
import random
from urllib.parse import urljoin, urlparse
import re
from fahasa_database import FahasaDatabase

class FahasaSeleniumScraper:
    def __init__(self, headless=False):
        self.books_data = []
        self.setup_driver(headless)
        
    def setup_driver(self, headless=False):
        """Thiết lập Selenium WebDriver"""
        print("Đang thiết lập trình duyệt...")
        
        chrome_options = Options()
        
        # Các tùy chọn để bypass detection
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # User agent ngẫu nhiên
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
        
        if headless:
            chrome_options.add_argument('--headless')
            
        # Tự động tải ChromeDriver
        service = Service(ChromeDriverManager().install())
        
        try:
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Ẩn dấu hiệu automation
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Thiết lập timeout
            self.driver.implicitly_wait(10)
            
            print("✅ Đã thiết lập trình duyệt thành công")
            
        except Exception as e:
            print(f"❌ Lỗi thiết lập trình duyệt: {e}")
            raise
    
    def extract_price(self, price_text):
        """Trích xuất số tiền từ text"""
        import re
        try:
            # Loại bỏ ký tự không phải số
            price_clean = re.sub(r'[^\d,.]', '', price_text)
            # Thay thế dấu phẩy thành dấu chấm nếu cần
            price_clean = price_clean.replace(',', '')
            return float(price_clean) if price_clean else 0.0
        except:
            return 0.0
    
    def extract_number(self, text):
        """Trích xuất số thập phân từ text"""
        import re
        try:
            match = re.search(r'(\d+\.?\d*)', text)
            return float(match.group(1)) if match else 0.0
        except:
            return 0.0
    
    def extract_integer(self, text):
        """Trích xuất số nguyên từ text"""
        import re
        try:
            # Tìm tất cả số trong text
            numbers = re.findall(r'\d+', text)
            if numbers:
                # Nối các số lại nếu có dấu phẩy (vd: 10,000)
                return int(''.join(numbers))
            return 0
        except:
            return 0
    
    def extract_product_details(self, book_data):
        """Thu thập thông tin chi tiết từ bảng thông số"""
        try:
            # Tìm bảng thông tin chi tiết
            detail_selectors = [
                '.product-details table',
                '.product-info table',
                '.product-specifications table',
                '.book-details table',
                '.additional-attributes table'
            ]
            
            table = None
            for selector in detail_selectors:
                try:
                    table = self.driver.find_element(By.CSS_SELECTOR, selector)
                    break
                except:
                    continue
            
            if table:
                rows = table.find_elements(By.TAG_NAME, 'tr')
                
                for row in rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, 'td')
                        if len(cells) >= 2:
                            label = cells[0].text.strip().lower()
                            value = cells[1].text.strip()
                            
                            # Mapping các trường dữ liệu
                            if any(keyword in label for keyword in ['nhà xuất bản', 'publisher']):
                                book_data['publisher'] = value
                            elif any(keyword in label for keyword in ['nhà cung cấp', 'supplier']):
                                book_data['supplier'] = value
                            elif any(keyword in label for keyword in ['năm xuất bản', 'publication year', 'năm phát hành']):
                                book_data['publish_year'] = self.extract_integer(value)
                            elif any(keyword in label for keyword in ['ngôn ngữ', 'language']):
                                book_data['language'] = value
                            elif any(keyword in label for keyword in ['số trang', 'pages', 'page count']):
                                book_data['page_count'] = self.extract_integer(value)
                            elif any(keyword in label for keyword in ['trọng lượng', 'weight']):
                                book_data['weight'] = self.extract_number(value)
                            elif any(keyword in label for keyword in ['kích thước', 'dimensions', 'size']):
                                book_data['dimensions'] = value
                                
                    except:
                        continue
            
            # Nếu không tìm thấy table, thử tìm dạng div/span
            if not book_data['publisher'] or not book_data['publish_year']:
                self.extract_details_from_divs(book_data)
                
        except Exception as e:
            print(f"Lỗi thu thập chi tiết: {e}")
    
    def extract_details_from_divs(self, book_data):
        """Thu thập thông tin từ các div/span thay vì table"""
        try:
            # Tìm tất cả text có chứa thông tin
            all_elements = self.driver.find_elements(By.XPATH, "//*[contains(text(), 'Nhà xuất bản') or contains(text(), 'Năm xuất bản') or contains(text(), 'Số trang') or contains(text(), 'Ngôn ngữ')]")
            
            for element in all_elements:
                try:
                    text = element.text.strip()
                    
                    if 'Nhà xuất bản' in text and not book_data['publisher']:
                        # Tìm giá trị ở element kế tiếp hoặc cùng element
                        value = self.extract_value_from_element(element, text, 'Nhà xuất bản')
                        if value:
                            book_data['publisher'] = value
                    
                    elif 'Năm xuất bản' in text and not book_data['publish_year']:
                        value = self.extract_value_from_element(element, text, 'Năm xuất bản')
                        if value:
                            book_data['publish_year'] = self.extract_integer(value)
                    
                    elif 'Số trang' in text and not book_data['page_count']:
                        value = self.extract_value_from_element(element, text, 'Số trang')
                        if value:
                            book_data['page_count'] = self.extract_integer(value)
                    
                    elif 'Ngôn ngữ' in text and not book_data['language']:
                        value = self.extract_value_from_element(element, text, 'Ngôn ngữ')
                        if value:
                            book_data['language'] = value
                            
                except:
                    continue
                    
        except Exception as e:
            print(f"Lỗi thu thập từ divs: {e}")
    
    def extract_value_from_element(self, element, text, label):
        """Trích xuất giá trị từ element"""
        try:
            # Thử tách từ cùng text
            if ':' in text:
                parts = text.split(':', 1)
                if len(parts) > 1:
                    return parts[1].strip()
            
            # Thử tìm ở element kế tiếp
            try:
                next_sibling = element.find_element(By.XPATH, "following-sibling::*[1]")
                return next_sibling.text.strip()
            except:
                pass
            
            # Thử tìm ở element cha
            try:
                parent = element.find_element(By.XPATH, "..")
                parent_text = parent.text.strip()
                if label in parent_text:
                    # Tách lấy phần sau label
                    index = parent_text.find(label)
                    after_label = parent_text[index + len(label):].strip()
                    if after_label.startswith(':'):
                        after_label = after_label[1:].strip()
                    # Lấy đến ký tự xuống dòng đầu tiên
                    return after_label.split('\n')[0].strip()
            except:
                pass
                
            return None
            
        except:
            return None
    
    def extract_categories(self, book_data):
        """Thu thập breadcrumb để lấy danh mục"""
        try:
            breadcrumb_selectors = [
                '.breadcrumb',
                '.breadcrumbs',
                '.navigation-path',
                '.category-path'
            ]
            
            categories = []
            
            for selector in breadcrumb_selectors:
                try:
                    breadcrumb = self.driver.find_element(By.CSS_SELECTOR, selector)
                    links = breadcrumb.find_elements(By.TAG_NAME, 'a')
                    
                    for link in links:
                        text = link.text.strip()
                        if text and text.lower() not in ['trang chủ', 'home']:
                            categories.append(text)
                    
                    if categories:
                        break
                        
                except:
                    continue
            
            # Gán categories
            if len(categories) >= 1:
                book_data['category_1'] = categories[0]
            if len(categories) >= 2:
                book_data['category_2'] = categories[1]
            if len(categories) >= 3:
                book_data['category_3'] = categories[2]
            
            # Nếu không có breadcrumb, thử lấy từ URL hoặc các element khác
            if not categories:
                self.extract_categories_from_url(book_data)
                
        except Exception as e:
            print(f"Lỗi thu thập categories: {e}")
    
    def extract_categories_from_url(self, book_data):
        """Trích xuất category từ URL"""
        try:
            url = book_data['url']
            
            # Mapping các pattern URL phổ biến
            category_mapping = {
                'sach-trong-nuoc': 'Sách trong nước',
                'sach-ngoai-quoc': 'Sách ngoại quốc', 
                'van-hoc': 'Văn học',
                'kinh-te': 'Kinh tế',
                'thieu-nhi': 'Thiếu nhi',
                'giao-khoa': 'Giáo khoa',
                'ky-nang-song': 'Kỹ năng sống',
                'tam-ly': 'Tâm lý'
            }
            
            for pattern, category in category_mapping.items():
                if pattern in url.lower():
                    book_data['category_1'] = category
                    break
                    
        except Exception as e:
            print(f"Lỗi trích xuất category từ URL: {e}")
    
    def wait_and_handle_cloudflare(self):
        """Xử lý CloudFlare check"""
        try:
            # Chờ trang load
            time.sleep(5)
            
            # Kiểm tra CloudFlare
            if "Just a moment" in self.driver.page_source or "Checking your browser" in self.driver.page_source:
                print("🔄 Đang chờ CloudFlare check...")
                
                # Chờ tối đa 30 giây
                for i in range(30):
                    time.sleep(1)
                    if "Just a moment" not in self.driver.page_source:
                        print("✅ Đã vượt qua CloudFlare check")
                        return True
                        
                print("❌ Không thể vượt qua CloudFlare check")
                return False
                
            return True
            
        except Exception as e:
            print(f"❌ Lỗi xử lý CloudFlare: {e}")
            return False
    
    def test_access(self):
        """Test truy cập website"""
        print("🧪 Đang test truy cập Fahasa.com...")
        
        try:
            self.driver.get('https://www.fahasa.com')
            
            if not self.wait_and_handle_cloudflare():
                return False
                
            # Kiểm tra title
            title = self.driver.title
            print(f"📄 Title: {title}")
            
            # Tìm các sản phẩm
            product_selectors = [
                '.product-item',
                '.item-inner',
                '.product',
                '[data-product-id]',
                '.book-item',
                '.product-info'
            ]
            
            products_found = False
            for selector in product_selectors:
                try:
                    products = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if products:
                        print(f"📚 Tìm thấy {len(products)} sản phẩm với selector '{selector}'")
                        products_found = True
                        break
                except:
                    continue
            
            if not products_found:
                # Tìm các link có thể là sản phẩm
                links = self.driver.find_elements(By.TAG_NAME, 'a')
                book_links = []
                
                for link in links[:50]:
                    try:
                        href = link.get_attribute('href')
                        text = link.text.strip()
                        
                        if href and text and len(text) > 5:
                            if any(keyword in href.lower() for keyword in ['.html', 'product', 'item']):
                                book_links.append({'text': text[:50], 'url': href})
                    except:
                        continue
                
                print(f"🔗 Tìm thấy {len(book_links)} link có thể là sản phẩm")
                
                # In vài link mẫu
                for i, item in enumerate(book_links[:3]):
                    print(f"   {i+1}. {item['text']}")
            
            return True
            
        except Exception as e:
            print(f"❌ Lỗi test truy cập: {e}")
            return False
    
    def get_book_categories(self):
        """Lấy danh sách danh mục sách"""
        print("📁 Đang tìm danh mục sách...")
        
        categories = []
        
        try:
            # Các selector có thể chứa menu danh mục
            menu_selectors = [
                '.nav-primary',
                '.main-menu',
                '.category-menu',
                '.navigation',
                '.menu-category'
            ]
            
            for selector in menu_selectors:
                try:
                    menu = self.driver.find_element(By.CSS_SELECTOR, selector)
                    links = menu.find_elements(By.TAG_NAME, 'a')
                    
                    for link in links:
                        href = link.get_attribute('href')
                        text = link.text.strip()
                        
                        if href and text and 'sach' in href.lower():
                            categories.append({
                                'name': text,
                                'url': href
                            })
                    
                    if categories:
                        break
                        
                except:
                    continue
            
            # Nếu không tìm được từ menu, thử tìm từ toàn bộ trang
            if not categories:
                all_links = self.driver.find_elements(By.TAG_NAME, 'a')
                
                for link in all_links:
                    try:
                        href = link.get_attribute('href')
                        text = link.text.strip()
                        
                        if href and text and len(text) > 3:
                            if any(keyword in href.lower() for keyword in ['sach-trong-nuoc', 'sach-ngoai-quoc', 'van-hoc', 'kinh-te']):
                                categories.append({
                                    'name': text,
                                    'url': href
                                })
                    except:
                        continue
            
            # Loại bỏ duplicates
            unique_categories = []
            seen_urls = set()
            
            for cat in categories:
                if cat['url'] not in seen_urls:
                    unique_categories.append(cat)
                    seen_urls.add(cat['url'])
            
            print(f"📚 Tìm thấy {len(unique_categories)} danh mục sách")
            
            for i, cat in enumerate(unique_categories[:10]):
                print(f"   {i+1}. {cat['name']}")
                
            return unique_categories
            
        except Exception as e:
            print(f"❌ Lỗi lấy danh mục: {e}")
            return []
    
    def scrape_book_details(self, book_url):
        """Thu thập chi tiết một cuốn sách"""
        try:
            print(f"📖 Đang thu thập: {book_url}")
            
            self.driver.get(book_url)
            time.sleep(random.uniform(2, 4))
            
            if not self.wait_and_handle_cloudflare():
                return None
            
            book_data = {
                'url': book_url,
                'title': '',
                'author': '',
                'publisher': '',
                'supplier': '',
                'category_1': '',
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
                'language': '',
                'page_count': 0,
                'weight': 0.0,
                'dimensions': '',
                'url_img': '',
                'description': ''
            }
            
            # Tên sách
            title_selectors = [
                'h1.product-title',
                'h1.title',
                '.product-name h1',
                'h1',
                '.product-view .product-name',
                '.product-detail-info h1'
            ]
            
            for selector in title_selectors:
                try:
                    title_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    book_data['title'] = title_elem.text.strip()
                    break
                except:
                    continue
            
            # Giá gốc và giá khuyến mãi
            original_price_selectors = [
                '.price-old',
                '.old-price',
                '.price .price-old',
                '.regular-price',
                '.original-price'
            ]
            
            discount_price_selectors = [
                '.price-current',
                '.current-price',
                '.price .price-new',
                '.product-price .price',
                '.special-price',
                '.sale-price'
            ]
            
            # Lấy giá gốc
            for selector in original_price_selectors:
                try:
                    price_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = price_elem.text.strip()
                    book_data['original_price'] = self.extract_price(price_text)
                    break
                except:
                    continue
            
            # Lấy giá khuyến mãi
            for selector in discount_price_selectors:
                try:
                    price_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = price_elem.text.strip()
                    book_data['discount_price'] = self.extract_price(price_text)
                    break
                except:
                    continue
            
            # Tính phần trăm giảm giá
            if book_data['original_price'] > 0 and book_data['discount_price'] > 0:
                book_data['discount_percent'] = round(
                    ((book_data['original_price'] - book_data['discount_price']) / book_data['original_price']) * 100, 2
                )
            
            # Tác giả
            author_selectors = [
                '.author',
                '.product-author',
                '[data-field="author"]',
                '.author-name',
                '.product-info .author'
            ]
            
            for selector in author_selectors:
                try:
                    author_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    book_data['author'] = author_elem.text.strip()
                    break
                except:
                    continue
            
            # Nhà xuất bản và nhà cung cấp
            publisher_selectors = [
                '.publisher',
                '.product-publisher',
                '[data-field="publisher"]'
            ]
            
            supplier_selectors = [
                '.supplier',
                '.product-supplier',
                '[data-field="supplier"]'
            ]
            
            for selector in publisher_selectors:
                try:
                    elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    book_data['publisher'] = elem.text.strip()
                    break
                except:
                    continue
            
            for selector in supplier_selectors:
                try:
                    elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    book_data['supplier'] = elem.text.strip()
                    break
                except:
                    continue
            
            # Đánh giá và số lượng đánh giá
            rating_selectors = [
                '.rating .rating-value',
                '.product-rating .rating',
                '.stars .rating-value',
                '.review-rating'
            ]
            
            rating_count_selectors = [
                '.rating-count',
                '.review-count',
                '.total-reviews'
            ]
            
            for selector in rating_selectors:
                try:
                    rating_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    rating_text = rating_elem.text.strip()
                    book_data['rating'] = self.extract_number(rating_text)
                    break
                except:
                    continue
            
            for selector in rating_count_selectors:
                try:
                    count_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    count_text = count_elem.text.strip()
                    book_data['rating_count'] = self.extract_integer(count_text)
                    break
                except:
                    continue
            
            # Số lượng đã bán
            sold_selectors = [
                '.sold-count',
                '.quantity-sold',
                '.sales-count',
                '.sold'
            ]
            
            for selector in sold_selectors:
                try:
                    sold_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    sold_text = sold_elem.text.strip()
                    book_data['sold_count'] = sold_text
                    book_data['sold_count_numeric'] = self.extract_integer(sold_text)
                    break
                except:
                    continue
            
            # Hình ảnh
            img_selectors = [
                '.product-image img',
                '.product-photo img',
                '.main-image img',
                '.product-view .product-image img'
            ]
            
            for selector in img_selectors:
                try:
                    img_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    img_src = img_elem.get_attribute('src') or img_elem.get_attribute('data-src')
                    if img_src:
                        book_data['url_img'] = img_src
                        break
                except:
                    continue
            
            # Mô tả
            desc_selectors = [
                '.product-description',
                '.description',
                '.product-collateral .std',
                '.product-view .description',
                '.book-description'
            ]
            
            for selector in desc_selectors:
                try:
                    desc_elem = self.driver.find_element(By.CSS_SELECTOR, selector)
                    book_data['description'] = desc_elem.text.strip()[:1000]  # Giới hạn 1000 ký tự
                    break
                except:
                    continue
            
            # Thu thập thông tin chi tiết từ bảng thông số
            self.extract_product_details(book_data)
            
            # Thu thập breadcrumb để lấy danh mục
            self.extract_categories(book_data)
            
            print(f"✅ Đã thu thập: {book_data['title']}")
            return book_data
            
        except Exception as e:
            print(f"❌ Lỗi thu thập sách {book_url}: {e}")
            return None
    
    def scrape_category(self, category_url, max_books=20):
        """Thu thập sách từ một danh mục"""
        print(f"📂 Đang thu thập danh mục: {category_url}")
        
        try:
            self.driver.get(category_url)
            time.sleep(3)
            
            if not self.wait_and_handle_cloudflare():
                return []
            
            book_links = []
            
            # Scroll để load thêm sản phẩm (nếu có lazy loading)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            # Tìm các link sản phẩm
            product_selectors = [
                '.product-item a',
                '.item a',
                '.product a',
                '.book-item a'
            ]
            
            for selector in product_selectors:
                try:
                    links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for link in links:
                        href = link.get_attribute('href')
                        if href and '.html' in href:
                            book_links.append(href)
                    
                    if book_links:
                        break
                        
                except:
                    continue
            
            # Loại bỏ duplicates và giới hạn số lượng
            book_links = list(set(book_links))[:max_books]
            
            print(f"🔗 Tìm thấy {len(book_links)} link sách")
            
            # Thu thập chi tiết từng sách
            books = []
            for i, book_url in enumerate(book_links):
                print(f"📚 {i+1}/{len(book_links)}")
                
                book_data = self.scrape_book_details(book_url)
                if book_data and book_data['title']:
                    books.append(book_data)
                
                # Delay để tránh bị block
                time.sleep(random.uniform(3, 6))
            
            return books
            
        except Exception as e:
            print(f"❌ Lỗi thu thập danh mục: {e}")
            return []
    
    def save_data(self, filename_prefix="fahasa_selenium"):
        """Lưu dữ liệu vào file và database"""
        if not self.books_data:
            print("❌ Không có dữ liệu để lưu")
            return
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        
        # Lưu CSV
        df = pd.DataFrame(self.books_data)
        csv_file = f"{filename_prefix}_{timestamp}.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"💾 Đã lưu {len(self.books_data)} sách vào {csv_file}")
        
        # Lưu JSON
        json_file = f"{filename_prefix}_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(self.books_data, f, ensure_ascii=False, indent=2)
        print(f"💾 Đã lưu dữ liệu vào {json_file}")
        
        # Lưu vào database SQLite
        try:
            db = FahasaDatabase()
            inserted_count = db.insert_books(self.books_data)
            print(f"🗄️ Đã lưu {inserted_count} sách vào database SQLite")
            
            # Xuất Excel từ database
            excel_file = db.export_to_excel(f"{filename_prefix}_{timestamp}.xlsx")
            print(f"📊 Đã tạo file Excel: {excel_file}")
            
            # Hiển thị thống kê database
            stats = db.get_statistics()
            print(f"\n📈 THỐNG KÊ DATABASE:")
            print(f"- Tổng số sách trong DB: {stats['total_books']}")
            print(f"- Sách có giá: {stats['books_with_price']}")
            print(f"- Giá trung bình: {stats['average_price']:,.0f} VNĐ")
            
            if stats['top_categories']:
                print(f"- Top danh mục:")
                for cat, count in stats['top_categories']:
                    print(f"  • {cat}: {count} sách")
            
        except Exception as e:
            print(f"❌ Lỗi lưu database: {e}")
    
    
    def run_scraping(self, max_categories=3, max_books_per_category=10):
        """Chạy thu thập dữ liệu chính"""
        print("🚀 Bắt đầu thu thập dữ liệu Fahasa với Selenium...")
        
        try:
            # Test truy cập
            if not self.test_access():
                print("❌ Không thể truy cập website")
                return
            
            # Lấy danh mục
            categories = self.get_book_categories()
            
            if not categories:
                print("❌ Không tìm thấy danh mục nào")
                return
            
            # Thu thập từng danh mục
            for i, category in enumerate(categories[:max_categories]):
                print(f"\n📁 Danh mục {i+1}/{min(max_categories, len(categories))}: {category['name']}")
                
                books = self.scrape_category(category['url'], max_books_per_category)
                
                for book in books:
                    book['category'] = category['name']
                    self.books_data.append(book)
                
                print(f"✅ Đã thu thập {len(books)} sách từ danh mục này")
                
                # Delay giữa các danh mục
                time.sleep(random.uniform(5, 10))
            
            print(f"\n🎉 Hoàn thành! Tổng cộng {len(self.books_data)} sách")
            
            # Lưu dữ liệu
            self.save_data()
            
            # Thống kê
            if self.books_data:
                print(f"\n📊 THỐNG KÊ:")
                print(f"- Tổng số sách: {len(self.books_data)}")
                print(f"- Sách có giá gốc: {len([b for b in self.books_data if b['original_price'] > 0])}")
                print(f"- Sách có giá khuyến mãi: {len([b for b in self.books_data if b['discount_price'] > 0])}")
                print(f"- Sách có tác giả: {len([b for b in self.books_data if b['author']])}")
                print(f"- Sách có nhà xuất bản: {len([b for b in self.books_data if b['publisher']])}")
                print(f"- Sách có năm xuất bản: {len([b for b in self.books_data if b['publish_year'] > 0])}")
                print(f"- Sách có số trang: {len([b for b in self.books_data if b['page_count'] > 0])}")
                print(f"- Sách có đánh giá: {len([b for b in self.books_data if b['rating'] > 0])}")
                print(f"- Sách có mô tả: {len([b for b in self.books_data if b['description']])}")
                
                # Tính giá trung bình
                prices = [b['discount_price'] or b['original_price'] for b in self.books_data if b['discount_price'] > 0 or b['original_price'] > 0]
                if prices:
                    avg_price = sum(prices) / len(prices)
                    print(f"- Giá trung bình: {avg_price:,.0f} VNĐ")
                
                print(f"\n📚 VÀI SÁCH MẪU:")
                for i, book in enumerate(self.books_data[:3]):
                    price_display = f"{book['discount_price']:,.0f}" if book['discount_price'] > 0 else f"{book['original_price']:,.0f}"
                    print(f"{i+1}. {book['title']}")
                    print(f"   - Tác giả: {book['author']}")
                    print(f"   - Giá: {price_display} VNĐ")
                    print(f"   - NXB: {book['publisher']}")
                    print(f"   - Danh mục: {book['category_1']}")
                    if book['rating'] > 0:
                        print(f"   - Đánh giá: {book['rating']}/5 ({book['rating_count']} đánh giá)")
                    print()
            
        except KeyboardInterrupt:
            print("\n⏹️ Đã dừng bởi người dùng")
            self.save_data()
            
        except Exception as e:
            print(f"❌ Lỗi chung: {e}")
            self.save_data()
            
        finally:
            self.driver.quit()
            print("🔚 Đã đóng trình duyệt")

def main():
    """Hàm chính"""
    print("🎯 FAHASA SELENIUM SCRAPER")
    print("=" * 50)
    
    # Tùy chọn
    headless = input("Chạy ẩn trình duyệt? (y/n): ").lower() == 'y'
    max_categories = int(input("Số danh mục tối đa (1-5): ") or "2")
    max_books = int(input("Số sách tối đa mỗi danh mục (1-20): ") or "5")
    
    scraper = FahasaSeleniumScraper(headless=headless)
    scraper.run_scraping(max_categories=max_categories, max_books_per_category=max_books)

if __name__ == "__main__":
    main()