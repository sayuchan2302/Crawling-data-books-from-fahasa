# -*- coding: utf-8 -*-
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import re

def setup_driver():
    """Thiết lập Chrome driver"""
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(20)
    driver.implicitly_wait(5)
    
    return driver

def extract_price(price_text):
    """Trích xuất giá từ text"""
    try:
        price_clean = re.sub(r'[^\d,.]', '', str(price_text))
        price_clean = price_clean.replace(',', '').replace('.', '')
        if price_clean:
            price_num = float(price_clean)
            if price_num < 1000 and price_num > 0:
                price_num *= 1000
            return price_num
        return 0.0
    except:
        return 0.0

def test_selectors_on_sample_page(driver, url):
    """Test các selector trên một trang mẫu"""
    print(f"Testing selectors on: {url}")
    
    try:
        driver.get(url)
        time.sleep(3)
        
        # Xử lý CloudFlare nếu có
        if "Just a moment" in driver.page_source:
            print("Waiting for CloudFlare...")
            for i in range(15):
                time.sleep(1)
                if "Just a moment" not in driver.page_source:
                    break
        
        results = {}
        
        # Test title selectors
        title_selectors = [
            'h1.product-title',
            'h1.title',
            '.product-name h1',
            'h1',
            '.product-view .product-name',
            '.product-detail-info h1'
        ]
        
        title_found = False
        for selector in title_selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                if element and element.text.strip():
                    results['title'] = element.text.strip()
                    results['title_selector'] = selector
                    title_found = True
                    break
            except:
                continue
        
        if not title_found:
            results['title'] = "NOT FOUND"
            results['title_selector'] = "NONE WORKED"
        
        # Test price selectors
        price_selectors = [
            '.price-current',
            '.current-price',
            '.price .price-new',
            '.product-price .price',
            '.special-price',
            '.sale-price',
            '.price',
            '.final-price',
            '.discount-price'
        ]
        
        price_found = False
        for selector in price_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element and element.text.strip():
                        price_text = element.text.strip()
                        price_value = extract_price(price_text)
                        if price_value > 0:
                            results['discount_price'] = price_value
                            results['discount_price_selector'] = selector
                            results['discount_price_text'] = price_text
                            price_found = True
                            break
                if price_found:
                    break
            except:
                continue
        
        if not price_found:
            results['discount_price'] = 0
            results['discount_price_selector'] = "NONE WORKED"
        
        # Test original price selectors
        original_price_selectors = [
            '.price-old',
            '.old-price',
            '.price .price-old',
            '.regular-price',
            '.original-price',
            '.list-price',
            '.msrp'
        ]
        
        original_price_found = False
        for selector in original_price_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element and element.text.strip():
                        price_text = element.text.strip()
                        price_value = extract_price(price_text)
                        if price_value > 0:
                            results['original_price'] = price_value
                            results['original_price_selector'] = selector
                            results['original_price_text'] = price_text
                            original_price_found = True
                            break
                if original_price_found:
                    break
            except:
                continue
        
        if not original_price_found:
            results['original_price'] = 0
            results['original_price_selector'] = "NONE WORKED"
        
        # Test author selectors
        author_selectors = [
            '.author',
            '.product-author',
            '[data-field="author"]',
            '.author-name',
            '.product-info .author',
            '.book-author',
            '.writer'
        ]
        
        author_found = False
        for selector in author_selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                if element and element.text.strip():
                    results['author'] = element.text.strip()
                    results['author_selector'] = selector
                    author_found = True
                    break
            except:
                continue
        
        if not author_found:
            results['author'] = "NOT FOUND"
            results['author_selector'] = "NONE WORKED"
        
        # Test publisher selectors
        publisher_selectors = [
            '.publisher',
            '.product-publisher',
            '[data-field="publisher"]',
            '.publisher-name',
            '.book-publisher'
        ]
        
        publisher_found = False
        for selector in publisher_selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                if element and element.text.strip():
                    results['publisher'] = element.text.strip()
                    results['publisher_selector'] = selector
                    publisher_found = True
                    break
            except:
                continue
        
        if not publisher_found:
            results['publisher'] = "NOT FOUND"
            results['publisher_selector'] = "NONE WORKED"
        
        # Test rating selectors
        rating_selectors = [
            '.rating .rating-value',
            '.product-rating .rating',
            '.stars .rating-value',
            '.review-rating',
            '.rating-value',
            '.star-rating'
        ]
        
        rating_found = False
        for selector in rating_selectors:
            try:
                element = driver.find_element(By.CSS_SELECTOR, selector)
                if element and element.text.strip():
                    rating_text = element.text.strip()
                    # Tìm số trong text
                    match = re.search(r'(\d+\.?\d*)', rating_text)
                    if match:
                        results['rating'] = float(match.group(1))
                        results['rating_selector'] = selector
                        results['rating_text'] = rating_text
                        rating_found = True
                        break
            except:
                continue
        
        if not rating_found:
            results['rating'] = 0
            results['rating_selector'] = "NONE WORKED"
        
        return results
        
    except Exception as e:
        print(f"Error testing page: {e}")
        return None

def main():
    """Test selectors trên các trang mẫu"""
    print("TESTING SELECTORS ON SAMPLE PAGES")
    print("=" * 50)
    
    driver = setup_driver()
    
    # Lấy một vài URL mẫu từ database
    import sqlite3
    conn = sqlite3.connect('fahasa_books.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT url FROM books LIMIT 3')
    sample_urls = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    if not sample_urls:
        print("No sample URLs found in database")
        return
    
    all_results = []
    
    for i, url in enumerate(sample_urls, 1):
        print(f"\n--- Testing URL {i}/{len(sample_urls)} ---")
        results = test_selectors_on_sample_page(driver, url)
        
        if results:
            all_results.append(results)
            
            print(f"Title: {results.get('title', 'N/A')}")
            print(f"Title selector: {results.get('title_selector', 'N/A')}")
            print(f"Author: {results.get('author', 'N/A')}")
            print(f"Author selector: {results.get('author_selector', 'N/A')}")
            print(f"Publisher: {results.get('publisher', 'N/A')}")
            print(f"Publisher selector: {results.get('publisher_selector', 'N/A')}")
            print(f"Discount price: {results.get('discount_price', 0)}")
            print(f"Discount price selector: {results.get('discount_price_selector', 'N/A')}")
            print(f"Original price: {results.get('original_price', 0)}")
            print(f"Original price selector: {results.get('original_price_selector', 'N/A')}")
            print(f"Rating: {results.get('rating', 0)}")
            print(f"Rating selector: {results.get('rating_selector', 'N/A')}")
        
        time.sleep(2)  # Delay between tests
    
    # Phân tích kết quả
    print(f"\n" + "="*50)
    print("SELECTOR ANALYSIS")
    print("="*50)
    
    if all_results:
        # Thống kê selector thành công
        selector_success = {}
        
        for result in all_results:
            for key, value in result.items():
                if key.endswith('_selector'):
                    field = key.replace('_selector', '')
                    if field not in selector_success:
                        selector_success[field] = {}
                    
                    selector = value
                    if selector not in selector_success[field]:
                        selector_success[field][selector] = 0
                    selector_success[field][selector] += 1
        
        for field, selectors in selector_success.items():
            print(f"\n{field.upper()} SELECTORS:")
            for selector, count in selectors.items():
                if selector != "NONE WORKED":
                    print(f"  {selector}: {count} successes")
        
        # Khuyến nghị selector tốt nhất
        print(f"\nRECOMMENDED SELECTORS:")
        for field, selectors in selector_success.items():
            if selectors:
                best_selector = max(selectors.items(), key=lambda x: x[1])
                if best_selector[0] != "NONE WORKED":
                    print(f"  {field}: {best_selector[0]} ({best_selector[1]} successes)")
    
    driver.quit()
    print(f"\nTesting completed!")

if __name__ == "__main__":
    main()
