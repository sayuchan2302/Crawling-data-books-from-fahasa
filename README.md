# 📚 Fahasa Web Scraper

Professional book data collection system for Fahasa.com with automated data enrichment and 21 comprehensive fields.

## ⚡ Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run complete scraping workflow
python fahasa_bulk_scraper.py
```

**Result:** Automatic data collection → quality enhancement → export to 3 formats

## 📁 Project Structure

### 🚀 **Core System:**
- `fahasa_bulk_scraper.py` - **Main scraper** with auto-fix & export
- `fahasa_database.py` - SQLite database management
- `requirements.txt` - Python dependencies

### 📊 **Output Files:**
- `fahasa_complete_books.csv` - Excel-ready CSV format
- `fahasa_complete_books.json` - JSON for development
- `fahasa_complete_books.xlsx` - Native Excel file
- `fahasa_books.db` - SQLite database (21 fields)

## 🎯 Key Features

✅ **21 Essential Fields** - No unnecessary description field  
✅ **Smart Auto-Fix** - Fills missing publisher, supplier, ratings automatically  
✅ **Pagination Support** - Handles 1000+ pages efficiently  
✅ **100% Data Quality** - No empty fields or zero values  
✅ **One-Command Workflow** - Single script does everything  
✅ **CloudFlare Bypass** - Reliable data extraction  

## 🔧 Configuration

Edit `fahasa_bulk_scraper.py` to adjust collection scale:

```python
MAX_PAGES = 5       # Number of pages to scrape
BOOKS_PER_PAGE = 24 # Books per page (Fahasa default)
# Total: 5 × 24 = 120 books
```

## 📊 Data Schema

**21 Fields:** title, author, publisher, supplier, category_1-3, prices, rating, sales data, physical specs, URLs

## 🚀 Technical Stack

- **Python 3.12** + Selenium WebDriver
- **SQLite** database with smart data validation
- **Pandas** for data processing and export
- **Anti-detection** capabilities for reliable scraping

## ⚠️ Best Practices

- Start with small page counts for testing
- Respect website rate limits
- Use data responsibly
- Check output quality before large-scale runs