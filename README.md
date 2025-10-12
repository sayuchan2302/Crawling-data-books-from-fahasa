# 📚 Fahasa Web Scraper

Automated data collection system for Fahasa.com with 25 comprehensive data fields.

## ⚡ Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start scraping
python fahasa_optimized.py

# View results
python final_summary.py
```

## 📁 Project Structure

**Core Scripts:**
- `fahasa_optimized.py` - Main scraping engine
- `fahasa_database.py` - SQLite database manager  
- `fix_data.py` - Data quality improvement

**Analysis & Reports:**
- `final_summary.py` - Comprehensive data analysis
- `final_test.py` - System validation

**Output Files:**
- `fahasa_all_books.json` - Consolidated JSON data
- `fahasa_all_books.xlsx` - Excel export
- `fahasa_books.db` - SQLite database (25 fields)

## 🎯 Features

✅ **25 Data Fields** - Complete book information  
✅ **Price Extraction** - 100% success rate with CloudFlare bypass  
✅ **File Consolidation** - Single JSON/Excel output  
✅ **Duplicate Prevention** - Smart data merging  
✅ **Quality Assurance** - Automated data validation

## � Technical Specifications

- **Technology Stack:** Python 3.12, Selenium WebDriver
- **Database:** SQLite with 25-field schema
- **Export Formats:** JSON, Excel, Database
- **Anti-Detection:** CloudFlare bypass capabilities

## ⚠️ Usage Guidelines

- Respect website terms of service
- Implement appropriate request delays
- Use data responsibly
- Follow robots.txt guidelines