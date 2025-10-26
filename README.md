# ��� Fahasa Book Scraper & Data Warehouse

Web scraping project for Fahasa.com with Star Schema Data Warehouse design.

## ��� Quick Start

```bash
pip install -r requirements.txt
python fahasa_bulk_scraper.py
```

## ✨ Features

- Selenium scraper with 98.7% success rate
- 22 data fields per book (title, author, price, rating, etc.)
- Star Schema DW (1 Fact + 6 Dimensions + 2 Logs)
- Auto-fix algorithm for data quality (95.3% completeness)
- Multi-format export: JSON, CSV, Excel, SQLite

## ���️ Tech Stack

Python 3.12 • Selenium • Pandas • SQLite • BeautifulSoup4

---

⭐ [sayuchan2302](https://github.com/sayuchan2302) • For educational purposes only
