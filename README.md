# 📚 Fahasa Book Crawler - Production Ready

Automated web crawler for collecting book data from Fahasa.com and storing in MySQL database.

## 🎯 Features

- **Fixed Collection**: Crawls exactly 50 books per run
- **Dual Storage**: MySQL database + Daily JSON/CSV files
- **Organized Export**: Files saved in `data/YYYY/MM/DD/` structure
- **Duplicate Prevention**: Automatically skips existing books
- **Windows Task Scheduler**: Ready for automated daily runs
- **Production Ready**: Clean, optimized, and reliable

## 🚀 Quick Start

### Setup Environment
```bash
# Install dependencies
pip install -r requirements.txt

# Setup MySQL databases
mysql < mysql_setup.sql
```

### Run Crawler
```bash
# Manual run (50 books)
python daily_crawler.py

# Or run main crawler
python src/crawler/fahasa_bulk_scraper.py
```

### Schedule Automation
See `TASK_SCHEDULER_SETUP.md` for Windows Task Scheduler configuration.

## 📁 Project Structure

```
📦 script/
├── 📄 daily_crawler.py          # Main automation script (50 books/run)
├── 📄 mysql_setup.sql           # Complete database setup
├── 📄 requirements.txt          # Clean dependencies
├── 📄 TASK_SCHEDULER_SETUP.md   # Automation guide
├── 📄 DATA_EXPORT_GUIDE.md      # File export documentation
├── 📂 src/crawler/
│   ├── 📄 fahasa_bulk_scraper.py    # Core crawler engine
│   └── 📄 insert_staging_book.py   # MySQL insert function
├── 📂 logs/                     # Crawler execution logs
└── 📂 data/                     # Organized daily exports (YYYY/MM/DD/)
```

## ⚙️ Configuration

- **Books per run**: 50 (configurable in `daily_crawler.py`)
- **Database**: MySQL 4-database architecture 
- **Scheduling**: Windows Task Scheduler compatible
- **Logging**: Automatic daily logs in `logs/` directory

## 🔧 Technical Details

- **Language**: Python 3.12+
- **Web Driver**: Selenium + Chrome
- **Database**: MySQL with UTF8MB4 encoding
- **Architecture**: Staging → Control → Data Warehouse → Data Mart

## 📊 Database Schema

4-database architecture:
- `fahasa_staging`: Raw crawled data
- `fahasa_control`: Batch tracking and metadata  
- `fahasa_dw`: Clean data warehouse
- `fahasa_datamart`: Aggregated analytics data
