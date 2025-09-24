# Bot Gudang Inventaris

Bot Telegram untuk mengelola inventaris gudang dengan integrasi Google Sheets.

## Setup Environment

1. **Copy file environment template:**
   ```bash
   cp .env.example .env
   ```

2. **Edit file `.env` dengan konfigurasi Anda:**
   - `API_ID`: Telegram API ID dari https://my.telegram.org
   - `API_HASH`: Telegram API Hash dari https://my.telegram.org
   - `BOT_TOKEN`: Token bot dari @BotFather
   - `SPREADSHEET_ID`: ID Google Spreadsheet
   - `GOOGLE_DRIVE_PARENT_FOLDER_ID`: ID folder Google Drive untuk menyimpan foto

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup Google Credentials:**
   - Letakkan file `credentials.json` di root directory
   - File ini berisi kredensial Google API

5. **Jalankan bot:**
   ```bash
   python inventaris.py
   ```

## File Struktur

- `.env` - File konfigurasi environment (jangan di-commit)
- `.env.example` - Template file environment
- `credentials.json` - Google API credentials
- `inventaris.py` - Script utama bot
- `requirements.txt` - Dependencies Python

## Keamanan

File `.env` dan `credentials.json` sudah ditambahkan ke `.gitignore` untuk keamanan.
Jangan pernah commit file yang berisi kredensial atau token ke repository.