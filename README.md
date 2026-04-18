# TVPL Camoufox Custom Batch Crawler

Crawler Ä‘á»™c láº­p cho `thuvienphapluat.vn` theo luá»“ng **custom batch 8 workers** (tá»« flow `09_start_camoufox_custom_batch_3workers.bat`).

Má»¥c tiÃªu: clone sang mÃ¡y khÃ¡c, cÃ i requirements, Ä‘iá»n `sá»‘ bÃ i + link + proxy` lÃ  cháº¡y ngay.

## 1) YÃªu cáº§u

- Python 3.10+ (khuyáº¿n nghá»‹ 3.11)
- Windows (Ä‘Ã£ cÃ³ `setup.bat`, `run.bat`)
- Tesseract OCR (náº¿u muá»‘n auto captcha OCR):
  - CÃ i táº¡i: `C:\Program Files\Tesseract-OCR\tesseract.exe`
  - Hoáº·c Ä‘áº·t biáº¿n mÃ´i trÆ°á»ng `TESSERACT_CMD` trá» Ä‘áº¿n `tesseract.exe`

## 2) CÃ i Ä‘áº·t nhanh

```bat
git clone <REPO_URL>
cd TVPL-Camoufox-CustomBatch
setup.bat
```

`setup.bat` sáº½:
- táº¡o `.venv`
- cÃ i `requirements.txt`
- cháº¡y `python -m camoufox fetch`

## 3) Cáº¥u hÃ¬nh Ä‘á»ƒ cháº¡y

Sá»­a file: `config/custom_batch.json`

Má»—i task cÃ³ format:

```json
{
  "docs": 120,
  "url": "https://thuvienphapluat.vn/page/tim-van-ban.aspx?...&org=22&page=1"
}
```

Ã nghÄ©a:
- `docs`: sá»‘ bÃ i muá»‘n crawl
- `url`: link listing báº¯t Ä‘áº§u (cÃ³ `org=...` vÃ  `page=...`)
- script tá»± tÃ­nh sá»‘ trang theo cÃ´ng thá»©c `ceil(docs / 20)`

Proxy theo worker:

```json
"workers": {
  "w1": { "proxy": "ip:port:user:password", "tasks": [ ... ] },
  "w2": { "proxy": "", "tasks": [ ... ] },
  "w3": { "proxy": "", "tasks": [ ... ] },
  "w4": { "proxy": "", "tasks": [ ... ] },
  "w5": { "proxy": "", "tasks": [ ... ] },
  "w6": { "proxy": "", "tasks": [ ... ] },
  "w7": { "proxy": "", "tasks": [ ... ] },
  "w8": { "proxy": "", "tasks": [ ... ] }
}
```

Proxy há»— trá»£ cÃ¡c format:
- `ip:port:user:password`
- `user:pass@ip:port`
- `ip:port`
- `http://user:pass@ip:port`

## 4) Cháº¡y

```bat
run.bat
```

Hoáº·c CLI:

```bat
.venv\Scripts\python.exe run_custom_batch.py --config config\custom_batch.json
```

Dry-run (kiá»ƒm tra config vÃ  lá»‡nh trÆ°á»›c khi cháº¡y tháº­t):

```bat
.venv\Scripts\python.exe run_custom_batch.py --config config\custom_batch.json --dry-run
```

## 5) ThÆ° má»¥c Ä‘áº§u ra

- Káº¿t quáº£ JSONL: `output/`
- PDF fallback links (1 URL / line): `output/pdf_urls_w1.txt` ... `output/pdf_urls_w8.txt`
- Log worker: `logs/camoufox_custom_batch/w1.log` ... `w8.log`
- Resume state: `state/custom_batch_resume/w1.json` ... `w8.json`

## 6) Resume / cháº¡y láº¡i

- Máº·c Ä‘á»‹nh script resume theo state cÅ©.
- Muá»‘n reset tiáº¿n Ä‘á»™ rá»“i crawl láº¡i:

```bat
.venv\Scripts\python.exe run_custom_batch.py --config config\custom_batch.json --reset-resume
```

## 7) File chÃ­nh

- `08_camoufox_crawl_by_org.py`: crawler 1 worker
- `09_parallel_camoufox_custom_batch.py`: Ä‘iá»u phá»‘i 8 worker + strict verify/backfill
- `run_custom_batch.py`: Ä‘á»c config JSON vÃ  build command cháº¡y
- `config/custom_batch.json`: cáº¥u hÃ¬nh cáº§n sá»­a khi dÃ¹ng

## 8) Ghi chu

Repo tap trung luong chay truc tiep bang Python (`run.bat` / `run_custom_batch.py`).
