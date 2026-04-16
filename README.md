# TVPL Camoufox Custom Batch Crawler

Crawler độc lập cho `thuvienphapluat.vn` theo luồng **custom batch 3 workers** (từ flow `09_start_camoufox_custom_batch_3workers.bat`).

Mục tiêu: clone sang máy khác, cài requirements, điền `số bài + link + proxy` là chạy ngay.

## 1) Yêu cầu

- Python 3.10+ (khuyến nghị 3.11)
- Windows (đã có `setup.bat`, `run.bat`)
- Tesseract OCR (nếu muốn auto captcha OCR):
  - Cài tại: `C:\Program Files\Tesseract-OCR\tesseract.exe`
  - Hoặc đặt biến môi trường `TESSERACT_CMD` trỏ đến `tesseract.exe`

## 2) Cài đặt nhanh

```bat
git clone <REPO_URL>
cd TVPL-Camoufox-CustomBatch
setup.bat
```

`setup.bat` sẽ:
- tạo `.venv`
- cài `requirements.txt`
- chạy `python -m camoufox fetch`

## 3) Cấu hình để chạy

Sửa file: `config/custom_batch.json`

Mỗi task có format:

```json
{
  "docs": 120,
  "url": "https://thuvienphapluat.vn/page/tim-van-ban.aspx?...&org=22&page=1"
}
```

Ý nghĩa:
- `docs`: số bài muốn crawl
- `url`: link listing bắt đầu (có `org=...` và `page=...`)
- script tự tính số trang theo công thức `ceil(docs / 20)`

Proxy theo worker:

```json
"workers": {
  "w1": { "proxy": "ip:port:user:password", "tasks": [ ... ] },
  "w2": { "proxy": "", "tasks": [ ... ] },
  "w3": { "proxy": "", "tasks": [ ... ] }
}
```

Proxy hỗ trợ các format:
- `ip:port:user:password`
- `user:pass@ip:port`
- `ip:port`
- `http://user:pass@ip:port`

## 4) Chạy

```bat
run.bat
```

Hoặc CLI:

```bat
.venv\Scripts\python.exe run_custom_batch.py --config config\custom_batch.json
```

Dry-run (kiểm tra config và lệnh trước khi chạy thật):

```bat
.venv\Scripts\python.exe run_custom_batch.py --config config\custom_batch.json --dry-run
```

## 5) Thư mục đầu ra

- Kết quả JSONL: `output/`
- Log worker: `logs/camoufox_custom_batch/w1.log`, `w2.log`, `w3.log`
- Resume state: `state/custom_batch_resume/w1.json`, `w2.json`, `w3.json`

## 6) Resume / chạy lại

- Mặc định script resume theo state cũ.
- Muốn reset tiến độ rồi crawl lại:

```bat
.venv\Scripts\python.exe run_custom_batch.py --config config\custom_batch.json --reset-resume
```

## 7) File chính

- `08_camoufox_crawl_by_org.py`: crawler 1 worker
- `09_parallel_camoufox_custom_batch.py`: điều phối 3 worker + strict verify/backfill
- `run_custom_batch.py`: đọc config JSON và build command chạy
- `config/custom_batch.json`: cấu hình cần sửa khi dùng

## 8) Chạy bằng Docker Compose

Repo đã có sẵn:
- `Dockerfile`
- `docker-compose.yml`
- `docker/entrypoint.sh`

### Chuẩn bị

1. Cài Docker Desktop.
2. Sửa `config/custom_batch.json` (docs/url/proxy).

### Build image

```bash
docker compose build
```

### Chạy crawl

```bash
docker compose up
```

Container sẽ mount sẵn:
- `./config -> /app/config`
- `./output -> /app/output`
- `./logs -> /app/logs`
- `./state -> /app/state`

Mặc định compose chạy `--headless`.

### Chạy lại và reset resume

```bash
docker compose run --rm crawler python run_custom_batch.py --config /app/config/custom_batch.json --headless --reset-resume
```

### Bỏ bước fetch Camoufox mỗi lần start

Mặc định entrypoint chạy `python -m camoufox fetch` để đảm bảo browser có sẵn.
Sau lần đầu, nếu muốn start nhanh hơn:

```yaml
environment:
  SKIP_CAMOUFOX_FETCH: "1"
```
