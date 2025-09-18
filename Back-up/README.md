## DESY Scraper (Notebook → Python Modules)

### Setup
- Create and activate a virtualenv (optional).
- Install dependencies:

```bash
pip install -r requirements.txt
python -m playwright install --with-deps | cat
```

### Run

```bash
python main.py --url-map Zero_text_scraped_urls.json --max-depth 2 --batch-size 100 --limit 1000
For long run without interuption
nohup python main.py --url-map desy_url_map_20250425_155033_urls=200_000_dedup.json --max-depth 2 --batch-size 100 --limit 1000 > scraper.log 2>&1 &
```

Flags:
- `--url-map`: Path to mapping JSON. Supports either `{ "urls_by_depth": { "0": [..] } }` or `{url: meta}`.
- `--max-depth`: Max depth to include from `urls_by_depth`.
- `--batch-size`: URL batch size for processing.
- `--limit`: Optional cap on total URLs.
- `-v`/`-vv`: Increase log verbosity (INFO/DEBUG to `desy_scraper.log`).

### Modules
- `processing.py`: `DESYContentProcessor` with async fetching, JS fallback, cleaning, chunking.
- `analysis.py`: merge/export helpers for results.
- `data_loader.py`: small utilities for loading URL maps.
- `main.py`: CLI entrypoint assembling the pipeline.

### Outputs
Files with prefix `desy_final*` and `page_character_counts_final.json` are written in the working directory.
**Full text per page** => `desy_final_full_text_base_text_chunks_final.json`  
**chunked_text_based_on_structural_method** => `desy_final_structural_base_text_chunks_final.json`  
**chunked_text_based_on_fixed_size_method** => `desy_final_sized_base_text_chunks_final.json`
