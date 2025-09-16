"""
Main entrypoint to run the scraping pipeline.

Usage:
  python main.py --url-map Zero_text_scraped_urls.json --max-depth 2 --batch-size 100 --limit 1000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List

from analysis import export_merged_results, merge_batch_results
from processing import DESYContentProcessor


def configure_logging(verbosity: int) -> None:
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    level = logging.DEBUG if verbosity >= 2 else logging.INFO
    logging.basicConfig(
        filename='desy_scraper.log',
        filemode='w',
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
    )


def load_mapping(path: str) -> Dict[str, Any]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def batch_urls(urls: List[str], batch_size: int):
    for i in range(0, len(urls), batch_size):
        yield urls[i:i + batch_size]


def extract_all_urls(url_map: Dict[str, Any]) -> List[str]:
    if isinstance(url_map, dict) and 'urls_by_depth' in url_map:
        ordered_depths = sorted((int(k), v) for k, v in url_map['urls_by_depth'].items() if str(k).isdigit())
        urls: List[str] = []
        for _, lst in ordered_depths:
            urls.extend(lst)
        return urls
    if isinstance(url_map, dict):
        return list(url_map.keys())
    return list(url_map)


def merge_url_maps_with_priority(files_to_scrape: List[str]) -> List[str]:
    seen: set[str] = set()
    merged: List[str] = []
    for path in files_to_scrape:
        try:
            data = load_mapping(path)
        except FileNotFoundError:
            continue
        urls = extract_all_urls(data)
        for u in urls:
            if u not in seen:
                seen.add(u)
                merged.append(u)
    return merged


# def process_mapped_urls(url_map_file: str, max_depth: int, batch_size: int, limit: int | None):
#     async def _run():
#         # Call the processor ONCE for the file (match notebook behavior to avoid duplicates)
#         processor = DESYContentProcessor(max_depth=max_depth, chunk_size=500, chunk_overlap=75)
#         results = await processor.process_urls_from_mapping(url_map_file, batch_size=batch_size, limit=limit)

#         # Build the same merged structure shape as the notebook
#         character_urls = set(doc.metadata.get('source', '') for doc in results['character_chunks'])
#         structural_urls = set(doc.metadata.get('source', '') for doc in results['structural_chunks'])
#         full_text_urls = set(doc.metadata.get('source', '') for doc in results['full_text_chunks'])

#         def build_chunk_metadata(docs):
#             return [
#                 {
#                     'url': doc.metadata.get('source', ''),
#                     'chunk_index': i,
#                     'character_count': len(doc.page_content),
#                     'metadata': doc.metadata,
#                 }
#                 for i, doc in enumerate(docs)
#             ]

#         character_counts_data = {
#             'character_chunks': build_chunk_metadata(results['character_chunks']),
#             'structural_chunks': build_chunk_metadata(results['structural_chunks']),
#             'full_text_chunks': build_chunk_metadata(results['full_text_chunks']),
#         }

#         batch_result = {
#             'character_chunks': {
#                 'text_chunks': [doc.page_content for doc in results['character_chunks']],
#                 'document_metadata': [doc.metadata for doc in results['character_chunks']],
#             },
#             'structural_chunks': {
#                 'text_chunks': [doc.page_content for doc in results['structural_chunks']],
#                 'document_metadata': [doc.metadata for doc in results['structural_chunks']],
#             },
#             'full_text_chunks': {
#                 'text_chunks': [doc.page_content for doc in results['full_text_chunks']],
#                 'document_metadata': [doc.metadata for doc in results['full_text_chunks']],
#             },
#             'character_counts_data': character_counts_data,
#             'processed_urls': list(set(list(character_urls | structural_urls | full_text_urls))),
#             'error_urls': list(processor.error_urls.keys()),
#             'url_stats': {
#                 'total_character_urls': len(character_urls),
#                 'total_structural_urls': len(structural_urls),
#                 'total_full_text_urls': len(full_text_urls),
#                 'redirected_urls': processor.redirected_urls,
#             },
#         }
#         final_result = merge_batch_results([batch_result])
#         return final_result
#     return asyncio.run(_run())

def process_mapped_urls(url_map_file: str, max_depth: int, batch_size: int, limit: int | None):
    async def _run():
        # Call the processor ONCE for the file (match notebook behavior)
        processor = DESYContentProcessor(max_depth=max_depth, chunk_size=500, chunk_overlap=75)
        results = await processor.process_urls_from_mapping(url_map_file, batch_size=batch_size, limit=limit)

        # Build batch result in the same structure the notebook uses
        def build_chunk_metadata(docs):
            return [
                {
                    'url': doc.metadata.get('source', ''),
                    'chunk_index': i,
                    'character_count': len(doc.page_content),
                    'metadata': doc.metadata,
                }
                for i, doc in enumerate(docs)
            ]

        batch_result = {
            'character_chunks': {
                'text_chunks': [doc.page_content for doc in results['character_chunks']],
                'document_metadata': [doc.metadata for doc in results['character_chunks']],
            },
            'structural_chunks': {
                'text_chunks': [doc.page_content for doc in results['structural_chunks']],
                'document_metadata': [doc.metadata for doc in results['structural_chunks']],
            },
            'full_text_chunks': {
                'text_chunks': [doc.page_content for doc in results['full_text_chunks']],
                'document_metadata': [doc.metadata for doc in results['full_text_chunks']],
            },
            'character_counts_data': {
                'character_chunks': build_chunk_metadata(results['character_chunks']),
                'structural_chunks': build_chunk_metadata(results['structural_chunks']),
                'full_text_chunks': build_chunk_metadata(results['full_text_chunks']),
            },
            'processed_urls': list({doc.metadata.get('source','') for doc in (results['character_chunks'] + results['structural_chunks'] + results['full_text_chunks'])}),
            'error_urls': list(processor.error_urls.keys()),
            'url_stats': {
                'redirected_urls': processor.redirected_urls,
            },
        }
        return batch_result
    return asyncio.run(_run())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run DESY scraping pipeline")
    p.add_argument('--url-map', required=True, nargs='+', help='One or more URL mapping JSON files (priority by order)')
    p.add_argument('--max-depth', type=int, default=2)
    p.add_argument('--batch-size', type=int, default=100)
    p.add_argument('--limit', type=int, default=None)
    p.add_argument('-v', '--verbose', action='count', default=1)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    # Merge multiple url maps with first-file priority, then limit
    files_to_scrape: List[str] = list(args.url_map)
    merged_urls = merge_url_maps_with_priority(files_to_scrape)
    if args.limit is not None:
        merged_urls = merged_urls[: args.limit]

    # Create a transient in-memory url_map structure like the notebook expects
    url_map = {u: {} for u in merged_urls}

    # Write a temporary file or pass directly by adapting processor (we adapt by dumping to a temp path)
    tmp_path = 'merged_url_map_temp.json'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(url_map, f, ensure_ascii=False)

    result = process_mapped_urls(tmp_path, args.max_depth, args.batch_size, None)
    export_merged_results(result, prefix="desy_final")
    print("Done. Outputs written with prefix desy_final*")


if __name__ == '__main__':
    main()


