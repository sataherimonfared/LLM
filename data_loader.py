"""
Analysis utilities: merging batch results and exporting summaries.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from itertools import chain
from typing import Any, Dict, List


def merge_batch_results(all_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {
        'character_chunks': {'text_chunks': [], 'document_metadata': []},
        'structural_chunks': {'text_chunks': [], 'document_metadata': []},
        'full_text_chunks': {'text_chunks': [], 'document_metadata': []},
        'character_counts_data': {
            'character_chunks': [], 'structural_chunks': [], 'full_text_chunks': []
        },
        'processed_urls': set(),
        'error_urls': set(),
        'url_stats': defaultdict(list),
    }

    for key in ['character_chunks', 'structural_chunks', 'full_text_chunks']:
        merged[key]['text_chunks'] = list(chain.from_iterable(r.get(key, {}).get('text_chunks', []) for r in all_results))
        merged[key]['document_metadata'] = list(chain.from_iterable(r.get(key, {}).get('document_metadata', []) for r in all_results))
        merged['character_counts_data'][key] = list(chain.from_iterable(r.get('character_counts_data', {}).get(key, []) for r in all_results))

    merged['processed_urls'].update(chain.from_iterable(r.get('processed_urls', []) for r in all_results))
    merged['error_urls'].update(chain.from_iterable(r.get('error_urls', []) for r in all_results))

    for result in all_results:
        for stat_key, stat_val in result.get('url_stats', {}).items():
            if isinstance(stat_val, (list, set)):
                merged['url_stats'][stat_key].extend(stat_val)
            elif isinstance(stat_val, dict):
                merged['url_stats'][stat_key].append(stat_val)
            elif isinstance(stat_val, int):
                merged['url_stats'][stat_key].append(stat_val)

    merged['processed_urls'] = list(merged['processed_urls'])
    merged['error_urls'] = list(merged['error_urls'])
    return merged


def export_merged_results(merged: Dict[str, Any], prefix: str = "processor") -> None:
    import orjson

    timestamp = datetime.now()
    iso_time = timestamp.isoformat()

    def write_json(filename: str, data: Any) -> None:
        with open(filename, "wb") as f:
            f.write(orjson.dumps(data))

    def format_text_chunks(docs: Dict[str, Any]) -> Dict[str, Any]:
        assert len(docs["text_chunks"]) == len(docs["document_metadata"])  # must match
        return {
            "timestamp": iso_time,
            "text_chunks": [
                {"content": doc, "metadata": meta}
                for doc, meta in zip(docs["text_chunks"], docs["document_metadata"])
            ],
        }

    def format_character_counts(data: Dict[str, Any]) -> Dict[str, Any]:
        total_pages = sum(len(v) for v in data.values())
        total_characters = sum(item.get("character_count", 0) for v in data.values() for item in v)
        total_words = sum(item.get("metadata", {}).get("word_count", 0) for v in data.values() for item in v)
        return {
            "timestamp": iso_time,
            "summary": {
                "total_pages": total_pages,
                "total_characters": total_characters,
                "total_words": total_words,
                "average_characters_per_page": round(total_characters / total_pages, 2) if total_pages else 0,
            },
            "pages": [item for v in data.values() for item in v],
        }

    write_json("page_character_counts_final.json", format_character_counts(merged.get("character_counts_data", {})))
    # structural
    write_json(f"{prefix}_structural_base_text_chunks_final.json", format_text_chunks(merged["structural_chunks"]))
    write_json(f"{prefix}_structural_base_results_final.json", merged["structural_chunks"]["document_metadata"])
    # full text
    write_json(f"{prefix}_full_text_base_text_chunks_final.json", format_text_chunks(merged["full_text_chunks"]))
    write_json(f"{prefix}_full_text_base_results_final.json", merged["full_text_chunks"]["document_metadata"])
    # size/character-based (notebook naming used 'sized_base' for character)
    write_json(f"{prefix}_sized_base_text_chunks_final.json", format_text_chunks(merged["character_chunks"]))
    write_json(f"{prefix}_sized_base_results_final.json", merged["character_chunks"]["document_metadata"])


    write_json(f"{prefix}_processed_urls_final.json", list(merged.get("processed_urls", [])))
    write_json(f"{prefix}_error_urls_final.json", list(merged.get("error_urls", [])))

    safe_url_stats = {
        k: list(v) if isinstance(v, set) else v
        for k, v in dict(merged.get("url_stats", {})).items()
    }
    write_json(f"{prefix}_url_stats_final.json", safe_url_stats)

