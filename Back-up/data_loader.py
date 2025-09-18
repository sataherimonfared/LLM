"""
Data loading utilities for URL maps and simple JSON IO.
"""

from __future__ import annotations

import json
from typing import Dict, List


def load_url_keys_from_mapping(path: str) -> List[str]:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, dict) and 'urls_by_depth' in data:
        urls: List[str] = []
        for depth_list in data['urls_by_depth'].items():
            for u in depth_list[1]:
                urls.append(u)
        return urls
    if isinstance(data, dict):
        return list(data.keys())
    return list(data)


def map_urls_to_depth(url_map: Dict) -> Dict[str, int]:
    depth_dict: Dict[str, int] = {}
    for depth_key, url_list in url_map.get("urls_by_depth", {}).items():
        try:
            depth_int = int(depth_key)
        except (ValueError, TypeError):
            continue
        for url in url_list:
            if url not in depth_dict or depth_int < depth_dict[url]:
                depth_dict[url] = depth_int
    return depth_dict



