import os
import json
from urllib.parse import urlparse, urlunparse

REDIRECT_FILE = "redirected_urls.json"

SCHEME_INSENSITIVE_HOSTS = {
    "photon-science.desy.de",
    "www.desy.de",
    "desy.de",
}
WWW_ALIAS_HOSTS = {
    "www.desy.de",
    "desy.de",
}
TRAILING_SLASH_COLLAPSE = True
INDEX_PAGES = {"/index.html", "/index_ger.html", "/index_eng.html", "/index_en.html"}

QUERY_INSENSITIVE = {}

def load_json_any(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def detect_format(data):
    if isinstance(data, dict) and 'urls_by_depth' in data:
        return 'urls_by_depth'
    if isinstance(data, dict):
        return 'dict_map'
    if isinstance(data, list):
        return 'list'
    raise ValueError("Unsupported JSON structure")

def iter_urls(data):
    fmt = detect_format(data)
    if fmt == 'urls_by_depth':
        for _, urls in data['urls_by_depth'].items():
            for u in urls:
                yield u
    elif fmt == 'dict_map':
        for u in data.keys():
            yield u
    else:
        for u in data:
            yield u

def rebuild_same_shape(original, kept_urls):
    fmt = detect_format(original)
    if fmt == 'urls_by_depth':
        return {'urls_by_depth': {
            k: [u for u in v if u in kept_urls] for k, v in original['urls_by_depth'].items()
        }}
    if fmt == 'dict_map':
        return {u: original[u] for u in original if u in kept_urls}
    return [u for u in original if u in kept_urls]

def build_redirect_map(path=REDIRECT_FILE):
    if not os.path.exists(path):
        return {}
    try:
        m = load_json_any(path)
        return {k: v for k, v in m.items() if k and v}
    except Exception:
        return {}

def resolve_final(u, redir_map, cache):
    if u in cache:
        return cache[u]
    seen = set()
    cur = u
    while cur not in seen and cur in redir_map and redir_map[cur] != cur:
        seen.add(cur)
        cur = redir_map[cur]
    for v in seen:
        cache[v] = cur
    cache[u] = cur
    return cur

def canonicalize(u):
    reasons = set()
    try:
        p = urlparse(u)
        scheme, netloc, path, params, query, frag = p.scheme, p.netloc, p.path or "", p.params, p.query, p.fragment
        host = netloc.lower()

        if host in SCHEME_INSENSITIVE_HOSTS and scheme.lower() in ("http", "https"):
            if scheme != "https":
                scheme = "https"
                reasons.add("canonical_equivalent_scheme")

        if host in WWW_ALIAS_HOSTS:
            new_host = host.replace("www.", "")
            if new_host != host:
                host = new_host
                reasons.add("canonical_equivalent_www")

        for idx in INDEX_PAGES:
            if path.lower().endswith(idx):
                path = path[: -len(idx)] or "/"
                reasons.add("canonical_equivalent_index")
                break

        if TRAILING_SLASH_COLLAPSE and path != "/" and path.endswith("/"):
            path = path[:-1]
            reasons.add("canonical_equivalent_index")

        if host in QUERY_INSENSITIVE and path in QUERY_INSENSITIVE[host] and query:
            query = ""
            reasons.add("canonical_equivalent_querystrip")

        return urlunparse((scheme or "", host, path, params, query, "")), reasons
    except Exception:
        return u, reasons

def dedupe_secondary_with_redirects_and_canon(secondary_path, primary_path=None):
    secondary = load_json_any(secondary_path)
    primary = load_json_any(primary_path) if primary_path else None

    redir_map = build_redirect_map()
    final_cache = {}

    def final_of(u):
        f = resolve_final(u, redir_map, final_cache)
        fc, canon_reasons = canonicalize(f)
        return fc, canon_reasons

    final_to_rep = {}
    if primary:
        for u in iter_urls(primary):
            fu, _ = final_of(u)
            final_to_rep.setdefault(fu, u)

    kept = []
    removed_pairs = []
    seen_finals_local = {}

    for u in iter_urls(secondary):
        fu, canon_reasons = final_of(u)

        if fu in final_to_rep:
            kept_rep = final_to_rep[fu]
            reason = "redirect_to_same_final_as_primary" if u != kept_rep else "exact_match_primary"
            removed_pairs.append({
                "duplicate": u,
                "kept": kept_rep,
                "reason": reason,
                "canonicalization": list(canon_reasons)
            })
            continue

        if fu in seen_finals_local:
            kept_rep = seen_finals_local[fu]
            reason = "redirect_to_same_final_within_secondary" if u != kept_rep else "exact_match_within_secondary"
            removed_pairs.append({
                "duplicate": u,
                "kept": kept_rep,
                "reason": reason,
                "canonicalization": list(canon_reasons)
            })
            continue

        kept.append(u)
        seen_finals_local[fu] = u
        final_to_rep.setdefault(fu, u)

    dedup_secondary = rebuild_same_shape(secondary, set(kept))

    out_path = secondary_path.replace(".json", "_dedup.json")
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(dedup_secondary, f, ensure_ascii=False, indent=2)

    report_path = secondary_path.replace(".json", "_dedup_removed_pairs.json")
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(removed_pairs, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Deduplicated written to: {out_path}")
    print(f"📝 Duplicate pairs report: {report_path}")
    print(f"   Kept: {len(kept)} | Removed: {len(removed_pairs)}")

    print("\n🔍 Removed URLs and reasons:")
    for entry in removed_pairs[:20]:
        print(f"- ❌ {entry['duplicate']} → kept as {entry['kept']} | reason: {entry['reason']}")
        if entry.get("canonicalization"):
            print(f"    Canonicalization applied: {entry['canonicalization']}")
    if len(removed_pairs) > 20:
        print(f"...and {len(removed_pairs) - 20} more removed URLs. See full report in: {report_path}\n")

# ✅ Run with one file (self-deduplication)
dedupe_secondary_with_redirects_and_canon(
    secondary_path="desy_url_map_20250425_155033_urls=200_000.json"
)

# ✅ Or run with two files
# dedupe_secondary_with_redirects_and_canon(
#     primary_path="Zero_text_scraped_urls.json",
#     secondary_path="desy_url_map_20250425_155033_urls=200_000.json"
# )


