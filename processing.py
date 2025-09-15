"""
Processing module: contains the DESYContentProcessor and the async scraping pipeline.

This file refactors the notebook implementation into a reusable module.
Run the pipeline via main.py, not directly from here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import ssl
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from urllib.parse import urlparse

import aiohttp
import psutil
from bs4 import BeautifulSoup
from langchain.schema import Document
from langdetect import detect
from playwright.async_api import async_playwright
from tqdm import tqdm


logger = logging.getLogger(__name__)


class DESYContentProcessor:
    """High-throughput content processor for DESY websites.

    Mirrors the notebook behavior, with async fetching (aiohttp),
    optional JS rendering fallback (Playwright), cleaning and chunking.
    """

    MIN_CHUNK_CHARS = 30
    MIN_INITIAL_CHARS = 20
    MIN_TEXT_SAMPLE_LENGTH = 50
    MAX_CONTENT_AREA_SIZE = 50000
    DEFAULT_TIMEOUT = 300
    JS_DETECTION_MIN_LINKS = 3
    NON_HTML_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.pdf', '.mp4', '.mp3',
        '.avi', '.mov', '.wmv', '.zip', '.tar', '.gz', '.doc', '.docx', '.xls',
        '.xlsx', '.ppt', '.pptx', '.xml'
    }

    def __init__(
        self,
        max_depth: int,
        content_tags: List[str] | None = None,
        excluded_keywords: List[str] | None = None,
        chunk_size: int = 1000,
        chunk_overlap: int = 200,
        batch_size: int = 10,
        timeout: int = 300,
        js_wait_time: int = 10000,
        js_scroll: bool = True,
    ) -> None:
        self.browser = None
        self.context = None

        logical_cpus = psutil.cpu_count(logical=True) or 2
        total_ram_gb = (psutil.virtual_memory().total / 1e9) if psutil.virtual_memory() else 4

        self.max_workers = min(int(logical_cpus * 2), int((total_ram_gb // 2) * logical_cpus), 200)
        js_slots = max(4, self.max_workers // 6)
        self.js_semaphore = asyncio.Semaphore(js_slots)

        self.browser_lock = asyncio.Lock()
        self.session_lock = asyncio.Lock()
        self.url_lock = asyncio.Lock()

        self.cookie_text_patterns = [
            r'cookie[- ]?banner', r'cookie[- ]?consent', r'diese website verwendet cookies',
            r'we use cookies', r'accept all cookies', r'cookie einstellungen', r'cookie policy',
            r'consent to cookies', r'diese seite nutzt cookies', r'cookie notice', r'cookie preferences',
            r'cookie declaration', r'cookie information', r'cookie settings', r'cookie usage'
        ]

        # Patterns (kept as in notebook)
        self.critical_patterns = [
            re.compile(r'<(script|style)[^>]*>.*?</\1>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<nav\b[^>]*>.*?</nav>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:header|footer)\b[^>]*>.*?</(?:header|footer)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<form\b[^>]*>.*?</form>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|section|nav|ul|header)\b[^>]*id\s*=\s*[\'\"](?:footer|overall|wrapper|icons|search_icon|phone_icon|close_gcs|mobile_menu_header|mobile_menu|mobile_dropdown|mobile_loading|mobile_dropdown_content|top|logoarea|topleft|topright|topmenu|menu|main_menu|header|leftmenu|rightmenu)\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|section|nav|ul|header)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(div|section|aside|footer)[^>]*id=["\']?[^"\'>]*\b(cookie|consent|privacy|banner|notice|preferences)\b[^"\'>]*["\']?[^>]*>.*?</\1>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(div|section|aside|footer)[^>]*class=["\'][^"\'>]*\b(cookie|consent|banner|popup|notice|preferences|privacy|cookie-consent-wrapper|cookie-bar-wrapper)[^"\'>]*["\'][^>]*>.*?</\1>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(div|section|aside|footer)[^>]*style=["\'][^"\']*display\s*:\s*none[^"\']*["\'][^>]*>.*?</\1>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<[^>]+class=["\'][^"\'>]*\bcookie-bar__inner\b[^"\'>]*["\'][^>]*>.*?</[^>]+>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<!--\s*Cookie\s+Bar\s*-->.*?<!--\s*End\s+Cookie\s+Bar\s*-->', re.DOTALL | re.IGNORECASE),
            re.compile(r'<div[^>]*id=["\']?cookie-bar["\']?[^>]*>.*?</div>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<nav\b[^>]*id\s*=\s*[\'\"](?:leftmenu|topmenu|menu)[^\'\"]*[\'\"][^>]*>.*?</nav>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<ul\b[^>]*id\s*=\s*[\'\"](?:main_menu|menu)[^\'\"]*[\'\"][^>]*>.*?</ul>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<li\b[^>]*class\s*=\s*[\'\"][^\'\"]*\b(?:inactive|active|ZMSFolder\d*|ZMSDocument\d*)\b[^\'\"]*[\'\"][^>]*>.*?</li>', re.DOTALL | re.IGNORECASE),
        ]

        self.high_priority_patterns = [
            re.compile(r'<(?:div|ul|ol|section)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:breadcrumb|bread[-_]?nav|nav|navigation|tagline|menu[-_]?bar|top[-_]?nav|site[-_]?nav|main[-_]?navigation|nav[-_]?container|sub[-_]?nav|menu[-_]?container|menu|sub[-_]?menu|nav[-_]?menu|quick[-_]?nav|quick[-_]?links)\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|ul|ol|section)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|ul|ol|section|li)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:breadcrumb|bread[-_]?nav|nav|navigation|tagline|menu[-_]?bar|top[-_]?nav|site[-_]?nav|main[-_]?navigation|nav[-_]?container|sub[-_]?nav|menu[-_]?container|menu|sub[-_]?menu|nav[-_]?menu|quick[-_]?nav|quick[-_]?links|topright[-_]?button|wrapper)\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|ul|ol|section|li)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:header|footer)\b[^>]*(?:id\s*=\s*[\'\"]header[\'\"])?.*?</(?:header|footer)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<div\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:header|footer|site[-_]?footer|page[-_]?footer|site[-_]?header|nav[-_]?footer|group[-_]?header|banner[-_]?header|wrapper)\b[^\'\"]*[\'\"][^>]*>.*?</div>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|section|aside)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:cookies?|consent|banner|popup|modal|cookie[-_]?notices?|cookie[-_]?consents?|cookie[-_]?policys?|gdpr|privacy[-_]?banner)\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|section|aside)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|aside|section)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:sidebar|left|right|side[-_]?nav|widget[-_]?area|nav[-_]?panel)\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|aside|section)>', re.DOTALL | re.IGNORECASE),
        ]

        self.medium_priority_patterns = [
            re.compile(r'<div\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:search|search[-_]?form|search[-_]?box|search[-_]?bar|cse[-_]?search[-_]?form)\b[^\'\"]*[\'\"][^>]*>.*?</div>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|nav|ul)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\bmobile(?:[-_]?((?:nav|menu|back|toggle|dropdown|loading)))?\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|nav|ul)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|ul|select)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:lang|language|lang[-_]?switch)\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|ul|select)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|section)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:overlay|modal[-_]?overlay|popup[-_]?overlay)\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|section)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:button|input|div)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:btns?|buttons?|btt|topright[-_]?button)\b[^\'\"]*[\'\"][^>]*(?:>.*?</(?:button|input|div)>|/??>)', re.DOTALL | re.IGNORECASE),
            re.compile(r'<a\b[^>]*href\s*=\s*[\'\"][^\'\"]*\b(?:doi\.org|journals\.aps\.org|dx\.doi\.org|DOI:)\b[^\'\"]*[\'\"][^>]*>.*?</a>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|section)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:wrapper|container|main[-_]?container|page[-_]?wrapper|site[-_]?wrapper)\b[^\'\"]*[\'\"][^>]*>(?:(?!<(?:main|article|content)\b).)*?</(?:div|section)>', re.DOTALL | re.IGNORECASE),
        ]

        self.low_priority_patterns = [
            re.compile(r'<li\b[^>]*(?:class\s*=\s*[\'\"][^\'\"]*\b(?:inactive|folder|nav[-_]?item|menu[-_]?item|ZMSFolder\d*|ZMSDocument\d*)\b[^\'\"]*[\'\"])??[^>]*>.*?</li>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<(?:div|section|aside|span)\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:footnotes?|foot[-_]?notes?|references?|citations?|endnotes?)\b[^\'\"]*[\'\"][^>]*>.*?</(?:div|section|aside|span)>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<a\b[^>]*(?:id\s*=\s*[\'\"](?:mobile_back_to_desy|mobile[-_]?nav[-_]?toggle|search|phone)[\'\"]|(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:inactive|ZMSFolder\d*|ZMSDocument\d*)\b[^\'\"]*[\'\"]|href\s*=\s*[\'\"][^\'\"]*(?:index_print|desy\.de|testbeam\.desy\.de)[^\'\"]*[\'\"]|title\s*=\s*[\'\"][^\'\"]*(?:Change\s+language|DESY\s+Homepage|to\s+[\'\"]Accelerators[\'\"])\b[^\'\"]*[\'\"]|target\s*=\s*[\'\"]_blank[\'\"])??[^>]*>.*?</a>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<img\b[^>]*(?:id\s*=\s*[\'\"][^\'\"]*(?:phonebook_icon|print_icon|lang_icon|desylogo)[^\'\"]*[\'\"]|alt\s*=\s*[\'\"][^\'\"]*(?:phone\s+book|Diese\s+Seite\s+drucken|loading|DESY\s+Logo)[^\'\"]*[\'\"]|src\s*=\s*[\'\"][^\'\"]*(?:loading\.gif|logo_desy\.gif|arrow_large_white\.png)[^\'\"]*[\'\"])??[^>]*/?>', re.IGNORECASE),
            re.compile(r'<[^>]*(?:role\s*=\s*[\'\"]navigation[\'\"]|aria-label\s*=\s*[\'\"][^\'\"]*[\'\"])??[^>]*>.*?</[^>]+>', re.DOTALL | re.IGNORECASE),
            re.compile(r'<ul\b[^>]*>(?:\s*<li\b[^>]*(?:class|id)\s*=\s*[\'\"][^\'\"]*\b(?:inactive|ZMSFolder\d*|ZMSDocument\d*)\b[^\'\"]*[\'\"][^>]*>.*?</li>\s*)+</ul>', re.DOTALL | re.IGNORECASE),
        ]

        self.specialized_patterns = [
            re.compile(r'Deutsches\s+Elektronen-Synchrotron\s+DESY\s+A\s+Research\s+Centre\s+of\s+the\s+Helmholtz\s+Association', re.IGNORECASE),
            re.compile(r'Data\s+Privacy\s+Policy\s*\|\s*Declaration\s+of\s+Accessibility\s*\|\s*Imprint\s*©[^.]*', re.IGNORECASE),
            re.compile(r'A\s+Research\s+Centre\s+of\s+the\s+Helmholtz\s+Association', re.IGNORECASE),
            re.compile(r'©\s*\d{4}\s*Deutsches\s+Elektronen-Synchrotron\s+DESY.*?(?:Helmholtz\s+Association)?', re.IGNORECASE),
            re.compile(r'Deutsches\s*Elektronen-Synchrotron', re.IGNORECASE),
            re.compile(r'Data\s+Privacy\s+Policy\s*\|.*?(?:Imprint|©)', re.IGNORECASE),
            re.compile(r'Impressum\s*/\s*Datenschutz\s*/\s*Erklärung\s+zur\s+Barrierefreiheit', re.IGNORECASE),
            re.compile(r'\bSprungnavigation\b', re.IGNORECASE),
            re.compile(r'\bZielgruppennavigation\b', re.IGNORECASE),
            re.compile(r'\bServicefunktionen\b', re.IGNORECASE),
            re.compile(r'\bBreadcrumb\b', re.IGNORECASE),
            re.compile(r'\bFooter\b', re.IGNORECASE),
            re.compile(r'\bDesy\s+Global\b', re.IGNORECASE),
            re.compile(r'\bZum\s+Untermenü\b', re.IGNORECASE),
            re.compile(r'\bZum\s+Inhalt\b', re.IGNORECASE),
            re.compile(r'\bZum\s+Hauptmenu\b', re.IGNORECASE),
            re.compile(r'\bInfos\s*&\s*Services\b', re.IGNORECASE),
            re.compile(r'\bLeichte\s+Sprache\b', re.IGNORECASE),
            re.compile(r'\bGebärdensprache\b', re.IGNORECASE),
        ]

        self.cleanup_patterns = [
            re.compile(r'<!--\s*(?://wrapper\s*//\s*-->.*?<!--\s*/standard_html_header\s*--|/?\s*standard_html_header\s*-->)', re.DOTALL | re.IGNORECASE),
            re.compile(r'<!--[^>]*(?:wrapper|overall|standard_html)[^>]*-->', re.DOTALL | re.IGNORECASE),
            re.compile(r'<!--[^>]*tal:attributes[^>]*-->', re.IGNORECASE),
            re.compile(r'<!--a\s+tal:.*?</a-->', re.DOTALL | re.IGNORECASE),
            re.compile(r'<svg[^>]*>.*?</svg>', re.DOTALL | re.IGNORECASE),
            re.compile(r'title\s*=\s*[\'\"][^\'\"]*(?:Aktuelle|Seminare|Events)[^\'\"]*[\'\"]', re.IGNORECASE),
            re.compile(r'<[^>]*style\s*=\s*[\'\"][^\'\"]*(?:display\s*:\s*block|text-align\s*:\s*right|margin|opacity)[^\'\"][\'\"][^>]*>', re.IGNORECASE),
        ]

        self.text_cleanup_patterns = [
            re.compile(r'\bNavigation\b', re.IGNORECASE),
            re.compile(r'\bDatenschutzerklärung\b', re.IGNORECASE),
            re.compile(r'\bErklärung\s+zur\s+Barrierefreiheit\b', re.IGNORECASE),
            re.compile(r'\bBack\s+to\s+Top\b', re.IGNORECASE),
            re.compile(r'\b(?:nav|menu|breadcrumb|navigation)\s*[:\-\|]\s*', re.IGNORECASE),
            re.compile(r'\b(?:Home|Startseite|Kontakt|Suche|Login|Anmelden)\b', re.IGNORECASE),
            re.compile(r'\b(?:Archiv|Archive)\s*\d{4}', re.IGNORECASE),
            re.compile(r'\b(?:Page\s+\d+|Seite\s+\d+|\d+\s+of\s+\d+)\b', re.IGNORECASE),
            re.compile(r'\b(?:cookie|gdpr|popup|consent)\b', re.IGNORECASE),
        ]

        self.whitespace_pattern = re.compile(r'[\xa0\u202f\n\r\t\s]+')

        self.max_depth = max_depth
        self.content_tags = content_tags or [
            "p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "ul", "ol",
            "td", "th", "tr", "table", "caption", "dt", "dd", "span",
            "article", "section", "main", "div",
            "div.teaser-text", "div.content", "div.text-block",
            "div.publication-item", "div.news-item", "div.portlet-body",
            "div.event-details", "div.indico-content", "div.publication-list",
            "div.event-description", "div.news-content", "div.status-report",
            "div.status", "div.monitor", "div.experiment", "div.results",
            "p[id]", "table.i-table", "div.timetable",
        ]
        self.excluded_keywords = excluded_keywords or ["cookie", "privacy", "disclaimer", "login", "password"]
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.batch_size = batch_size
        self.timeout = timeout
        self.processed_hashes: Set[str] = set()
        self.full_text_hashes: Set[str] = set()
        self.processed_urls: Set[str] = set()
        self.max_hashes = 100000
        self.max_urls = 10000
        self.error_urls: Dict[str, str] = {}
        self.redirected_urls: Dict[str, str] = {}
        self.js_wait_time = js_wait_time
        self.js_scroll = js_scroll
        self.session: Optional[aiohttp.ClientSession] = None
        self.progress_bar = None
        self.ssl_bypass_domains: Set[str] = set()
        self.url_to_documents_map: Dict[str, List[Document]] = {}
        self.page_character_counts: Dict[str, Dict] = {}
        self.debug_mode = False

        self.domain_configs: Dict[str, Dict] = {
            "petra3.desy.de": {"timeout": 500, "max_connections": 2, "retry_delay": 3, "js_wait_time": 12000},
            "indico.desy.de": {"timeout": 500, "max_connections": 2, "retry_delay": 5, "js_wait_time": 15000},
            "pitz.desy.de": {"timeout": 500, "max_connections": 2, "retry_delay": 3, "js_wait_time": 12000},
            "www.desy.de": {"timeout": 900, "max_connections": 1, "retry_delay": 3, "js_wait_time": 60000},
            "desy.de": {"timeout": 500, "max_connections": 1, "retry_delay": 3, "js_wait_time": 30000},
            "newsletter.desy.de": {"timeout": 900, "max_connections": 1, "retry_delay": 3, "js_wait_time": 60000},
            "connect.desy.de": {"timeout": 500, "max_connections": 1, "retry_delay": 3, "js_wait_time": 30000},
            "astroparticle-physics.desy.de": {"timeout": 500, "max_connections": 1, "retry_delay": 3, "js_wait_time": 30000},
            "innovation.desy.de": {"timeout": 500, "max_connections": 1, "retry_delay": 3, "js_wait_time": 30000},
            "petra4.desy.de": {"timeout": 900, "max_connections": 1, "retry_delay": 3, "js_wait_time": 90000},
            "accelerators.desy.de": {"timeout": 900, "max_connections": 1, "retry_delay": 3, "js_wait_time": 60000},
            "v22.desy.de": {"timeout": 500, "max_connections": 1, "retry_delay": 3, "js_wait_time": 30000},
            "photon-science.desy.de": {"timeout": 500, "max_connections": 1, "retry_delay": 3, "js_wait_time": 30000},
            "particle-physics.desy.de": {"timeout": 900, "max_connections": 1, "retry_delay": 3, "js_wait_time": 60000},
            "pr.desy.de": {"timeout": 500, "max_connections": 1, "retry_delay": 3, "js_wait_time": 30000},
            "fh.desy.de": {"timeout": 900, "max_connections": 1, "retry_delay": 3, "js_wait_time": 60000},
        }
        self.default_domain_config = {"timeout": timeout, "max_connections": 10, "retry_delay": 2}

    def add_to_processed_hashes(self, content_hash: str) -> None:
        if len(self.processed_hashes) >= self.max_hashes:
            self.processed_hashes = set(list(self.processed_hashes)[self.max_hashes // 2 :])
        self.processed_hashes.add(content_hash)

    def add_to_processed_urls(self, url: str) -> None:
        if len(self.processed_urls) >= self.max_urls:
            self.processed_urls = set(list(self.processed_urls)[self.max_urls // 2 :])
        self.processed_urls.add(url)

    def track_page_character_count(self, url: str, content: str, title: str = "", language: str = "en", depth: int = 0) -> None:
        if len(content) < self.MIN_CHUNK_CHARS:
            return
        self.page_character_counts[url] = {
            'url': url,
            'title': title,
            'character_count': len(content),
            'word_count': len(content.split()) if content else 0,
            'language': language,
            'depth': depth,
        }

    def should_skip_url(self, url: str) -> bool:
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        return any(path.endswith(ext) for ext in self.NON_HTML_EXTENSIONS)

    def detect_language(self, soup: BeautifulSoup, text_sample: str, url: str | None = None) -> str:
        if url and url.lower().endswith('_ger.html'):
            return 'de'
        if text_sample and len(text_sample) >= 50:
            try:
                return detect(text_sample[:1000])
            except Exception:
                pass
        html_lang = None
        if soup.html and soup.html.get('lang'):
            html_lang = soup.html.get('lang').strip().lower()
        if not html_lang and soup.html and soup.html.get('xml:lang'):
            html_lang = soup.html.get('xml:lang').strip().lower()
        if not html_lang:
            meta_lang = soup.find('meta', attrs={'http-equiv': 'content-language'})
            if meta_lang and meta_lang.get('content'):
                html_lang = meta_lang.get('content').strip().lower()
        if not html_lang:
            meta_lang = soup.find('meta', attrs={'property': 'og:locale'})
            if meta_lang and meta_lang.get('content'):
                html_lang = meta_lang.get('content').strip().lower()
        if html_lang:
            html_lang = re.sub(r'[^a-z]', '', html_lang.split('-')[0].lower())
            if len(html_lang) == 2:
                return html_lang
        return 'en'

    def _apply_pattern_group(self, text: str, patterns: List[re.Pattern], group_name: str) -> str:
        if not patterns:
            return text
        for pattern in patterns:
            text = pattern.sub('', text)
        return text

    def clean_content(self, text: str) -> str:
        if not text:
            return ""
        try:
            soup = BeautifulSoup(text, 'html.parser')
            main_content = None
            main_content_selectors = [
                'main', 'article', 'section[class*="content"]',
                'div[class*="main-content"]', 'div[class*="content-section"]', 'div[class*="text-block"]',
                'div[id="content"]', 'div[id="main"]', 'div[id="bodyContent"]',
                'div[class*="content"]', 'div[class*="text"]', 'div[class*="body"]',
                'div[class*="page"]', 'div[class*="container"]', 'center'
            ]
            # for selector in main_content_selectors:
            #     found = soup.select_one(selector)
            #     if found is not None:
            #         main_content = found
            #         break
    #Sara: we have to check if this is correct, maybe we should keep the original code  
            for selector in main_content_selectors:
                main_content = soup.select_one(selector)
             
            if not main_content:
                main_content = soup.body or soup

            selectors = [
                'div[id="overall"]', 'div[class="wrapper"]', 'header[id="header"]',
                'div[id="mobile_menu_header"]', 'div[id="mobile_menu"]', 'div[id="mobile_dropdown"]',
                'div[id="top"]', 'div[id="logoarea"]', 'div[id="topleft"]', 'div[id="topright"]',
                'div[id="topmenu"]', 'nav[id="menu"]', 'ul[id="main_menu"]',
                'nav', 'ul[id*="menu" i]', 'ol[id*="menu" i]',
                'div[id="icons"]', 'div[class="topright_button"]',
                'li[class*="ZMS"]', 'a[class*="ZMS"]',
                'img[class="imgNoborder"]', 'img[id*="logo"]', 'img[id*="icon"]',
                'a[target="_blank"]', 'a[href*="doi.org"]', 'a[href*="DOI"]',
                'a[href*="journals.aps.org"]', 'a[href*="dx.doi.org"]', 'a[href*="doi:"]',
                'a[href*="abstract"]', 'a[href*="citation"]',
                'div[class="clear"]', 'div[class="loading"]',
                'footer', 'div[id*="footer" i]', 'div[class*="footer" i]', 'div[class*="copyright" i]',
                'div[class*="teaser" i]', 'div[class*="LinkElement" i]', 'div[class*="quicklinks" i]', 
                'div[class*="ZMS" i]', 'div[id*="teaser" i]', 'div[id*="quicklinks" i]',
                '[data-cookie]', '[data-consent]', '[class*="cookie" i]', '[class*="consent" i]', 
                '[style*="display:none" i]', '[style*="visibility:hidden" i]',
                'div[id="quick_nav_container"]',
                'a[href*="data_privacy_policy"]', 'a[href*="declaration_of_accessibility"]',
                'ul[style*="padding-bottom"]',
                'button[class*="btt"]', 'div[class*="btt"]',
                'ul[class*="footer__links"]', 'div[class*="footer__logos"]',
                'img[alt*="Logo"]', 'a[href*="linkedin"]', 'a[href*="twitter"]',
                'li[class*="ZMSFolder"]', 'li[class*="ZMSDocument"]',
                'a[class*="ZMSFolder"]', 'a[class*="ZMSDocument"]',
                'p.hidden.showforprint',
                'p[class*="hidden"][class*="showforprint" i]',
                '[class*="showforprint" i]', '[class*="show-for-print" i]',
                '[class*="hidden" i][class*="print" i]',
                '[class~="showforprint"]', '[class~="hidden"]',
                'a[class*="print" i]', 'a[class*="changelang" i]',
                'nav', 'header', 'footer',
                'div[class*="nav" i]', 'div[id*="nav" i]',
                'div[class*="menu" i]', 'div[id*="menu" i]',
                'ul[class*="menu" i]', 'ul[id*="menu" i]',
                'li[class*="menu" i]', 'li[id*="menu" i]',
                'a[class*="menu" i]', 'a[id*="menu" i]',
                'section[class*="nav" i]', 'section[class*="menu" i]',
                'ul[class*="nav" i]',
                'ul[id*="nav" i]',
                'div[id*="content-nav" i]',
                'div[id="page-footer"]',
                'ul[id="footer-nav"]',
            ]
            for selector in selectors:
                elements = set(main_content.select(selector)) | set(soup.select(selector))
                for element in elements:
                    if getattr(element, 'extractable', True):
                        element.decompose()
            for li in main_content.find_all("li"):
                if not li.find_parent(id="content"):
                    li.decompose()
            doi_href_pattern = re.compile(r'(doi\.org|journals\.aps\.org|dx\.doi\.org|DOI:)', re.IGNORECASE)
            for a_tag in main_content.find_all('a', href=True):
                if doi_href_pattern.search(a_tag['href']):
                    a_tag.decompose()
            text = str(main_content)
        except Exception:
            text = re.sub(r'<[^>]+>', ' ', text)

        text = self._apply_pattern_group(text, self.critical_patterns, "CRITICAL")
        text = self._apply_pattern_group(text, self.high_priority_patterns, "HIGH")
        text = self._apply_pattern_group(text, self.medium_priority_patterns, "MEDIUM")
        text = self._apply_pattern_group(text, self.low_priority_patterns, "LOW")
        text = self._apply_pattern_group(text, self.specialized_patterns, "SPECIALIZED")
        text = self._apply_pattern_group(text, self.cleanup_patterns, "CLEANUP")

        if '<' in text and '>' in text:
            try:
                soup = BeautifulSoup(text, 'html.parser')
                for el in soup.find_all(text=re.compile(r'©\s*\d{4}\s*Deutsches\s*Elektronen-Synchrotron\s*DESY', re.I)):
                    el.replace_with('')
                for el in soup.find_all(text=True):
                    text_content = (el.string or "").lower()
                    for pattern in self.cookie_text_patterns:
                        if re.search(pattern, text_content, re.I):
                            parent = el.parent
                            for _ in range(4):
                                if parent and parent.name in ['div', 'section', 'aside', 'p', 'span']:
                                    parent.decompose()
                                    break
                                parent = parent.parent if parent else None
                            break
                text = soup.get_text(separator=' ', strip=True)
            except Exception:
                text = re.sub(r'<[^>]+>', ' ', text)

        text = self._apply_pattern_group(text, self.text_cleanup_patterns, "TEXT_CLEANUP")
        text = self.whitespace_pattern.sub(' ', text)

        doi_pattern = re.compile(r'\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b', re.IGNORECASE)
        seen_dois: Set[str] = set()

        def replace_doi(match: re.Match) -> str:
            doi = match.group(0)
            if doi in seen_dois:
                return ''
            seen_dois.add(doi)
            return doi

        text = doi_pattern.sub(replace_doi, text)
        return re.sub(r'\s+', ' ', text).strip()

    def is_login_page(self, soup: BeautifulSoup) -> bool:
        login_indicators = [
            soup.find('form', {'id': lambda x: x and 'login' in x.lower()}),
            soup.find('form', {'action': lambda x: x and 'login' in x.lower()}),
            soup.find('input', {'name': 'username'}),
            soup.find('input', {'name': 'password', 'type': 'password'}),
            soup.find('button', text=re.compile(r'log\s*in|sign\s*in', re.I)),
            soup.find('input', {'value': re.compile(r'log\s*in|sign\s*in', re.I)}),
            soup.find('div', class_=['login-box', 'auth-form']),
            soup.find('a', text=re.compile(r'log\s*in|sign\s*in|authenticate', re.I)),
        ]
        title_matches = soup.title and re.search(r'log\s*in|sign\s*in', soup.title.text, re.I)
        return bool(title_matches or any(login_indicators))

    def is_not_found_page(self, soup: BeautifulSoup) -> bool:
        error_phrases = [
            'not found', "page doesn't exist", '404', 'page not found', 'does not exist',
            'could not be found', 'site error', 'error was encountered', 'error occurred'
        ]
        page_text = soup.get_text(strip=True).lower()
        if soup.title and any(phrase in soup.title.text.lower() for phrase in error_phrases):
            return True
        if re.search(r'error.*encountered.*publishing', page_text, re.I):
            return True
        for heading in soup.find_all(['h1', 'h2', 'h3']):
            if any(phrase in heading.get_text(strip=True).lower() for phrase in error_phrases):
                return True
        if any(phrase in page_text for phrase in error_phrases):
            return True
        if len(page_text) < self.MIN_TEXT_SAMPLE_LENGTH:
            return True
        return False

    def extract_list_metadata(self, soup: BeautifulSoup) -> str:
        content_parts: List[str] = []
        lists = soup.find_all(['ul', 'ol', 'dl'], class_=['publication-list', 'pub-list'])
        for list_elem in lists:
            items = list_elem.find_all(['li', 'dt', 'dd'])
            if len(items) > 1:
                list_content: List[str] = []
                for item in items:
                    item_text = item.get_text(strip=True)
                    if item_text and len(item_text) > 10:
                        if any(k in item_text.lower() for k in ['author', 'title', 'journal', 'doi', 'isbn', 'vol', 'pp', 'year', '20']):
                            list_content.append(item_text)
                if len(list_content) > 2:
                    content_parts.extend(list_content)
        return "\n".join(content_parts)

    def extract_table_metadata(self, soup: BeautifulSoup) -> str:
        content_parts: List[str] = []
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
            table_text = table.get_text(strip=True).lower()
            if any(k in table_text for k in ['author', 'title', 'journal', 'publication', 'presenter', 'date', 'conference']):
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) > 1:
                        cell_texts = []
                        for cell in cells:
                            cell_text = cell.get_text(strip=True)
                            if cell_text and len(cell_text) > 3:
                                cell_texts.append(cell_text)
                        if cell_texts:
                            content_parts.append(" | ".join(cell_texts))
            else:
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) > 1:
                        cell_texts = [cell.get_text(strip=True) for cell in cells if cell.get_text(strip=True)]
                        if len(cell_texts) > 1 and any(len(t) > 15 for t in cell_texts):
                            content_parts.append(" | ".join(cell_texts))
        return "\n".join(content_parts)

    def extract_content(self, soup: BeautifulSoup, use_tags: bool = True) -> Tuple[str, str]:
        if not soup:
            return "", ""
        all_text_parts: List[str] = []
        processed_elements: Set[int] = set()
        seen_text_hashes: Set[str] = set()

        selectors = (
            '[id*="nav" i], [class*="nav" i], '
            '[id*="menu" i], [class*="menu" i], '
            '[id*="sidebar" i], [class*="sidebar" i], '
            '[id*="quicklinks" i], [class*="quicklinks" i], '
            'p.copyright, div.copyright, footer, '
            '[class*="footer" i], [id*="footer" i], '
            '[class*="impressum" i], [id*="impressum" i], '
            '[class*="datenschutz" i], [id*="datenschutz" i], '
            '[class*="legal" i], [id*="legal" i], '
            '[class*="social" i], [id*="social" i], '
            '[class*="share" i], [id*="share" i], '
            '[class*="links" i], [id*="links" i], '
            '[class*="bottom" i], [id*="bottom" i], '
            '[class*="contact" i], [id*="contact" i], '
            '[class*="mastodon" i], [class*="facebook" i], '
            '[class*="instagram" i], [class*="linkedin" i], '
            '[class*="twitter" i], [class*="rss" i], '
            'a[href*="impressum"], a[href*="datenschutz"], '
            'a[href*="privacy"], a[href*="accessibility"], '
            'a[href*="kontakt"], a[href*="contact"], '
            'a[href*="social"], a[href*="linkedin"], '
            'a[href*="twitter"], a[href*="facebook"], '
            'a[href*="instagram"], a[href*="mastodon"], '
            'a[href*="rss"]'
        )
        for nav_elem in soup.select(selectors):
            nav_elem.decompose()

        def should_skip_element(element) -> bool:
            return (
                element.get('id') in ['cookie-bar', 'footer', 'page-footer', 'site-footer'] or 
                any(cls in element.get('class', []) for cls in [
                    'cookie-bar', 'LinkElementTitle', 'ZMSTeaserContainer', 'footer', 'copyright',
                    'link', 'site-footer', 'ZMSDocument0'
                ]) or 
                element.name == 'li' or 
                element.find_parent('li') or 
                element.find_parent(attrs={'id': re.compile(r'(footer|page-footer|site-footer)', re.I)})
            )

        comprehensive_tags = [
            'p[id]', 'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
            'div.content-section', 'div.module', 'div.text', 'div.content',
            'div.text-block', 'div.main-content', 'div.publication-item',
            'div.news-item', 'div.event-details', 'div.news-content',
            'div.status-report', 'div.status', 'div.monitor',
            *self.content_tags,
            'table', 'table.i-table', 'caption', 'td', 'th', 'tr',
            'section', 'article', 'main', 'span', 'div',
        ]

        for tag in comprehensive_tags:
            if "." in tag:
                tag_name, tag_class = tag.split(".", 1)
                elements = soup.find_all(tag_name, class_=tag_class)
            elif tag.startswith('p['):
                elements = soup.find_all('p', id=True)
            else:
                elements = soup.find_all(tag)
            for element in elements:
                if id(element) in processed_elements or should_skip_element(element):
                    continue
                if any(id(ancestor) in processed_elements for ancestor in element.parents):
                    continue
                if any(id(descendant) in processed_elements for descendant in element.descendants if hasattr(descendant, 'name')):
                    continue
                raw_html = str(element)
                cleaned_text = self.clean_content(raw_html)
                cleaned_text = self._apply_pattern_group(cleaned_text, self.critical_patterns, "CRITICAL")
                cleaned_text = self._apply_pattern_group(cleaned_text, self.high_priority_patterns, "HIGH")
                cleaned_text = self._apply_pattern_group(cleaned_text, self.medium_priority_patterns, "MEDIUM")
                cleaned_text = self._apply_pattern_group(cleaned_text, self.low_priority_patterns, "LOW")
                cleaned_text = self._apply_pattern_group(cleaned_text, self.specialized_patterns, "SPECIALIZED")
                cleaned_text = self._apply_pattern_group(cleaned_text, self.cleanup_patterns, "CLEANUP")
                if not cleaned_text or len(cleaned_text) < self.MIN_CHUNK_CHARS:
                    continue
                normalized_for_hash = re.sub(r'\s+', ' ', cleaned_text.lower().strip())
                content_hash = hashlib.md5(normalized_for_hash.encode()).hexdigest()
                if content_hash in seen_text_hashes:
                    continue
                seen_text_hashes.add(content_hash)
                processed_elements.add(id(element))
                for descendant in element.descendants:
                    if hasattr(descendant, 'name'):
                        processed_elements.add(id(descendant))
                all_text_parts.append(cleaned_text)
        content = "\n".join(all_text_parts)
        try:
            soup_final = BeautifulSoup(content, 'html.parser')
            for el in soup_final.find_all(text=re.compile(r'©\s*\d{4}.*?DESY', re.I)):
                el.replace_with('')
            for el in soup_final.find_all(text=True):
                text_content = (el.string or "").lower()
                for pattern in self.cookie_text_patterns:
                    if re.search(pattern, text_content, re.I):
                        parent = el.parent
                        for _ in range(4):
                            if parent and parent.name in ['div', 'section', 'aside', 'p', 'span']:
                                parent.decompose()
                                break
                            parent = parent.parent if parent else None
                        break
            content = soup_final.get_text(separator=' ', strip=True)
        except Exception:
            pass
        content = self._apply_pattern_group(content, self.text_cleanup_patterns, "TEXT_CLEANUP")
        content = self.whitespace_pattern.sub(' ', content)
        doi_pattern = re.compile(r'\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b', re.IGNORECASE)
        seen_dois: Set[str] = set()
        def replace_doi(match: re.Match) -> str:
            doi = match.group(0)
            if doi in seen_dois:
                return ''
            seen_dois.add(doi)
            return doi
        content = doi_pattern.sub(replace_doi, content)
        return content, content

    def create_chunks(self, text: str, metadata: Dict, chunk_type: str = "character") -> List[Document]:
        if not text:
            return []
        cleaned_text = self.clean_content(text)
        if not cleaned_text:
            return []
        def split_text_by_size(cleaned_text: str, max_size: int, overlap_size: int, min_chars: int) -> List[str]:
            if len(cleaned_text) <= max_size:
                return [cleaned_text]
            chunks: List[str] = []
            start = 0
            min_chunk_size = max(max_size // 2, max_size - overlap_size)
            text_len = len(cleaned_text)
            sentence_pattern = re.compile(r'[.!?]\s+|[.!?]$|\n\s*\n')
            while start < text_len:
                end = min(start + max_size, text_len)
                if end < text_len:
                    search_start = max(end - int(max_size * 0.3), start + min_chunk_size)
                    search_zone = cleaned_text[search_start:end]
                    matches = list(sentence_pattern.finditer(search_zone))
                    if matches:
                        match = matches[-1]
                        match_end = search_start + match.end()
                        end = match_end - len(match.group().lstrip('.!?'))
                    else:
                        word_boundary = cleaned_text.rfind(' ', search_start, end)
                        if word_boundary > start:
                            end = word_boundary
                chunk = cleaned_text[start:end].strip()
                if len(chunk) >= min_chars:
                    chunks.append(chunk)
                if end >= text_len:
                    break
                ideal_next_start = end - overlap_size
                if ideal_next_start <= start:
                    next_start = start + min_chunk_size
                else:
                    word_start = cleaned_text.find(' ', ideal_next_start)
                    next_start = word_start + 1 if word_start != -1 and word_start < end else ideal_next_start
                start = next_start
            return chunks
        # NOTE: Match notebook behavior: split on original input text, not cleaned
        texts = split_text_by_size(text, self.chunk_size, self.chunk_overlap, self.MIN_CHUNK_CHARS)
        section_title = metadata.get("section_title", "")
        section_level = metadata.get("section_level", 0)
        total_chunks = len(texts)
        unique_chunks: List[Document] = []
        chunk_fingerprints: Set[str] = set()
        for i, chunk in enumerate(texts):
            normalized_text = re.sub(r'\s+', ' ', chunk.strip().lower())
            text_fingerprint = hashlib.md5((normalized_text + section_title).encode()).hexdigest()
            if text_fingerprint in chunk_fingerprints:
                continue
            chunk_fingerprints.add(text_fingerprint)
            content_hash = hashlib.md5(chunk.encode()).hexdigest()
            if content_hash not in self.processed_hashes:
                self.add_to_processed_hashes(content_hash)
                chunk_metadata = {
                    **metadata,
                    "chunk_index": i,
                    "total_chunks": total_chunks,
                    "chunk_type": chunk_type,
                    "section_title": section_title,
                    "section_level": section_level,
                }
                if i > 0:
                    chunk_metadata["continued"] = True
                unique_chunks.append(Document(page_content=chunk, metadata=chunk_metadata))
        return unique_chunks

    def create_full_text_chunks(self, text: str, metadata: Dict, chunk_type: str = "full_text") -> List[Document]:
        if not text:
            return []
        cleaned_text = self.clean_content(text)
        if not cleaned_text or len(cleaned_text) < self.MIN_CHUNK_CHARS:
            return []
        def split_text_for_full_coverage(t: str, max_size: int) -> List[str]:
            if len(t) <= max_size:
                return [t]
            chunks: List[str] = []
            start = 0
            text_len = len(t)
            while start < text_len:
                end = min(start + max_size, text_len)
                chunk = t[start:end].strip()
                if chunk:
                    chunks.append(chunk)
                start = end
            return chunks
        texts = split_text_for_full_coverage(cleaned_text, self.chunk_size)
        section_title = metadata.get("section_title", "")
        section_level = metadata.get("section_level", 0)
        total_chunks = len(texts)
        unique_chunks: List[Document] = []
        chunk_fingerprints: Set[str] = set()
        for i, chunk in enumerate(texts):
            normalized_text = re.sub(r'\s+', ' ', chunk.strip().lower())
            text_fingerprint = hashlib.md5((normalized_text + section_title).encode()).hexdigest()
            if text_fingerprint in chunk_fingerprints:
                continue
            chunk_fingerprints.add(text_fingerprint)
            content_hash = hashlib.md5(chunk.encode()).hexdigest()
            if content_hash not in self.full_text_hashes:
                self.full_text_hashes.add(content_hash)
                chunk_metadata = {
                    **metadata,
                    "chunk_index": i,
                    "total_chunks": total_chunks,
                    "chunk_type": "full_text",
                    "section_title": section_title,
                    "section_level": section_level,
                }
                if i > 0:
                    chunk_metadata["continued"] = True
                unique_chunks.append(Document(page_content=chunk, metadata=chunk_metadata))
        return unique_chunks

    def create_structure_based_chunks(self, soup: BeautifulSoup, url: str, depth: int, language: str) -> List[Document]:
        if self.is_login_page(soup) or self.is_not_found_page(soup):
            return []
        chunks: List[Document] = []
        processed_elements: Set[int] = set()
        page_title = soup.title.text.strip() if soup.title else "No title"
        header_tags = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']

        def add_section_to_chunks(section: Dict, metadata: Dict) -> List[Document]:
            if not section["content"]:
                return []
            content_text = "\n".join(section["content"]).strip()
            full_text = f"{section['title']}\n\n{content_text}" if section["title"] else content_text
            if len(full_text) < self.MIN_CHUNK_CHARS:
                return []
            cleaned_content, _ = self.extract_content(BeautifulSoup(full_text, 'html.parser'), use_tags=False)
            if not cleaned_content:
                return []
            c = self.create_chunks(cleaned_content, metadata, chunk_type="structural")
            return [chunk for chunk in c if len(chunk.page_content) >= self.MIN_CHUNK_CHARS]

        section_tags = [
            "section", "article", "main", "div.content-section", "div.module", "div.text",
            "div.content", "div.text-block", "div.main-content", "div.container", "div.row",
            "div.card", "div.content-main", "div.teaser-text", "div.publication-item",
            "div.news-item", "div.portlet-body", "div.event-details", "div.indico-content",
            "div.publication-list", "div.event-description", "div.news-content",
            "div.status-report", "div.status", "div.monitor", "div.experiment", "div.results",
            "div.timetable", "p", "p[id]", "span", "table", "table.i-table", "caption",
            "td", "th", "tr", "ul", "ol", "li", *header_tags,
        ]

        for tag in section_tags:
            if "." in tag:
                tag_name, tag_class = tag.split(".", 1)
                elements = soup.find_all(tag_name, class_=tag_class)
            elif tag.startswith('p['):
                elements = soup.find_all('p', id=True)
            else:
                elements = soup.find_all(tag)
            for element in elements:
                if id(element) in processed_elements:
                    continue
                if any(id(ancestor) in processed_elements for ancestor in element.parents):
                    continue
                text = element.get_text(separator=" ", strip=True)
                if text and len(text) > self.MIN_INITIAL_CHARS:
                    processed_elements.add(id(element))
                    for descendant in element.descendants:
                        if hasattr(descendant, 'name'):
                            processed_elements.add(id(descendant))
                    metadata = {
                        "source": url,
                        "section_title": element.find(header_tags).get_text(strip=True) if element.find(header_tags) else page_title,
                        "section_level": 1,
                        "language": language,
                        "depth": depth,
                        "title": page_title,
                    }
                    section = {"title": metadata["section_title"], "content": [text], "level": 1}
                    chunks.extend(add_section_to_chunks(section, metadata))

        if not chunks:
            active_sections: Dict[int, Dict] = {}
            elements = soup.find_all([*header_tags, 'p', 'li', 'td'])
            for element in elements:
                tag_name = element.name
                if tag_name in header_tags:
                    text = element.get_text(strip=True)
                    if not text:
                        continue
                    level = int(tag_name[1])
                    for i in range(level, 7):
                        if i in active_sections:
                            metadata = {
                                "source": url,
                                "section_title": active_sections[i]["title"],
                                "section_level": active_sections[i]["level"],
                                "language": language,
                                "depth": depth,
                                "title": page_title,
                            }
                            chunks.extend(add_section_to_chunks(active_sections[i], metadata))
                            del active_sections[i]
                    active_sections[level] = {"title": text, "content": [], "level": level}
                else:
                    text = element.get_text(strip=True)
                    if text and active_sections:
                        active_sections[max(active_sections.keys())]["content"].append(text)
            for level in sorted(active_sections.keys()):
                metadata = {
                    "source": url,
                    "section_title": active_sections[level]["title"],
                    "section_level": active_sections[level]["level"],
                    "language": language,
                    "depth": depth,
                    "title": page_title,
                }
                chunks.extend(add_section_to_chunks(active_sections[level], metadata))

        if not chunks:
            body = soup.find('body') or soup
            body_text = body.get_text(separator=" ", strip=True)
            if body_text:
                metadata = {
                    "source": url,
                    "section_title": page_title,
                    "section_level": 0,
                    "language": language,
                    "depth": depth,
                    "title": page_title,
                }
                chunks.extend(self.create_chunks(body_text, metadata, chunk_type="structural"))
        return chunks

    async def create_session(self, url: str | None = None) -> aiohttp.ClientSession:
        async with self.session_lock:
            if not hasattr(self, 'session_request_count'):
                self.session_request_count = 0
            if self.session and not self.session.closed and self.session_request_count > 50:
                await self.session.close()
                self.session = None
                self.session_request_count = 0
                await asyncio.sleep(0.1)
            if self.session and not self.session.closed:
                return self.session
            domain = urlparse(url).netloc if url else None
            domain_config = self.domain_configs.get(domain, self.default_domain_config)
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/14.0 Safari/605.1.15',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            ]
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    limit=domain_config.get("max_connections", 10),
                    ttl_dns_cache=300,
                    ssl=ssl_context,
                    force_close=False,
                    enable_cleanup_closed=True,
                    resolver=aiohttp.AsyncResolver(),
                ),
                timeout=aiohttp.ClientTimeout(total=self.timeout, connect=10, sock_read=20),
                headers={
                    'User-Agent': random.choice(user_agents),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Connection': 'keep-alive',
                },
            )
            return self.session

    async def close_session(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    async def fetch_simple(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                parsed_url = urlparse(url)
                domain = parsed_url.netloc
                domain_config = self.domain_configs.get(domain, self.default_domain_config)
                session = await self.create_session(url)
                headers = {
                    "Referer": random.choice([
                        "https://google.com",
                        "https://duckduckgo.com",
                        "https://www.bing.com",
                        f"https://{parsed_url.netloc}/",
                    ]),
                    "Origin": f"{parsed_url.scheme}://{parsed_url.netloc}",
                }
                timeout_config = aiohttp.ClientTimeout(
                    total=domain_config.get("timeout", self.timeout),
                    connect=60,
                    sock_connect=30,
                    sock_read=120,
                )
                await asyncio.sleep(random.uniform(0.5, 1.5))
                async with session.get(url, headers=headers, allow_redirects=True, timeout=timeout_config) as response:
                    self.session_request_count += 1
                    if response.status == 200:
                        resolved_url = str(response.url)
                        text = await response.text(errors='replace')
                        if len(text.strip()) < 500 or 'access denied' in text.lower() or 'javascript required' in text.lower():
                            self.error_urls[url] = "Soft block or empty content"
                            return await self.fetch_with_js(url)
                        if resolved_url != url:
                            self.redirected_urls[url] = resolved_url
                            if resolved_url not in self.processed_urls:
                                return text
                            else:
                                self.add_to_processed_urls(url)
                                return None
                        else:
                            return text
                    else:
                        self.error_urls[url] = f"HTTP status: {response.status}"
                        return None
            except Exception as e:
                if attempt == 2:
                    self.error_urls[url] = f"{str(e)}"
                    return None
                await asyncio.sleep(2 ** attempt)

    async def init_browser(self) -> None:
        async with self.browser_lock:
            if self.browser is None or self.context is None:
                self.playwright = await async_playwright().start()
                self.browser = await self.playwright.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu-vsync"],
                )

    async def fetch_with_js(self, url: str) -> Optional[str]:
        for attempt in range(3):
            try:
                async with self.js_semaphore:
                    if self.browser is None:
                        await self.init_browser()
                    user_agents = [
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/14.0 Safari/605.1.15',
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
                    ]
                    chosen_user_agent = random.choice(user_agents)
                    referers = [
                        "https://google.com",
                        f"https://{urlparse(url).netloc}/",
                        f"https://{urlparse(url).netloc}/index.html",
                        "https://www.desy.de/",
                        "https://desy.de/",
                    ]
                    chosen_referer = random.choice(referers)
                    extra_headers = {
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.5",
                        "Connection": "keep-alive",
                        "Referer": chosen_referer,
                        "Origin": f"{urlparse(url).scheme}://{urlparse(url).netloc}",
                        "DNT": "1",
                        "Sec-Fetch-Site": "cross-site",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Dest": "document",
                        "Upgrade-Insecure-Requests": "1",
                    }
                    context = await self.browser.new_context(
                        user_agent=chosen_user_agent,
                        locale="en-US",
                        viewport={"width": 1280, "height": 800},
                        extra_http_headers=extra_headers,
                        ignore_https_errors=True,
                    )
                    page = await context.new_page()
                    domain = urlparse(url).netloc
                    domain_config = self.domain_configs.get(domain, self.default_domain_config)
                    timeout_ms = domain_config.get("timeout", self.timeout) * 1000
                    js_wait_time = domain_config.get("js_wait_time", self.js_wait_time)
                    consent_timeout = domain_config.get("consent_timeout", 300)
                    try:
                        response = await page.goto(url, wait_until='networkidle', timeout=timeout_ms)
                    except Exception:
                        try:
                            response = await page.goto(url, wait_until='domcontentloaded', timeout=timeout_ms)
                        except Exception:
                            response = await page.goto(url, timeout=timeout_ms // 2)
                    if response and not response.ok:
                        logger.warning(f"[{url}] JS response status: {response.status}")
                    if response:
                        final_url = page.url
                        if final_url != url:
                            async with self.url_lock:
                                self.redirected_urls[url] = final_url
                                if final_url in self.processed_urls:
                                    self.add_to_processed_urls(url)
                                    await page.close()
                                    await context.close()
                                    return None
                        try:
                            try:
                                await page.click('button:has-text("Accept"), a:has-text("OK"), div:has-text(" Agree"), button:has-text("Consent"), button:has-text("Zustimmen")', timeout=consent_timeout)
                            except Exception:
                                pass
                            await asyncio.sleep(js_wait_time / 1000)
                            if self.js_scroll:
                                await self._scroll_page(page)
                            content = await page.content()
                            if len(content) > 5_000_000:
                                await page.close()
                                await context.close()
                                return None
                            soup = BeautifulSoup(content, 'html.parser')
                            soup.resolved_url = page.url
                            if 'login' in page.url.lower() or 'auth' in page.url.lower():
                                async with self.url_lock:
                                    self.error_urls[url] = f"Redirected to login page: {page.url}"
                                await page.close()
                                await context.close()
                                return None
                            if len(soup.get_text(strip=True)) >= 100 and len(soup.find_all(['p', 'div', 'section'])) >= 5:
                                await page.close()
                                await context.close()
                                return content
                            await page.close()
                            await context.close()
                        except Exception:
                            await page.close()
                            await context.close()
                    await asyncio.sleep(2)
            except Exception as e:
                if attempt == 2:
                    async with self.url_lock:
                        self.error_urls[url] = f"JS rendering failed: {str(e)}"
                    return None
                await asyncio.sleep(2 ** attempt)

    async def _scroll_page(self, page) -> None:
        try:
            height = await page.evaluate('document.body.scrollHeight')
            for i in range(0, height, 300):
                await page.evaluate(f'window.scrollTo(0, {i})')
                await asyncio.sleep(0.1)
            await page.evaluate('window.scrollTo(0, 0)')
            await asyncio.sleep(0.1)
        except Exception:
            pass

    async def fetch_with_retry(self, fetch_function, url: str, max_retries: int = 3) -> Optional[str | BeautifulSoup]:
        domain = urlparse(url).netloc
        base_delay = self.domain_configs.get(domain, self.default_domain_config).get("retry_delay", 2)
        for attempt in range(max_retries):
            if attempt > 0:
                delay = base_delay * (2 ** (attempt - 1)) * (0.5 + random.random())
                await asyncio.sleep(delay)
            try:
                return await fetch_function(url)
            except Exception as e:
                if attempt == max_retries - 1:
                    self.error_urls[url] = str(e)
                    return None
        return None

    async def fetch_url_async(self, url: str) -> Optional[BeautifulSoup]:
        if self.should_skip_url(url):
            async with self.url_lock:
                self.error_urls[url] = "Non-HTML content (skipped by extension)"
            return None
        content = await self.fetch_simple(url)
        if content:
            soup = BeautifulSoup(content, "html.parser")
            if self.is_login_page(soup):
                self.error_urls[url] = "Login page detected"
                return None
            if self.is_not_found_page(soup):
                self.error_urls[url] = "404 or content error detected"
                return None
            text_content = soup.get_text(strip=True)
            structure_tags = soup.find_all(['p', 'div', 'section', 'article'])
            weak_text = len(text_content) < 200
            low_structure = len(structure_tags) < 5
            has_js_warning = "javascript required" in content.lower()
            scripts = soup.find_all("script", src=True)
            external_js_count = sum(1 for s in scripts if 'zmi.js' in s['src'] or s['src'].startswith("/++resource++"))
            has_noscript_warning = bool(soup.find("noscript"))
            js_suspect = external_js_count > 1 or has_noscript_warning
            if weak_text or low_structure or has_js_warning or js_suspect:
                js_content = await self.fetch_with_js(url)
                if js_content:
                    soup = BeautifulSoup(js_content, "html.parser")
                    if self.is_login_page(soup):
                        self.error_urls[url] = "Login page detected (post-JS)"
                        return None
                    if self.is_not_found_page(soup):
                        self.error_urls[url] = "404 (post-JS)"
                        return None
                    return soup
                return None
            return soup
        js_content = await self.fetch_with_js(url)
        if js_content:
            soup = BeautifulSoup(js_content, "html.parser")
            if self.is_login_page(soup):
                self.error_urls[url] = "Login page detected (pure JS)"
                return None
            if self.is_not_found_page(soup):
                self.error_urls[url] = "404 (pure JS)"
                return None
            return soup
        return None

    async def process_url(self, url: str, depth: int) -> Tuple[List[Document], List[Document], List[Document]]:
        if url in self.processed_urls and url in self.url_to_documents_map:
            cached_docs = self.url_to_documents_map[url]
            char_docs = [doc for doc in cached_docs if doc.metadata.get("chunk_type") == "character"]
            struct_docs = [doc for doc in cached_docs if doc.metadata.get("chunk_type") == "structural"]
            full_docs = [doc for doc in cached_docs if doc.metadata.get("chunk_type") == "full_text"]
            return char_docs, struct_docs, full_docs
        if url in self.redirected_urls and self.redirected_urls[url] in self.processed_urls:
            self.add_to_processed_urls(url)
            return [], [], []
        soup = await self.fetch_with_retry(self.fetch_url_async, url)
        if not soup or self.is_login_page(soup) or self.is_not_found_page(soup):
            self.error_urls[url] = "Failed to fetch or invalid page"
            self.add_to_processed_urls(url)
            return [], [], []
        cleaned_soup = soup
        title = cleaned_soup.title.text.strip() if cleaned_soup.title else "No title"
        content, text_sample = self.extract_content(cleaned_soup, use_tags=True)
        detected_language = self.detect_language(cleaned_soup, text_sample, url)
        self.track_page_character_count(url, content, title=title, language=detected_language, depth=depth)
        char_docs: List[Document] = []
        if len(content) >= self.MIN_CHUNK_CHARS:
            char_metadata = {"source": url, "title": title, "depth": depth, "language": detected_language}
            char_docs = self.create_chunks(content, char_metadata, chunk_type="character")
        struct_docs: List[Document] = self.create_structure_based_chunks(cleaned_soup, url, depth, detected_language)
        full_docs: List[Document] = []
        if len(content) >= self.MIN_CHUNK_CHARS:
            # Full-text = one whole-page document per URL (as in notebook)
            full_metadata = {"source": url, "title": title, "depth": depth, "language": detected_language, "chunk_type": "full_text"}
            content_hash = hashlib.md5(content.encode()).hexdigest()
            self.add_to_processed_hashes(content_hash)
            full_docs = [Document(page_content=content, metadata=full_metadata)]


        all_docs = char_docs + struct_docs + full_docs
        if all_docs:
            self.add_to_processed_urls(url)
            self.url_to_documents_map[url] = all_docs
        return char_docs, struct_docs, full_docs

    def save_character_counts(self, final: bool = False) -> None:
        try:
            filename = "page_character_counts_final.json" if final else f"page_character_counts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            total_pages = len(self.page_character_counts)
            total_characters = sum(page['character_count'] for page in self.page_character_counts.values())
            total_words = sum(page['word_count'] for page in self.page_character_counts.values())
            avg_chars_per_page = total_characters / total_pages if total_pages > 0 else 0
            language_stats: Dict[str, Dict[str, int]] = {}
            for page in self.page_character_counts.values():
                lang = page['language']
                if lang not in language_stats:
                    language_stats[lang] = {'pages': 0, 'characters': 0, 'words': 0}
                language_stats[lang]['pages'] += 1
                language_stats[lang]['characters'] += page['character_count']
                language_stats[lang]['words'] += page['word_count']
            data = {
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_pages': total_pages,
                    'total_characters': total_characters,
                    'total_words': total_words,
                    'average_characters_per_page': round(avg_chars_per_page, 2),
                    'language_breakdown': language_stats,
                },
                'pages': list(self.page_character_counts.values()),
            }
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving character counts: {e}")

    def _save_progress(self, documents: List[Document], processed_urls: Set[str], error_urls: Dict[str, str], final: bool = False, chunk_type: str = "character") -> None:
        try:
            prefix = {"character": "processor_sized_base", "structural": "processor_structural_base", "full_text": "processor_full_text_base"}[chunk_type]
            filename = f"{prefix}_results_final.json" if final else f"{prefix}_progress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            data = {
                'timestamp': datetime.now().isoformat(),
                'processed_urls_count': len(processed_urls),
                'documents_count': len(documents),
                'error_urls_count': len(error_urls),
                'processed_urls': list(processed_urls),
                'error_urls': error_urls,
                'document_metadata': [doc.metadata for doc in documents],
            }
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            text_filename = f"{prefix}_text_chunks_final.json" if final else f"{prefix}_text_chunks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            text_data = {
                'timestamp': datetime.now().isoformat(),
                'text_chunks': [{'content': doc.page_content, 'metadata': doc.metadata} for doc in documents],
            }
            with open(text_filename, 'w', encoding='utf-8') as f:
                json.dump(text_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")

    async def process_urls_from_mapping(self, url_map_file: str, batch_size: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, List[Document]]:
        batch_size = batch_size or self.batch_size
        try:
            with open(url_map_file, 'r', encoding='utf-8') as f:
                url_map = json.load(f)
            urls_to_process: List[Tuple[str, int]] = []
            skipped_urls: List[str] = []
            for depth in range(self.max_depth + 1):
                depth_key = str(depth)
                if isinstance(url_map, dict) and "urls_by_depth" in url_map and depth_key in url_map["urls_by_depth"]:
                    for url in url_map["urls_by_depth"][depth_key]:
                        if self.should_skip_url(url):
                            skipped_urls.append(url)
                        else:
                            urls_to_process.append((url, depth))
                else:
                    # Fallback: a flat mapping {url: meta}
                    for url in (list(url_map.keys()) if isinstance(url_map, dict) else list(url_map)):
                        if self.should_skip_url(url):
                            skipped_urls.append(url)
                        else:
                            urls_to_process.append((url, 0))
                    break
            unique_urls: Dict[str, int] = {}
            for url, depth in urls_to_process:
                if url not in unique_urls:
                    unique_urls[url] = depth
            unique_urls_to_process = list(unique_urls.items())
            if limit:
                unique_urls_to_process = unique_urls_to_process[:limit]
            if skipped_urls:
                logger.info(f"Skipped {len(skipped_urls)} URLs with non-HTML extensions")
            logger.info(f"Loaded {len(unique_urls_to_process)} unique URLs from mapping file")
            self.progress_bar = tqdm(total=len(unique_urls_to_process), desc="Processing URLs", unit="URL", dynamic_ncols=True)
            semaphore = asyncio.Semaphore(self.max_workers)
            character_chunks: List[Document] = []
            structural_chunks: List[Document] = []
            full_text_chunks: List[Document] = []
            async def process_with_semaphore(url: str, depth: int) -> Tuple[List[Document], List[Document], List[Document]]:
                async with semaphore:
                    char_docs, struct_docs, full_docs = await self.process_url(url, depth)
                    self.progress_bar.update(1)
                    return char_docs, struct_docs, full_docs
            batch_size = batch_size or min(30, self.max_workers * 2)
            for i in range(0, len(unique_urls_to_process), batch_size):
                batch = unique_urls_to_process[i:i+batch_size]
                tasks = [process_with_semaphore(url, depth) for url, depth in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for j, result in enumerate(results):
                    url, depth = batch[j]
                    if isinstance(result, BaseException):
                        self.error_urls[url] = str(result)
                        continue
                    char_docs, struct_docs, full_docs = result
                    total_chunks = len(char_docs) + len(struct_docs) + len(full_docs)
                    if total_chunks > 0:
                        self.processed_urls.add(url)
                        if url in self.redirected_urls:
                            self.processed_urls.add(self.redirected_urls[url])
                    character_chunks.extend(char_docs)
                    structural_chunks.extend(struct_docs)
                    full_text_chunks.extend(full_docs)
            self.all_documents = character_chunks
            self.structural_documents = structural_chunks
            self.full_text_documents = full_text_chunks
            self.save_character_counts(final=True)
            if self.redirected_urls:
                with open("redirected_urls.json", "w", encoding="utf-8") as f:
                    json.dump(self.redirected_urls, f, ensure_ascii=False, indent=2)
            return {
                'character_chunks': character_chunks,
                'structural_chunks': structural_chunks,
                'full_text_chunks': full_text_chunks,
            }
        finally:
            await self.close_session()
            self.session = None
            if self.progress_bar:
                self.progress_bar.close()
            if getattr(self, 'context', None):
                try:
                    await self.context.close()
                except Exception:
                    pass
                self.context = None
            if getattr(self, 'browser', None):
                try:
                    await self.browser.close()
                except Exception:
                    pass
                self.browser = None
            if hasattr(self, "playwright"):
                try:
                    await self.playwright.stop()
                except Exception:
                    pass

    def track_extraction_results(self, url: str, character_success: bool, structural_success: bool, character_count: int = 0, structural_count: int = 0, error_msg: Optional[str] = None) -> None:
        if not hasattr(self, 'extraction_log'):
            self.extraction_log = {}
        self.extraction_log[url] = {
            'character_chunks_success': character_success,
            'structural_chunks_success': structural_success,
            'character_chunks_count': character_count,
            'structural_chunks_count': structural_count,
            'timestamp': datetime.now().isoformat(),
            'error_message': error_msg,
        }

    def print_extraction_summary(self) -> None:
        if not hasattr(self, 'extraction_log'):
            print("No extraction log available")
            return
        total_urls = len(self.extraction_log)
        character_successes = sum(1 for log in self.extraction_log.values() if log['character_chunks_success'])
        structural_successes = sum(1 for log in self.extraction_log.values() if log['structural_chunks_success'])
        both_failed = sum(1 for log in self.extraction_log.values() if not log['character_chunks_success'] and not log['structural_chunks_success'])
        print(f"\n--- EXTRACTION SUMMARY ---")
        print(f"Total URLs: {total_urls}")
        print(f"Character method succeeded: {character_successes}/{total_urls}")
        print(f"Structural method succeeded: {structural_successes}/{total_urls}")
        print(f"Both methods failed: {both_failed}")
        print(f"Character-only failures: {total_urls - character_successes}")
        print(f"Structural-only failures: {total_urls - structural_successes}")


