from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from http.cookiejar import CookieJar
from html import unescape
import importlib
from pathlib import PurePosixPath
import hashlib
import json
import re
import ssl
from typing import Any
from urllib.parse import parse_qsl, quote_plus, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPSHandler, HTTPCookieProcessor, Request, build_opener, urlopen
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

from .storage import Storage


Fetcher = Callable[[str], str]
_HTTPS_CONTEXT: ssl.SSLContext | None = None

JOBS_SENT_STATE_KEY = "jobs.sent_fingerprints"
JOBS_LAST_RUN_STATE_KEY = "jobs.last_run"
JOBS_DIIA_CITY_REGISTRY_STATE_KEY = "jobs.diia_city_registry"
JOBS_COMPANY_RESERVATION_EVIDENCE_STATE_KEY = "jobs.company_reservation_evidence"
JOBS_COMPANY_VERIFICATION_EVIDENCE_STATE_KEY = "jobs.company_verification_evidence"
JOBS_COMPANY_VERIFICATION_QUEUE_STATE_KEY = "jobs.company_verification_queue"
JOBS_RESERVATION_EVIDENCE_LEDGER_STATE_KEY = "jobs.reservation_evidence_ledger"
JOBS_SEEN_CANDIDATES_STATE_KEY = "jobs.seen_candidates"
JOBS_SOURCE_WATERMARKS_STATE_KEY = "jobs.source_watermarks"

DEFAULT_MIN_SCORE = 55
DIRECT_RESERVATION_LOW_FIT_MIN_SCORE = 45
DEFAULT_LIMIT = 3
DEFAULT_MAX_AGE_HOURS = 72
DIIA_CITY_REGISTRY_TTL = timedelta(days=7)
COMPANY_RESERVATION_EVIDENCE_POSITIVE_TTL = timedelta(days=7)
COMPANY_RESERVATION_EVIDENCE_NEGATIVE_TTL = timedelta(days=2)
COMPANY_RESERVATION_EVIDENCE_CACHE_LIMIT = 500
COMPANY_RESERVATION_DISCOVERY_LIMIT = 8
COMPANY_RESERVATION_DISCOVERY_RUN_LIMIT = 12
COMPANY_RESERVATION_DISCOVERY_PROMOTION_LIMIT = 3
SEEN_CANDIDATES_LIMIT = 2000
COMPANY_VERIFICATION_QUEUE_LIMIT = 500
RESERVATION_EVIDENCE_LEDGER_LIMIT = 500
DIIA_CITY_REGISTRY_URL = "https://city-backend.diia.gov.ua/api/front/registry/resident?page={page}"
COMPANY_VERIFICATION_ALERTABLE_EVIDENCE_TYPES = {
    "employer_verified_manual",
    "official_career_claim",
    "official_criticality_decision",
}
COMPANY_VERIFICATION_HINT_EVIDENCE_TYPES = {"sector_hint"}
COMPANY_VERIFICATION_EVIDENCE_TYPES = (
    COMPANY_VERIFICATION_ALERTABLE_EVIDENCE_TYPES
    | COMPANY_VERIFICATION_HINT_EVIDENCE_TYPES
)
ALERTABLE_RESERVATION_CONFIDENCES = {
    "direct",
    "adjacent",
    *COMPANY_VERIFICATION_ALERTABLE_EVIDENCE_TYPES,
}
JOB_ALERT_MODES = {"new", "light", "heavy", "backfill"}
LOCAL_JOB_TZ = ZoneInfo("Europe/Kyiv")
LOCAL_JOB_MONTHS = {
    "січня": 1,
    "января": 1,
    "лютого": 2,
    "февраля": 2,
    "березня": 3,
    "марта": 3,
    "квітня": 4,
    "апреля": 4,
    "травня": 5,
    "мая": 5,
    "червня": 6,
    "июня": 6,
    "липня": 7,
    "июля": 7,
    "серпня": 8,
    "августа": 8,
    "вересня": 9,
    "сентября": 9,
    "жовтня": 10,
    "октября": 10,
    "листопада": 11,
    "ноября": 11,
    "грудня": 12,
    "декабря": 12,
}
USER_AGENT = (
    "AskikuJobMonitor/1.0 "
    "(portable job search monitor; contact via repository owner)"
)

DOU_QUERIES = (
    "python бронювання",
    "backend бронювання",
    "data engineer бронювання",
    "automation бронювання",
    "ai бронювання",
    "llm бронювання",
    "fastapi бронювання",
    "django бронювання",
    "api бронювання",
    "telegram бронювання",
    "n8n бронювання",
    "rpa бронювання",
    "python remote",
    "backend remote",
    "ai automation remote",
    "data automation remote",
    "python без вищої освіти",
    "ai automation without degree",
)
DOU_NEW_QUERIES = DOU_QUERIES
DOU_XHR_MODES = {"heavy", "backfill"}
DOU_XHR_PAGE_LIMIT = 2

WORK_UA_URLS = (
    "https://www.work.ua/jobs-remote-python/?advs=1&deferment=1",
    "https://www.work.ua/jobs-remote/?search=python&deferment=1",
    "https://www.work.ua/jobs-remote/?search=backend&deferment=1",
    "https://www.work.ua/jobs-remote/?search=data%20engineer&deferment=1",
    "https://www.work.ua/jobs-remote/?search=automation&deferment=1",
    "https://www.work.ua/jobs-remote/?search=ai%20automation&deferment=1",
    "https://www.work.ua/jobs-remote/?search=llm&deferment=1",
    "https://www.work.ua/jobs-remote/?search=fastapi&deferment=1",
    "https://www.work.ua/jobs-remote/?search=django&deferment=1",
    "https://www.work.ua/jobs-remote/?search=api&deferment=1",
    "https://www.work.ua/jobs-remote/?search=telegram&deferment=1",
    "https://www.work.ua/jobs-remote/?search=n8n&deferment=1",
    "https://www.work.ua/jobs-remote/?search=rpa&deferment=1",
    "https://www.work.ua/jobs-remote-python/?advs=1",
    "https://www.work.ua/jobs-remote/?search=python%20%D0%B1%D1%80%D0%BE%D0%BD%D1%8E%D0%B2%D0%B0%D0%BD%D0%BD%D1%8F",
    "https://www.work.ua/jobs-remote/?search=ai%20%D0%B1%D1%80%D0%BE%D0%BD%D1%8E%D0%B2%D0%B0%D0%BD%D0%BD%D1%8F",
    "https://www.work.ua/jobs-remote/?search=automation%20python",
    "https://www.work.ua/jobs-remote/?search=python%20%D0%B1%D0%B5%D0%B7%20%D0%B2%D0%B8%D1%89%D0%BE%D1%97%20%D0%BE%D1%81%D0%B2%D1%96%D1%82%D0%B8",
    "https://www.work.ua/jobs-remote/?search=ai%20automation%20without%20degree",
)
WORK_UA_NEW_URLS = WORK_UA_URLS

ROBOTA_UA_URLS = (
    "https://api.robota.ua/vacancy/search?keyWords=python&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=backend&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=data%20engineer&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=automation&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=ai%20automation&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=llm&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=data%20automation&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=fastapi&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=django&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=api&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=telegram&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=n8n&scheduleId=3&isReservation=true",
    "https://api.robota.ua/vacancy/search?keyWords=rpa&scheduleId=3&isReservation=true",
)
ROBOTA_UA_DETAIL_LIMIT = 20
ROBOTA_UA_PAGE_LIMIT = 3
ROBOTA_UA_PAGE_SIZE = 50

HAPPY_MONDAY_URLS = (
    "https://happymonday.ua/jobs-search/software-engineering",
    "https://happymonday.ua/jobs-search",
)
HAPPY_MONDAY_DETAIL_LIMIT = 8
HAPPY_MONDAY_DETAIL_WORKERS = 4

JOBS_UA_URLS = (
    "https://jobs.ua/vacancy/rabota-api",
    "https://jobs.ua/vacancy/rabota-ai",
    "https://jobs.ua/vacancy/rabota-backend",
)
JOBS_UA_LISTING_BYTES = 45_000
JOBS_UA_DETAIL_BYTES = 35_000
JOBS_UA_DETAIL_LIMIT = 1
HAPPY_MONDAY_LISTING_BYTES = 450_000
HAPPY_MONDAY_DETAIL_BYTES = 240_000

DJINNI_QUERIES = (
    "",
    "python",
    "backend",
    "ai",
    "llm",
    "automation",
    "data engineer",
    "fastapi",
    "django",
    "api",
    "telegram",
    "typescript",
    "node.js",
)

WAR_SIGNALS = (
    "deftech",
    "miltech",
    "defence",
    "defense",
    "defender",
    "armed forces",
    "battlefield",
    "frontline",
    "military",
    "army",
    "drone",
    "drones",
    "uav",
    "electronic warfare",
    "robotics for defense",
    "defense capability",
    "classified",
    "secret clearance",
    "security clearance",
    "clearance required",
    "government sensitive",
    "military contractor",
    "українській армії",
    "збройних сил",
    "збройні сили",
    "сили оборони",
    "державної таємниці",
    "допуск до держтаємниці",
    "допуск к гостайне",
    "обороноздат",
    "оборонний",
    "оборона",
    "оборони",
    "військов",
    "военн",
    "зсу",
    "дрон",
    "безпілот",
    "бпла",
    "реб",
    "рер",
    "розвід",
    "штурм",
    "артилер",
    "ракета",
)

ROLE_SIGNALS = {
    "AI/LLM": (
        "ai ",
        " ai",
        "штучн",
        "искусствен",
        "llm",
        "rag",
        "machine learning",
        "ml engineer",
        "nlp",
        "applied ai",
        "genai",
        "generative ai",
    ),
    "automation": (
        "automation",
        "автоматиза",
        "automate",
        "workflow",
        "zapier",
        "make.com",
        "n8n",
    ),
    "backend": (
        "backend",
        "back-end",
        "python developer",
        "python engineer",
        "software engineer",
        "product engineer",
    ),
    "data": (
        "data engineer",
        "data analyst",
        "etl",
        "parsing",
        "scraping",
        "crawler",
        "research analyst",
    ),
    "telegram": (
        "telegram",
        "bot api",
        "chatbot",
        "bot developer",
    ),
}

STACK_SIGNALS = (
    "python",
    "fastapi",
    "django",
    "flask",
    "postgres",
    "postgresql",
    "redis",
    "docker",
    "typescript",
    "node.js",
    "nodejs",
    "javascript",
    "api",
    "rest",
    "graphql",
    "openai",
    "gemini",
    "claude",
    "langchain",
    "llamaindex",
)

REMOTE_SIGNALS = (
    "remote",
    "full remote",
    "remotely",
    "віддал",
    "дистанц",
    "удален",
    "удаленно",
    "удаленная",
    "remote work",
)

HYBRID_OR_OFFICE_SIGNALS = (
    "hybrid",
    "гібрид",
    "гибрид",
    "office",
    "офіс",
    "офис",
    "on-site",
    "onsite",
)

RESERVATION_SIGNALS = (
    "з бронюванням",
    "можливість бронювання",
    "бронювання",
    "бронюван",
    "бронь",
    "бронирование",
    "бронирован",
    "reservation",
    "reserved employees",
)

NON_EMPLOYEE_RESERVATION_PATTERNS = (
    r"\bброньован\w*\s+(?:вікн\w*|окон\w*|двер\w*|скл\w*)",
    r"\bбронюван\w*\s+(?:вікн\w*|окон\w*|двер\w*|скл\w*)",
    r"\bброн[ье][\w-]*(?:вікн|окон|двер|скл|жилет|технік|автомоб)\w*",
    r"\barmou?red\s+(?:window|door|glass|vehicle|car)s?\b",
    r"\bbulletproof\s+(?:window|door|glass|vehicle|car)s?\b",
)

CRITICALITY_SIGNALS = (
    "критично важлив",
    "критически важн",
    "critical enterprise",
    "critical company",
    "critical infrastructure",
)

NO_DEGREE_REQUIRED_SIGNALS = (
    "degree not required",
    "no degree required",
    "without degree",
    "higher education is not required",
    "higher education not required",
    "без вищої освіти",
    "вища освіта не обов",
    "вища освіта не вимага",
    "без высшего образования",
    "высшее образование не обяз",
    "высшее образование не треб",
)

EQUIVALENT_EXPERIENCE_SIGNALS = (
    "equivalent experience",
    "equivalent practical experience",
    "or relevant experience",
    "еквівалентний досвід",
    "релевантний досвід",
    "эквивалентный опыт",
    "релевантный опыт",
)

DEGREE_PREFERRED_SIGNALS = (
    "degree preferred",
    "bachelor's degree preferred",
    "bachelor’s degree preferred",
    "bachelor degree preferred",
    "higher education preferred",
    "вища освіта бажана",
    "диплом бажан",
    "высшее образование желательно",
    "диплом желател",
)

DEGREE_REQUIRED_SIGNALS = (
    "degree required",
    "bachelor's degree required",
    "bachelor’s degree required",
    "bachelor degree required",
    "master's degree required",
    "master’s degree required",
    "master degree required",
    "higher education required",
    "university degree required",
    "вища освіта обов",
    "освіта: вища",
    "освіта вища",
    "диплом обов",
    "образование: высшее",
    "образование высшее",
    "высшее образование обяз",
    "высшее образование треб",
    "диплом обязател",
)

NEGATIVE_SIGNALS = (
    "frontend",
    "front-end",
    "react developer",
    "vue developer",
    "angular developer",
    "викладач",
    "teacher",
    "minecraft",
    "support",
    "customer support",
    "embedded",
    "hardware",
    "ios",
    "android",
    "qa manual",
    "manual qa",
    "on-site",
    "офіс",
)

AGENT_AUTONOMY_HIGH_SIGNALS = (
    "automation",
    "автоматиза",
    "automate",
    "script",
    "scripting",
    "workflow",
    "zapier",
    "make.com",
    "n8n",
    "rpa",
    "etl",
    "pipeline",
    "batch",
    "ci/cd",
    "devops",
    "jenkins",
    "github actions",
    "terraform",
    "ansible",
    "kubernetes",
    "unit test",
    "integration test",
    "test automation",
    "monitoring",
    "logging",
    "cron",
    "scheduled",
    "endpoint",
    "microservice",
    "webhook",
    "data pipeline",
    "data cleaning",
    "data processing",
    "parsing",
    "scraping",
    "crawler",
    "rag",
    "llm",
    "chatbot",
    "telegram",
    "bot api",
    "api integration",
    "internal tool",
    "scripts",
    "python script",
    "sql query",
    "sql запрос",
    "написание кода",
    "генерация кода",
    "генерація коду",
    "reporting",
    "dashboard",
    "data cleanup",
    "openai",
    "langchain",
    "llamaindex",
)

AGENT_AUTONOMY_MID_SIGNALS = (
    "backend",
    "back-end",
    "data analysis",
    "analyze",
    "analysis",
    "prototype",
    "prototyping",
    "modeling",
    "refactor",
    "refactoring",
    "optimization",
    "performance tuning",
    "performance",
    "automation engineer",
    "ml engineer",
    "analytics",
    "machine learning",
    "mlops",
    "data modeling",
    "schema",
    "python developer",
    "python engineer",
    "fastapi",
    "django",
    "api",
    "data engineer",
    "data analyst",
    "postgres",
    "docker",
)

AGENT_AUTONOMY_LOW_SIGNALS = (
    "project manager",
    "product manager",
    "product owner",
    "account manager",
    "sales",
    "stakeholder",
    "stakeholders",
    "stakeholder engagement",
    "client calls",
    "client",
    "customer",
    "collaborate",
    "collaboration",
    "coordinate",
    "coordination",
    "communicate",
    "communication",
    "meeting",
    "meetings",
    "presentation",
    "presentations",
    "workshop",
    "interview",
    "mentor",
    "mentoring",
    "leadership",
    "consult",
    "consulting",
    "user research",
    "proposal",
    "proposals",
    "feature requirements",
    "roadmap",
    "people management",
    "team management",
    "manual qa",
    "customer support",
    "support",
    "teacher",
    "викладач",
)

AGENT_AUTONOMY_MANAGEMENT_SIGNALS = (
    "project manager",
    "product manager",
    "product owner",
    "team lead",
    "tech lead",
    "technical lead",
    "lead engineer",
    "head of",
    "director",
    "project planning",
    "planning",
    "budget",
    "roadmap",
    "stakeholder management",
    "cross-functional",
    "scrum",
    "agile",
    "organized",
    "training",
    "управление проектом",
    "менеджер проекта",
    "менеджер продукт",
    "керівник проект",
    "керівник продукт",
)

AGENT_AUTONOMY_SECURITY_CAP_SIGNALS = (
    "security",
    "compliance",
    "regulation",
    "regulatory",
    "audit",
    "encryption",
    "iso",
    "finance",
    "financial",
    "banking",
    "medical",
    "healthcare",
    "nuclear",
    "безпека",
    "відповідність",
    "аудит",
    "конфіденційність",
    "безопасность",
    "соответствие",
    "защита данных",
)

AGENT_AUTONOMY_STRICT_COMPLIANCE_CAP_SIGNALS = (
    "gdpr",
    "hipaa",
    "fda",
    "finra",
    "pci dss",
    "pci/dss",
    "pci",
)

AGENT_AUTONOMY_UNCLEAR_CAP_SIGNALS = (
    "r&d",
    "research",
    "strategy",
    "innovation",
    "exploration",
    "undefined tasks",
    "multi-role",
    "emerging tech",
    "дослідження",
    "эксперимент",
    "исследование",
    "стратег",
    "інновац",
    "инновац",
)

GENERIC_COMPANY_TOKENS = {
    "company",
    "digital",
    "group",
    "labs",
    "software",
    "solutions",
    "systems",
    "technologies",
    "technology",
    "ukraine",
    "компанія",
    "сервіс",
    "софт",
    "солюшенс",
    "технології",
    "україна",
}


@dataclass(frozen=True)
class JobPosting:
    source: str
    title: str
    company: str
    url: str
    summary: str
    location: str = ""
    salary: str = ""
    remote_mode: str = ""
    posted_at: str = ""
    source_query: str = ""
    company_url: str = ""
    reservation_evidence_kind: str = ""


@dataclass(frozen=True)
class ReservationEvidence:
    kind: str
    source_url: str
    quote: str


@dataclass(frozen=True)
class CompanyReservationEvidenceCacheEntry:
    checked_at: str
    evidence: ReservationEvidence | None


@dataclass(frozen=True)
class JobModeConfig:
    source_mode: str
    company_discovery_budget: int
    apply_freshness_gate: bool


JOB_MODE_CONFIGS = {
    "light": JobModeConfig(
        source_mode="new",
        company_discovery_budget=0,
        apply_freshness_gate=True,
    ),
    "heavy": JobModeConfig(
        source_mode="new",
        company_discovery_budget=COMPANY_RESERVATION_DISCOVERY_RUN_LIMIT,
        apply_freshness_gate=True,
    ),
    # Backward-compatible alias for older cron/manual calls.
    "new": JobModeConfig(
        source_mode="new",
        company_discovery_budget=COMPANY_RESERVATION_DISCOVERY_RUN_LIMIT,
        apply_freshness_gate=True,
    ),
    "backfill": JobModeConfig(
        source_mode="backfill",
        company_discovery_budget=COMPANY_RESERVATION_DISCOVERY_RUN_LIMIT,
        apply_freshness_gate=False,
    ),
}


@dataclass(frozen=True)
class JobGateDecision:
    reservation: str
    remote: str
    education: str
    defense: str
    score: str
    primary_block: str
    alertable: bool


@dataclass(frozen=True)
class ScoredJob:
    posting: JobPosting
    score: int
    reservation_confidence: str
    education_requirement: str
    reasons: tuple[str, ...]
    disqualifiers: tuple[str, ...]
    stack: tuple[str, ...]
    diia_city_resident: bool = False
    agent_delegate_pct: int = 0
    agent_delegate_label: str = "low_autonomy"
    reservation_evidence: tuple[ReservationEvidence, ...] = ()

    @property
    def is_alertable(self) -> bool:
        return _job_gate_decision(self, min_score=DEFAULT_MIN_SCORE).alertable


def fetch_job_sources(fetcher: Fetcher | None = None, *, mode: str = "new") -> list[JobPosting]:
    mode = _normalize_jobs_mode(mode)
    source_mode = _job_mode_config(mode).source_mode
    fetch = fetcher or _fetch_url
    postings: list[JobPosting] = []
    problems: list[str] = []
    dou_queries = DOU_NEW_QUERIES if source_mode == "new" else DOU_QUERIES
    work_urls = WORK_UA_NEW_URLS if source_mode == "new" else WORK_UA_URLS

    for query in dou_queries:
        url = _dou_feed_url(query)
        try:
            postings.extend(_parse_dou_rss(fetch(url), source_query=query))
        except Exception:
            problems.append(f"DOU:{query}")
        if mode in DOU_XHR_MODES:
            try:
                postings.extend(_fetch_dou_xhr_postings(fetcher=fetch, query=query))
            except Exception:
                problems.append(f"DOU XHR:{query}")

    for url in work_urls:
        try:
            postings.extend(_parse_work_ua_html(fetch(url), source_query=url))
        except Exception:
            problems.append(f"Work.ua:{PurePosixPath(urlsplit(url).path).name or 'search'}")

    for query in DJINNI_QUERIES:
        url = _djinni_feed_url(query)
        try:
            postings.extend(_fetch_djinni_postings(fetcher=fetch, source_query=url))
        except Exception:
            problems.append(f"Djinni:{query or 'remote'}")

    for url in HAPPY_MONDAY_URLS:
        try:
            postings.extend(_fetch_happy_monday_postings(fetcher=fetch, source_query=url))
        except Exception:
            problems.append(f"Happy Monday:{PurePosixPath(urlsplit(url).path).name or 'jobs-search'}")

    for url in JOBS_UA_URLS:
        try:
            postings.extend(_fetch_jobs_ua_postings(fetcher=fetch, source_query=url))
        except Exception:
            problems.append(f"Jobs.ua:{PurePosixPath(urlsplit(url).path).name or 'vacancy'}")

    for url in ROBOTA_UA_URLS:
        try:
            postings.extend(_fetch_robota_ua_postings(fetcher=fetch, source_query=url))
        except Exception:
            problems.append("Robota.ua:search")

    deduped = _dedupe_postings(postings)
    if problems:
        # Keep source failures visible to status without breaking the run.
        deduped.append(
            JobPosting(
                source="internal",
                title="source problems",
                company="Askiku",
                url="",
                summary="; ".join(problems),
                source_query="problems",
            )
        )
    return deduped


def drain_jobs_alerts(
    *,
    storage: Storage,
    limit: int = DEFAULT_LIMIT,
    min_score: int = DEFAULT_MIN_SCORE,
    fetcher: Fetcher | None = None,
    dry_run: bool = False,
    mode: str = "new",
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    mode = _normalize_jobs_mode(mode)
    mode_config = _job_mode_config(mode)
    now = (now or datetime.now(UTC)).astimezone(UTC)
    max_age_hours = max(1, int(max_age_hours or DEFAULT_MAX_AGE_HOURS))
    postings_with_internal = fetch_job_sources(fetcher=fetcher, mode=mode)
    problems = [
        posting.summary
        for posting in postings_with_internal
        if posting.source == "internal" and posting.title == "source problems"
    ]
    postings = [
        posting
        for posting in postings_with_internal
        if not (posting.source == "internal" and posting.title == "source problems")
    ]
    fetch = fetcher or _fetch_url
    try:
        diia_city_registry = _load_diia_city_registry(storage=storage, fetcher=fetch)
    except Exception:
        diia_city_registry = set()
        problems.append("Diia.City registry unavailable")
    company_reservation_cache = _load_company_reservation_evidence_cache(storage)
    company_verification_evidence = _load_company_verification_evidence(storage)
    source_company_reservation_evidence = _source_company_reservation_evidence(
        postings,
        cache=company_reservation_cache,
    )
    source_watermarks = _load_source_watermarks(storage)
    sent = _load_sent_fingerprints(storage)
    sent_before = set(sent)
    company_discovery_budget = [mode_config.company_discovery_budget]
    scored = [
        _score_job_with_known_reservation(
            posting,
            min_score=min_score,
            fetcher=fetch,
            diia_city_registry=diia_city_registry,
            company_reservation_cache=company_reservation_cache,
            source_company_reservation_evidence=source_company_reservation_evidence,
            company_verification_evidence=company_verification_evidence,
            now=now,
        )
        for posting in postings
    ]
    scored = _apply_company_discovery_to_ranked_near_misses(
        scored=scored,
        min_score=min_score,
        fetcher=fetch,
        company_reservation_cache=company_reservation_cache,
        fetch_budget=company_discovery_budget,
        source_watermarks=source_watermarks,
        sent_before=sent_before,
        mode=mode,
        now=now,
        max_age_hours=max_age_hours,
        storage=storage,
        persist_company_verification_evidence=not dry_run,
    )
    _save_company_reservation_evidence_cache(storage, company_reservation_cache)
    _update_reservation_evidence_ledger(
        storage=storage,
        scored=scored,
        company_reservation_cache=company_reservation_cache,
        now=now,
    )
    if not dry_run:
        _update_company_verification_evidence_from_scored(
            storage=storage,
            scored=scored,
            now=now,
        )
    selected_fingerprints: set[str] = set()
    alertable = [_is_alertable_job(item, min_score=min_score) for item in scored]
    freshness_reasons = [
        _freshness_hidden_reason(
            item.posting,
            mode=mode,
            now=now,
            max_age_hours=max_age_hours,
            source_watermarks=source_watermarks,
        )
        if is_alertable
        else ""
        for item, is_alertable in zip(scored, alertable, strict=True)
    ]
    fresh = [
        item
        for item, is_alertable, freshness_reason in zip(
            scored,
            alertable,
            freshness_reasons,
            strict=True,
        )
        if is_alertable
        and not freshness_reason
        and job_fingerprint(item.posting) not in sent_before
    ]
    fresh.sort(
        key=lambda item: (
            _freshness_bucket(item.posting, now=now),
            _posting_timestamp(item.posting),
            item.score,
            item.agent_delegate_pct,
            item.posting.title,
        ),
        reverse=True,
    )
    selected = fresh[: max(0, limit)]
    selected_fingerprints = {job_fingerprint(item.posting) for item in selected}

    if selected and not dry_run:
        for item in selected:
            sent.add(job_fingerprint(item.posting))
        _save_sent_fingerprints(storage, sent)
    if not dry_run:
        if mode == "new":
            _advance_source_watermarks(storage, postings, source_watermarks)
        _update_seen_candidates(
            storage=storage,
            scored=scored,
            now=now,
            selected_fingerprints=selected_fingerprints,
            sent_before=sent_before,
            freshness_reasons=freshness_reasons,
            min_score=min_score,
        )
        _update_company_verification_queue_from_seen(storage=storage, now=now)

    company_verification_queue = _load_company_verification_queue(storage)
    company_verification_counts = _company_verification_evidence_counts(storage)
    status = {
        "ran_at": now.replace(microsecond=0).isoformat(),
        "mode": mode,
        "max_age_hours": max_age_hours,
        "scanned": len(postings),
        "qualified": sum(
            1
            for is_alertable, freshness_reason in zip(
                alertable,
                freshness_reasons,
                strict=True,
            )
            if is_alertable and not freshness_reason
        ),
        "without_reservation": sum(
            1
            for item in scored
            if item.score >= min_score
            and item.reservation_confidence == "unknown"
            and not item.disqualifiers
        ),
        "non_remote_hidden": sum(
            1
            for item in scored
            if item.score >= min_score
            and item.reservation_confidence in ALERTABLE_RESERVATION_CONFIDENCES
            and not _has_remote_signal(item.posting)
            and not item.disqualifiers
        ),
        "selected": len(selected),
        "duplicates": sum(
            1
            for item, is_alertable, freshness_reason in zip(
                scored,
                alertable,
                freshness_reasons,
                strict=True,
            )
            if is_alertable
            and not freshness_reason
            and job_fingerprint(item.posting) in sent_before
        ),
        "stale_hidden": sum(1 for reason in freshness_reasons if reason == "stale"),
        "undated_hidden": sum(1 for reason in freshness_reasons if reason == "undated"),
        "watermark_hidden": sum(1 for reason in freshness_reasons if reason == "watermark"),
        "blocked": sum(1 for item in scored if item.disqualifiers),
        "required_education_hidden": 0,
        "required_education_risk": sum(
            1
            for item in scored
            if item.education_requirement == "required" and not item.disqualifiers
        ),
        "diia_city_matches": sum(1 for item in scored if item.diia_city_resident),
        "diia_city_only_hidden": sum(
            1
            for item in scored
            if item.score >= min_score
            and item.reservation_confidence == "unknown"
            and item.diia_city_resident
            and not item.disqualifiers
        ),
        "diia_city_registry_names": len(diia_city_registry),
        "company_evidence_cache_entries": len(company_reservation_cache),
        "company_evidence_positive_entries": sum(
            1 for entry in company_reservation_cache.values() if entry.evidence is not None
        ),
        "company_discovery_fetches": (
            mode_config.company_discovery_budget - company_discovery_budget[0]
        ),
        "gate_counts": _gate_counts(
            scored=scored,
            freshness_reasons=freshness_reasons,
            sent_before=sent_before,
            min_score=min_score,
        ),
        "candidate_status_counts": _candidate_status_counts(
            scored=scored,
            freshness_reasons=freshness_reasons,
            selected_fingerprints=selected_fingerprints,
            sent_before=sent_before,
            min_score=min_score,
        ),
        "source_health": _source_health_rows(postings=postings, problems=problems),
        "source_watermarks": len(source_watermarks),
        "seen_candidates": len(_load_seen_candidates(storage)),
        "company_verification_queue_entries": len(company_verification_queue),
        "company_verification_queue_companies": _company_verification_queue_company_count(
            company_verification_queue
        ),
        "company_verification_queue_top": _company_verification_queue_top(
            company_verification_queue
        ),
        "company_verification_evidence_entries": company_verification_counts.get(
            "total",
            0,
        ),
        "company_verification_evidence_counts": company_verification_counts,
        "near_misses": _near_miss_payloads(
            scored=scored,
            freshness_reasons=freshness_reasons,
            selected_fingerprints=selected_fingerprints,
            sent_before=sent_before,
            min_score=min_score,
            limit=5,
        ),
        "problems": problems,
        "dry_run": dry_run,
    }
    storage.set_state(JOBS_LAST_RUN_STATE_KEY, json.dumps(status, ensure_ascii=False, sort_keys=True))

    message = render_jobs_digest(selected) if selected else ""
    return {
        "count": len(selected),
        "message": message,
        "items": [_scored_payload(item) for item in selected],
        "status": status,
    }


def jobs_status_panel(storage: Storage) -> str:
    raw_status = storage.get_state(JOBS_LAST_RUN_STATE_KEY)
    raw_sent = storage.get_state(JOBS_SENT_STATE_KEY)
    sent_count = len(_decode_json_list(raw_sent))
    binding = storage.get_topic_binding("jobs")
    lines = ["Вакансии / мониторинг"]
    if binding is None:
        lines.append("Topic: не привязан")
    else:
        _chat_id, _topic_id, label = binding
        lines.append(f"Topic: Monitor -> {label or 'Вакансии'}")
    lines.append(
        "Источники: DOU RSS, Work.ua search, Robota.ua search, Djinni RSS, "
        "Happy Monday HTML, Jobs.ua HTML, Diia.City context"
    )
    lines.append("Режим: new alerts <=72h; manual backfill отдельно")
    lines.append("Гейты: direct/adjacent бронь с evidence, remote, отсев defense/miltech; образование только риск")
    lines.append(f"Уже отправлено: {sent_count}")

    if not raw_status:
        _append_live_company_verification_status(lines, storage)
        lines.append("Последний прогон: еще не было")
        return "\n".join(lines)

    try:
        status = json.loads(raw_status)
    except json.JSONDecodeError:
        lines.append("Последний прогон: состояние повреждено")
        return "\n".join(lines)

    ran_at = str(status.get("ran_at") or "").strip() or "unknown"
    selected_label = "выбрано dry-run" if status.get("dry_run") else "отправлено"
    mode = str(status.get("mode") or "new")
    max_age_hours = int(status.get("max_age_hours") or DEFAULT_MAX_AGE_HOURS)
    lines.append(f"Последний прогон: {ran_at}")
    lines.append(f"Последний режим: {mode}; окно свежести: {max_age_hours}h")
    lines.append(
        "Скан: "
        f"{int(status.get('scanned') or 0)} просмотрено, "
        f"{int(status.get('qualified') or 0)} подошло, "
        f"{int(status.get('selected') or 0)} {selected_label}, "
        f"{int(status.get('blocked') or 0)} отсечено"
    )
    without_reservation = int(status.get("without_reservation") or 0)
    if without_reservation:
        lines.append(f"Без бронь-сигнала скрыто: {without_reservation}")
    required_education_hidden = int(status.get("required_education_hidden") or 0)
    if required_education_hidden:
        lines.append(f"С обязательной вышкой скрыто: {required_education_hidden}")
    required_education_risk = int(status.get("required_education_risk") or 0)
    if required_education_risk:
        lines.append(f"С обязательной вышкой отмечено как риск: {required_education_risk}")
    non_remote_hidden = int(status.get("non_remote_hidden") or 0)
    if non_remote_hidden:
        lines.append(f"Без remote скрыто: {non_remote_hidden}")
    stale_hidden = int(status.get("stale_hidden") or 0)
    if stale_hidden:
        lines.append(f"Старше окна скрыто: {stale_hidden}")
    undated_hidden = int(status.get("undated_hidden") or 0)
    if undated_hidden:
        lines.append(f"Без даты скрыто в new-mode: {undated_hidden}")
    watermark_hidden = int(status.get("watermark_hidden") or 0)
    if watermark_hidden:
        lines.append(f"Не новее source watermark скрыто: {watermark_hidden}")
    registry_names = int(status.get("diia_city_registry_names") or 0)
    if registry_names:
        lines.append(f"Diia.City registry cache: {registry_names} normalized names")
    diia_city_only_hidden = int(status.get("diia_city_only_hidden") or 0)
    if diia_city_only_hidden:
        lines.append(f"Diia.City-only скрыто: {diia_city_only_hidden}")
    company_cache_entries = int(status.get("company_evidence_cache_entries") or 0)
    if company_cache_entries:
        company_cache_positive = int(status.get("company_evidence_positive_entries") or 0)
        lines.append(
            f"Company evidence cache: {company_cache_entries} entries, "
            f"{company_cache_positive} with бронь"
        )
    company_discovery_fetches = int(status.get("company_discovery_fetches") or 0)
    if company_discovery_fetches:
        lines.append(f"Company discovery fetches: {company_discovery_fetches}")
    gate_counts = status.get("gate_counts")
    if isinstance(gate_counts, dict):
        lines.append(
            "Gate counts: "
            f"alertable={int(gate_counts.get('alertable') or 0)}, "
            f"no_reservation={int(gate_counts.get('without_reservation') or 0)}, "
            f"blocked={int(gate_counts.get('blocked') or 0)}"
        )
    candidate_status_counts = status.get("candidate_status_counts")
    if isinstance(candidate_status_counts, dict):
        parts = [
            f"{key}={int(value or 0)}"
            for key, value in sorted(candidate_status_counts.items())
            if int(value or 0) > 0
        ]
        if parts:
            lines.append("Candidate statuses: " + ", ".join(parts[:8]))
    source_health = status.get("source_health")
    if isinstance(source_health, list) and source_health:
        lines.append(f"Source health rows: {len(source_health)}")
    source_watermarks = int(status.get("source_watermarks") or 0)
    if source_watermarks:
        lines.append(f"Source watermarks: {source_watermarks}")
    seen_candidates = int(status.get("seen_candidates") or 0)
    if seen_candidates:
        lines.append(f"Seen candidates: {seen_candidates}")
    company_verification_queue_entries = int(
        status.get("company_verification_queue_entries") or 0
    )
    if company_verification_queue_entries:
        company_verification_queue_companies = int(
            status.get("company_verification_queue_companies") or 0
        )
        lines.append(
            "Company verification queue: "
            f"{company_verification_queue_entries} vacancies, "
            f"{company_verification_queue_companies} companies"
        )
        top_queue = status.get("company_verification_queue_top")
        if isinstance(top_queue, list) and top_queue:
            parts: list[str] = []
            for item in top_queue[:3]:
                if not isinstance(item, dict):
                    continue
                company = str(item.get("company") or "unknown").strip()
                score = int(item.get("score") or 0)
                hints = item.get("verification_hints")
                hint_label = ""
                if isinstance(hints, list) and hints:
                    hint_label = f" [{', '.join(str(value) for value in hints[:3])}]"
                parts.append(f"{company}: {score}/100{hint_label}")
            if parts:
                lines.append("Top verification queue: " + "; ".join(parts))
    company_verification_evidence_entries = int(
        status.get("company_verification_evidence_entries") or 0
    )
    if company_verification_evidence_entries:
        counts = status.get("company_verification_evidence_counts")
        typed_counts = counts if isinstance(counts, dict) else {}
        parts = [
            f"{key}={int(value or 0)}"
            for key, value in sorted(typed_counts.items())
            if key != "total" and int(value or 0) > 0
        ]
        suffix = f" ({', '.join(parts[:6])})" if parts else ""
        lines.append(
            f"Company verification evidence: {company_verification_evidence_entries} entries"
            f"{suffix}"
        )
    problems = status.get("problems")
    if isinstance(problems, list) and problems:
        lines.append("Проблемы источников: " + "; ".join(str(item) for item in problems[:3]))
    near_misses = status.get("near_misses")
    if isinstance(near_misses, list) and near_misses:
        lines.append("Почти кандидаты:")
        for item in near_misses[:5]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "Без названия").strip()
            company = str(item.get("company") or "").strip()
            company_label = f" — {company}" if company else ""
            score = int(item.get("score") or 0)
            reason = str(item.get("reason") or "скрыто фильтром").strip()
            lines.append(f"- {title}{company_label}: fit {score}/100; {reason}")
    return "\n".join(lines)


def _append_live_company_verification_status(lines: list[str], storage: Storage) -> None:
    queue = _load_company_verification_queue(storage)
    if queue:
        lines.append(
            "Company verification queue: "
            f"{len(queue)} vacancies, "
            f"{_company_verification_queue_company_count(queue)} companies"
        )
        top_queue = _company_verification_queue_top(queue)
        if top_queue:
            parts = [
                f"{str(item.get('company') or 'unknown').strip()}: "
                f"{int(item.get('score') or 0)}/100"
                for item in top_queue[:3]
                if isinstance(item, dict)
            ]
            if parts:
                lines.append("Top verification queue: " + "; ".join(parts))
    counts = _company_verification_evidence_counts(storage)
    entries = int(counts.get("total") or 0)
    if not entries:
        return
    parts = [
        f"{key}={int(value or 0)}"
        for key, value in sorted(counts.items())
        if key != "total" and int(value or 0) > 0
    ]
    suffix = f" ({', '.join(parts[:6])})" if parts else ""
    lines.append(f"Company verification evidence: {entries} entries{suffix}")


def _is_alertable_job(item: ScoredJob, *, min_score: int) -> bool:
    return _job_gate_decision(item, min_score=min_score).alertable


def _job_gate_decision(item: ScoredJob, *, min_score: int) -> JobGateDecision:
    reservation = (
        "pass"
        if item.reservation_confidence in ALERTABLE_RESERVATION_CONFIDENCES
        else "fail"
    )
    remote = "pass" if _has_remote_signal(item.posting) else "fail"
    education = "risk" if item.education_requirement == "required" else "pass"
    defense = "fail" if item.disqualifiers else "pass"
    score = "pass" if _passes_score_gate(item, min_score=min_score) else "fail"
    if defense == "fail":
        primary_block = "defense"
    elif (
        reservation == "fail"
        and item.score >= min_score - 14
        and remote == "pass"
    ):
        primary_block = "reservation"
    elif score == "fail":
        primary_block = "score"
    elif remote == "fail":
        primary_block = "remote"
    elif reservation == "fail":
        primary_block = "reservation"
    else:
        primary_block = ""
    return JobGateDecision(
        reservation=reservation,
        remote=remote,
        education=education,
        defense=defense,
        score=score,
        primary_block=primary_block,
        alertable=not primary_block,
    )


def _gate_counts(
    *,
    scored: list[ScoredJob],
    freshness_reasons: list[str],
    sent_before: set[str],
    min_score: int,
) -> dict[str, int]:
    counts = {
        "alertable": 0,
        "blocked": 0,
        "below_score": 0,
        "without_reservation": 0,
        "required_education": 0,
        "required_education_risk": 0,
        "non_remote": 0,
        "duplicate": 0,
        "stale": 0,
        "undated": 0,
        "watermark": 0,
    }
    for item, freshness_reason in zip(scored, freshness_reasons, strict=True):
        if freshness_reason in {"stale", "undated", "watermark"}:
            counts[freshness_reason] += 1
            continue
        if job_fingerprint(item.posting) in sent_before and _is_alertable_job(
            item,
            min_score=min_score,
        ):
            counts["duplicate"] += 1
            continue
        decision = _job_gate_decision(item, min_score=min_score)
        if decision.alertable:
            counts["alertable"] += 1
            if decision.education == "risk":
                counts["required_education_risk"] += 1
        elif decision.primary_block == "defense":
            counts["blocked"] += 1
        elif decision.primary_block == "score":
            counts["below_score"] += 1
        elif decision.primary_block == "reservation":
            counts["without_reservation"] += 1
        elif decision.primary_block == "remote":
            counts["non_remote"] += 1
    return counts


def _passes_score_gate(item: ScoredJob, *, min_score: int) -> bool:
    if item.score >= min_score:
        return True
    return (
        item.reservation_confidence == "direct"
        and item.score >= DIRECT_RESERVATION_LOW_FIT_MIN_SCORE
    )


def _source_health_rows(
    *,
    postings: list[JobPosting],
    problems: list[str],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for posting in postings:
        key = (posting.source, posting.source_query)
        row = grouped.setdefault(
            key,
            {
                "source": posting.source,
                "source_query": posting.source_query,
                "fetched": 0,
                "undated": 0,
                "direct_reservation": 0,
                "remote": 0,
                "newest_posted_at": "",
                "problem": "",
            },
        )
        row["fetched"] = int(row["fetched"]) + 1
        if not posting.posted_at:
            row["undated"] = int(row["undated"]) + 1
        if _reservation_evidence_from_posting(posting) is not None:
            row["direct_reservation"] = int(row["direct_reservation"]) + 1
        if _has_remote_signal(posting):
            row["remote"] = int(row["remote"]) + 1
        current_timestamp = _posting_timestamp(posting)
        previous = str(row.get("newest_posted_at") or "")
        previous_timestamp = _posting_timestamp(
            JobPosting(
                source=posting.source,
                title="",
                company="",
                url=posting.url,
                summary="",
                posted_at=previous,
            )
        )
        if current_timestamp > previous_timestamp:
            row["newest_posted_at"] = posting.posted_at
    for problem in problems:
        label = str(problem or "").split(":", 1)[0].strip() or "unknown"
        key = (label, "problem")
        row = grouped.setdefault(
            key,
            {
                "source": label,
                "source_query": "problem",
                "fetched": 0,
                "undated": 0,
                "direct_reservation": 0,
                "remote": 0,
                "newest_posted_at": "",
                "problem": str(problem or ""),
            },
        )
        row["problem"] = str(problem or "")
    return sorted(
        grouped.values(),
        key=lambda row: (str(row.get("source") or ""), str(row.get("source_query") or "")),
    )


def _has_remote_signal(posting: JobPosting) -> bool:
    if posting.remote_mode.strip().lower() == "remote":
        return True
    text = _score_text(posting).lower()
    if "full remote" in text or "fully remote" in text:
        return True
    return any(signal in text for signal in REMOTE_SIGNALS)


def _remote_risk_reason(posting: JobPosting) -> str:
    text = _score_text(posting).lower()
    if any(signal in text for signal in REMOTE_SIGNALS) and any(
        signal in text for signal in HYBRID_OR_OFFICE_SIGNALS
    ):
        return "remote unclear/optional office"
    return ""


def _normalize_jobs_mode(mode: str) -> str:
    normalized = str(mode or "new").strip().lower()
    return normalized if normalized in JOB_ALERT_MODES else "new"


def _job_mode_config(mode: str) -> JobModeConfig:
    return JOB_MODE_CONFIGS[_normalize_jobs_mode(mode)]


def _posting_datetime(posting: JobPosting) -> datetime | None:
    return _parse_posting_datetime(posting.posted_at)


def _parse_posting_datetime(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError):
            parsed = _parse_local_job_date(raw)
            if parsed is None:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_JOB_TZ)
    return parsed.astimezone(UTC).replace(microsecond=0)


def _parse_local_job_date(value: str) -> datetime | None:
    clean = _strip_html(value).lower()
    today = datetime.now(LOCAL_JOB_TZ).date()
    if any(token in clean for token in ("сьогодні", "сегодня")):
        return datetime(today.year, today.month, today.day, tzinfo=LOCAL_JOB_TZ)
    if any(token in clean for token in ("вчора", "вчера")):
        yesterday = today - timedelta(days=1)
        return datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=LOCAL_JOB_TZ)
    match = re.search(
        r"\b(?P<day>\d{1,2})\s+(?P<month>[а-яіїєґ]+)(?:\s+(?P<year>20\d{2}))?\b",
        clean,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    month = LOCAL_JOB_MONTHS.get(match.group("month").lower())
    if month is None:
        return None
    year = int(match.group("year") or today.year)
    parsed = datetime(year, month, int(match.group("day")), tzinfo=LOCAL_JOB_TZ)
    if match.group("year") is None and parsed.date() - today > timedelta(days=7):
        parsed = parsed.replace(year=year - 1)
    return parsed


def _posting_age_hours(posting: JobPosting, *, now: datetime) -> float | None:
    posted = _posting_datetime(posting)
    if posted is None:
        return None
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    return (now.astimezone(UTC) - posted).total_seconds() / 3600


def _is_posting_fresh(posting: JobPosting, *, now: datetime, max_age_hours: int) -> bool:
    age_hours = _posting_age_hours(posting, now=now)
    return age_hours is not None and 0 <= age_hours <= max_age_hours


def _is_after_source_watermark(
    posting: JobPosting,
    *,
    source_watermarks: dict[str, datetime],
) -> bool:
    if not posting.source_query:
        return True
    watermark = source_watermarks.get(posting.source_query)
    if watermark is None:
        return True
    posted = _posting_datetime(posting)
    if posted is None:
        return False
    return posted > watermark


def _freshness_bucket(posting: JobPosting, *, now: datetime) -> int:
    age_hours = _posting_age_hours(posting, now=now)
    if age_hours is None or age_hours < 0:
        return 0
    if age_hours <= 24:
        return 3
    if age_hours <= 48:
        return 2
    if age_hours <= DEFAULT_MAX_AGE_HOURS:
        return 1
    return 0


def _posting_timestamp(posting: JobPosting) -> float:
    posted = _posting_datetime(posting)
    return posted.timestamp() if posted is not None else 0


def score_job(
    posting: JobPosting,
    *,
    reservation_confidence: str | None = None,
    diia_city_resident: bool = False,
    reservation_evidence: tuple[ReservationEvidence, ...] | None = None,
) -> ScoredJob:
    text = _score_text(posting)
    lowered = text.lower()
    title_lowered = posting.title.lower()
    disqualifiers = tuple(signal for signal in WAR_SIGNALS if signal in lowered)
    reservation_confidence = reservation_confidence or (
        "direct" if _has_reservation_signal(lowered) else "unknown"
    )
    if reservation_evidence is None:
        reservation_evidence = ()
    if reservation_confidence == "direct" and not reservation_evidence:
        evidence = _reservation_evidence_from_posting(posting)
        if evidence is not None:
            reservation_evidence = (evidence,)
    education_requirement = _education_requirement(lowered)
    stack = _matched_stack(lowered)
    agent_delegate_pct = _agent_delegate_pct(lowered, title_lowered)
    agent_delegate_label = _agent_delegate_label(agent_delegate_pct)
    if disqualifiers:
        return ScoredJob(
            posting=posting,
            score=0,
            reservation_confidence=reservation_confidence,
            education_requirement=education_requirement,
            reasons=(),
            disqualifiers=disqualifiers[:4],
            stack=(),
            diia_city_resident=diia_city_resident,
            agent_delegate_pct=agent_delegate_pct,
            agent_delegate_label=agent_delegate_label,
            reservation_evidence=reservation_evidence,
        )

    reasons: list[str] = []
    role_score = 0
    for label, signals in ROLE_SIGNALS.items():
        if any(signal in lowered for signal in signals):
            role_score += 7
            reasons.append(label)
        if any(signal in title_lowered for signal in signals):
            role_score += 5

    score = min(35, round(agent_delegate_pct * 0.35))
    score += min(25, role_score)
    if stack:
        score += min(15, 3 * len(stack))
        reasons.append("stack: " + ", ".join(stack[:4]))

    if _has_remote_signal(posting):
        reasons.append("remote")
    remote_risk = _remote_risk_reason(posting)
    if remote_risk:
        reasons.append(f"risk: {remote_risk}")

    if reservation_confidence == "direct":
        reasons.append("mentions reservation")
    elif reservation_confidence == "adjacent":
        reasons.append("company vacancies mention reservation")
    elif reservation_confidence == "official_career_claim":
        reasons.append("company career page mentions reservation")
    elif reservation_confidence == "employer_verified_manual":
        reasons.append("manual employer reservation verification")
    elif reservation_confidence == "official_criticality_decision":
        reasons.append("official criticality decision")
    elif reservation_confidence == "diia-city":
        reasons.append("Diia.City resident")

    if education_requirement == "not_required":
        score += 10
        reasons.append("degree not required")
    elif education_requirement == "equivalent_experience":
        score += 8
        reasons.append("experience can replace degree")
    elif education_requirement == "preferred":
        score += 2
        reasons.append("degree preferred")
    elif education_requirement == "not_mentioned":
        score += 5
    elif education_requirement == "required":
        score -= 20
        reasons.append("risk: degree required")

    if "senior" in title_lowered or "lead" in title_lowered:
        score += 6
    elif any(signal in title_lowered for signal in ("middle", "junior", "developer", "engineer")):
        score += 10
    else:
        score += 8

    if "україн" in lowered or "ukraine" in lowered or ".ua" in posting.url:
        score += 5

    negative_matches = [signal for signal in NEGATIVE_SIGNALS if signal in lowered]
    if negative_matches:
        score -= min(28, 8 * len(negative_matches))
        reasons.append("risk: " + ", ".join(negative_matches[:3]))

    score = max(0, min(100, score))
    return ScoredJob(
        posting=posting,
        score=score,
        reservation_confidence=reservation_confidence,
        education_requirement=education_requirement,
        reasons=tuple(_dedupe_strings(reasons))[:6],
        disqualifiers=(),
        stack=tuple(stack),
        diia_city_resident=diia_city_resident,
        agent_delegate_pct=agent_delegate_pct,
        agent_delegate_label=agent_delegate_label,
        reservation_evidence=reservation_evidence,
    )


def _score_job_with_known_reservation(
    posting: JobPosting,
    *,
    min_score: int,
    fetcher: Fetcher,
    diia_city_registry: set[str],
    company_reservation_cache: dict[str, CompanyReservationEvidenceCacheEntry],
    source_company_reservation_evidence: dict[str, ReservationEvidence],
    company_verification_evidence: dict[str, dict[str, Any]],
    now: datetime,
) -> ScoredJob:
    diia_city_resident = _is_diia_city_resident(posting.company, diia_city_registry)
    initial = score_job(posting, diia_city_resident=diia_city_resident)
    if initial.disqualifiers or initial.reservation_confidence != "unknown":
        return initial
    # Adjacent company evidence can add at most 14 points.
    # If the role is below that window, no reservation source can make it alertable.
    if initial.score < min_score - 14:
        return initial
    if posting.source == "Djinni" and not posting.company:
        enriched = _enrich_djinni_posting(posting, fetcher=fetcher)
        if enriched.company:
            posting = enriched
            diia_city_resident = _is_diia_city_resident(posting.company, diia_city_registry)
            initial = score_job(posting, diia_city_resident=diia_city_resident)
            if initial.disqualifiers or initial.reservation_confidence != "unknown":
                return initial
            if initial.score < min_score - 14:
                return initial
    source_company_evidence = _company_identity_reservation_evidence(
        posting,
        source_company_reservation_evidence,
    )
    if source_company_evidence is not None:
        return score_job(
            posting,
            reservation_confidence="adjacent",
            diia_city_resident=diia_city_resident,
            reservation_evidence=(source_company_evidence,),
        )
    verified_company_evidence = _company_verification_evidence_for_posting(
        posting,
        company_verification_evidence,
        now=now,
    )
    if verified_company_evidence is not None:
        return score_job(
            posting,
            reservation_confidence=verified_company_evidence.kind,
            diia_city_resident=diia_city_resident,
            reservation_evidence=(verified_company_evidence,),
        )
    return initial


def _apply_company_discovery_to_ranked_near_misses(
    *,
    scored: list[ScoredJob],
    min_score: int,
    fetcher: Fetcher,
    company_reservation_cache: dict[str, CompanyReservationEvidenceCacheEntry],
    fetch_budget: list[int],
    source_watermarks: dict[str, datetime],
    sent_before: set[str],
    mode: str,
    now: datetime,
    max_age_hours: int,
    storage: Storage,
    persist_company_verification_evidence: bool,
) -> list[ScoredJob]:
    if not fetch_budget or fetch_budget[0] <= 0:
        return scored
    rows: list[tuple[tuple[float, float, int, str], int, ScoredJob]] = []
    for index, item in enumerate(scored):
        decision = _job_gate_decision(item, min_score=min_score)
        if decision.primary_block != "reservation":
            continue
        if job_fingerprint(item.posting) in sent_before:
            continue
        if _freshness_hidden_reason(
            item.posting,
            mode=mode,
            now=now,
            max_age_hours=max_age_hours,
            source_watermarks=source_watermarks,
        ):
            continue
        rows.append(
            (
                (
                    float(item.score),
                    _posting_timestamp(item.posting),
                    int(item.agent_delegate_pct),
                    item.posting.title,
                ),
                index,
                item,
            )
        )
    rows.sort(key=lambda row: row[0], reverse=True)

    updated = list(scored)
    promoted = 0
    for _key, index, item in rows:
        if fetch_budget[0] <= 0:
            break
        if promoted >= COMPANY_RESERVATION_DISCOVERY_PROMOTION_LIMIT:
            break
        evidence = _company_discovery_reservation_evidence(
            item.posting,
            fetcher=fetcher,
            cache=company_reservation_cache,
            fetch_budget=fetch_budget,
        )
        if evidence is None:
            continue
        if (
            persist_company_verification_evidence
            and evidence.kind in COMPANY_VERIFICATION_ALERTABLE_EVIDENCE_TYPES
        ):
            record_company_verification_evidence(
                storage=storage,
                company=item.posting.company,
                evidence_type=evidence.kind,
                source_url=evidence.source_url,
                quote=evidence.quote,
                source="discovery",
                now=now,
            )
        updated[index] = score_job(
            item.posting,
            reservation_confidence=(
                evidence.kind
                if evidence.kind in COMPANY_VERIFICATION_ALERTABLE_EVIDENCE_TYPES
                else "adjacent"
            ),
            diia_city_resident=item.diia_city_resident,
            reservation_evidence=(evidence,),
        )
        promoted += 1
    return updated


def _source_company_reservation_evidence(
    postings: list[JobPosting],
    *,
    cache: dict[str, CompanyReservationEvidenceCacheEntry],
) -> dict[str, ReservationEvidence]:
    result: dict[str, ReservationEvidence] = {}
    for cache_key, entry in cache.items():
        if not cache_key.startswith("company:") or entry.evidence is None:
            continue
        result[cache_key.removeprefix("company:")] = entry.evidence

    for posting in postings:
        if not posting.company:
            continue
        text = _score_text(posting).lower()
        if any(signal in text for signal in WAR_SIGNALS):
            continue
        evidence = _reservation_evidence_from_posting(posting)
        if evidence is None:
            continue
        for key in _company_identity_keys(posting.company):
            result.setdefault(key, evidence)
            cache.setdefault(
                _company_cache_key(key),
                CompanyReservationEvidenceCacheEntry(
                    checked_at=_utc_now_iso(),
                    evidence=evidence,
                ),
            )
    return result


def _company_identity_reservation_evidence(
    posting: JobPosting,
    evidence_by_company: dict[str, ReservationEvidence],
) -> ReservationEvidence | None:
    for key in _company_identity_keys(posting.company):
        evidence = evidence_by_company.get(key)
        if evidence is not None and evidence.source_url != posting.url:
            return replace(evidence, kind="adjacent")
    return None


def _company_identity_keys(company: str) -> set[str]:
    normalized = _normalize_company_name(company)
    if not normalized or normalized in GENERIC_COMPANY_TOKENS:
        return set()
    return {
        key
        for key in {normalized, _latinize_ukrainian(normalized)}
        if len(key) >= 3 and key not in GENERIC_COMPANY_TOKENS
    }


def _company_cache_key(identity_key: str) -> str:
    return f"company:{identity_key}"


def _company_vacancies_reservation_evidence(
    posting: JobPosting,
    *,
    fetcher: Fetcher,
    cache: dict[str, CompanyReservationEvidenceCacheEntry],
    fetch_budget: list[int],
) -> ReservationEvidence | None:
    company_url = _company_vacancies_url(posting)
    if not company_url:
        return None
    if company_url in cache:
        return cache[company_url].evidence
    if not fetch_budget or fetch_budget[0] <= 0:
        return None
    fetch_budget[0] -= 1
    try:
        text = fetcher(company_url)
    except Exception:
        cache[company_url] = CompanyReservationEvidenceCacheEntry(
            checked_at=_utc_now_iso(),
            evidence=None,
        )
        return None
    lowered = _strip_html(text).lower()
    if any(signal in lowered for signal in WAR_SIGNALS):
        cache[company_url] = CompanyReservationEvidenceCacheEntry(
            checked_at=_utc_now_iso(),
            evidence=None,
        )
        return None
    evidence = _reservation_evidence_from_text(
        text,
        kind="adjacent",
        source_url=company_url,
    )
    cache[company_url] = CompanyReservationEvidenceCacheEntry(
        checked_at=_utc_now_iso(),
        evidence=evidence,
    )
    return evidence


def _company_discovery_reservation_evidence(
    posting: JobPosting,
    *,
    fetcher: Fetcher,
    cache: dict[str, CompanyReservationEvidenceCacheEntry],
    fetch_budget: list[int],
) -> ReservationEvidence | None:
    company_evidence = _company_vacancies_reservation_evidence(
        posting,
        fetcher=fetcher,
        cache=cache,
        fetch_budget=fetch_budget,
    )
    if company_evidence is not None:
        return company_evidence
    career_evidence = _company_owned_career_reservation_evidence(
        posting,
        fetcher=fetcher,
        cache=cache,
        fetch_budget=fetch_budget,
    )
    if career_evidence is not None:
        return career_evidence
    criticality_evidence = _official_criticality_reservation_evidence(
        posting,
        fetcher=fetcher,
        cache=cache,
        fetch_budget=fetch_budget,
    )
    if criticality_evidence is not None:
        return criticality_evidence
    return _company_search_reservation_evidence(
        posting,
        fetcher=fetcher,
        cache=cache,
        fetch_budget=fetch_budget,
    )


def _company_vacancies_url(posting: JobPosting) -> str:
    if posting.company_url:
        return posting.company_url
    match = re.search(r"https://jobs\.dou\.ua/companies/([^/]+)/vacancies/", posting.url)
    if match is None:
        return ""
    return f"https://jobs.dou.ua/companies/{match.group(1)}/vacancies/"


def _company_owned_career_reservation_evidence(
    posting: JobPosting,
    *,
    fetcher: Fetcher,
    cache: dict[str, CompanyReservationEvidenceCacheEntry],
    fetch_budget: list[int],
) -> ReservationEvidence | None:
    company_url = _company_vacancies_url(posting)
    if not company_url:
        return None
    identities = sorted(_company_identity_keys(posting.company))
    cache_identity = identities[0] if identities else _normalize_company_name(posting.company)
    cache_key = f"official-career:{cache_identity}:{company_url}"
    if cache_key in cache:
        return cache[cache_key].evidence
    if not fetch_budget or fetch_budget[0] <= 0:
        return None
    fetch_budget[0] -= 1
    try:
        company_page = fetcher(company_url)
    except Exception:
        cache[cache_key] = CompanyReservationEvidenceCacheEntry(
            checked_at=_utc_now_iso(),
            evidence=None,
        )
        return None

    evidence: ReservationEvidence | None = None
    for url in _official_career_urls_from_html(company_page, base_url=company_url):
        if not fetch_budget or fetch_budget[0] <= 0:
            break
        fetch_budget[0] -= 1
        try:
            page = fetcher(url)
        except Exception:
            continue
        evidence = _reservation_evidence_from_text(
            page,
            kind="official_career_claim",
            source_url=url,
        )
        if evidence is not None:
            break

    cache[cache_key] = CompanyReservationEvidenceCacheEntry(
        checked_at=_utc_now_iso(),
        evidence=evidence,
    )
    return evidence


def _official_career_urls_from_html(text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"""href=(["'])(?P<href>[^"']+)\1""", text, flags=re.IGNORECASE):
        url = urljoin(base_url, unescape(match.group("href")).strip())
        if not _is_company_owned_career_url(url):
            continue
        canonical = _canonical_url(url)
        if canonical in seen:
            continue
        seen.add(canonical)
        urls.append(url)
    return urls[:4]


def _is_company_owned_career_url(url: str) -> bool:
    split = urlsplit(url)
    if split.scheme not in {"http", "https"} or not split.netloc:
        return False
    if _is_job_board_or_search_domain(split.netloc):
        return False
    path = split.path.lower()
    return any(
        signal in path
        for signal in (
            "career",
            "careers",
            "vacanc",
            "jobs",
            "join",
            "team",
            "work",
        )
    )


def _official_criticality_reservation_evidence(
    posting: JobPosting,
    *,
    fetcher: Fetcher,
    cache: dict[str, CompanyReservationEvidenceCacheEntry],
    fetch_budget: list[int],
) -> ReservationEvidence | None:
    edrpou_values = _posting_edrpou_values(posting)
    legal_names = _posting_legal_names(posting)
    if not edrpou_values and not legal_names:
        return None
    identities = sorted(_company_identity_keys(posting.company))
    cache_identity = identities[0] if identities else _normalize_company_name(posting.company)
    cache_key = (
        "official-criticality:"
        f"{cache_identity}:{','.join(edrpou_values)}:{','.join(legal_names)}"
    )
    if cache_key in cache:
        return cache[cache_key].evidence
    if not fetch_budget or fetch_budget[0] <= 0:
        return None
    fetch_budget[0] -= 1
    try:
        search_page = fetcher(
            _official_criticality_search_url(
                company=posting.company,
                edrpou_values=edrpou_values,
                legal_names=legal_names,
            )
        )
    except Exception:
        cache[cache_key] = CompanyReservationEvidenceCacheEntry(
            checked_at=_utc_now_iso(),
            evidence=None,
        )
        return None

    evidence: ReservationEvidence | None = None
    for url in _official_criticality_candidate_urls(search_page):
        if not fetch_budget or fetch_budget[0] <= 0:
            break
        fetch_budget[0] -= 1
        try:
            page = fetcher(url)
        except Exception:
            continue
        quote = _official_criticality_quote_for_match(
            page,
            edrpou_values=edrpou_values,
            legal_names=legal_names,
        )
        if quote:
            evidence = ReservationEvidence(
                kind="official_criticality_decision",
                source_url=url,
                quote=quote,
            )
            break

    cache[cache_key] = CompanyReservationEvidenceCacheEntry(
        checked_at=_utc_now_iso(),
        evidence=evidence,
    )
    return evidence


def _official_criticality_search_url(
    *,
    company: str,
    edrpou_values: tuple[str, ...],
    legal_names: tuple[str, ...],
) -> str:
    query = " ".join(
        part
        for part in (
            company,
            " ".join(legal_names),
            " ".join(edrpou_values),
            "критично важливе підприємство бронювання",
        )
        if part
    )
    return f"https://duckduckgo.com/html/?q={quote_plus(query)}"


def _official_criticality_candidate_urls(text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"""href=(["'])(?P<href>[^"']+)\1""", text, flags=re.IGNORECASE):
        url = _extract_search_result_url(unescape(match.group("href")).strip())
        if not url or not _is_official_public_authority_url(url):
            continue
        canonical = _canonical_url(url)
        if canonical in seen:
            continue
        seen.add(canonical)
        urls.append(url)
    return urls[:4]


def _extract_search_result_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    url = urljoin("https://duckduckgo.com/html/", raw_url)
    params = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))
    redirected = str(params.get("uddg") or "").strip()
    return redirected or url


def _is_official_public_authority_url(url: str) -> bool:
    split = urlsplit(url)
    if split.scheme not in {"http", "https"}:
        return False
    host = split.netloc.lower().removeprefix("www.")
    if _is_job_board_or_search_domain(host):
        return False
    return (
        host == "gov.ua"
        or host.endswith(".gov.ua")
        or host == "mil.gov.ua"
        or host.endswith(".mil.gov.ua")
    )


def _is_job_board_or_search_domain(host: str) -> bool:
    host = host.lower().removeprefix("www.")
    return (
        host.endswith("work.ua")
        or host.endswith("robota.ua")
        or host.endswith("jobs.dou.ua")
        or host.endswith("djinni.co")
        or host.endswith("happymonday.ua")
        or host.endswith("jobs.ua")
        or host.endswith("duckduckgo.com")
        or host.endswith("google.com")
        or host.endswith("bing.com")
    )


def _posting_edrpou_values(posting: JobPosting) -> tuple[str, ...]:
    text = _strip_html(_score_text(posting))
    values = []
    for match in re.finditer(
        r"(?:єдрпоу|едрпоу|edrpou)\D{0,16}(?P<value>\d{8,10})",
        text,
        flags=re.IGNORECASE,
    ):
        values.append(match.group("value"))
    return tuple(_dedupe_strings(values))


def _posting_legal_names(posting: JobPosting) -> tuple[str, ...]:
    text = _strip_html(_score_text(posting))
    values: list[str] = []
    pattern = (
        r"\b(?P<form>ТОВ|ТЗОВ|ПП|АТ|ПрАТ|ДП)\s*"
        r"[\"'«“„]?(?P<name>[^\"'»”“,.;:\n<>]{2,90})"
    )
    for match in re.finditer(pattern, text, flags=re.IGNORECASE):
        form = match.group("form").strip()
        name = match.group("name").strip(" -")
        if name:
            values.append(f"{form} {name}")
    return tuple(_dedupe_strings(values))


def _official_criticality_quote_for_match(
    text: str,
    *,
    edrpou_values: tuple[str, ...],
    legal_names: tuple[str, ...],
) -> str:
    clean = re.sub(r"\s+", " ", _strip_html(text)).strip()
    lowered = clean.lower()
    if not any(signal in lowered for signal in CRITICALITY_SIGNALS):
        return ""
    if edrpou_values and any(value in clean for value in edrpou_values):
        return _criticality_quote(clean)
    normalized_page = _normalize_legal_match_text(clean)
    if any(
        _normalize_legal_match_text(legal_name) in normalized_page
        for legal_name in legal_names
        if legal_name
    ):
        return _criticality_quote(clean)
    return ""


def _criticality_quote(text: str) -> str:
    parts = [
        part.strip(" -:;,.")
        for part in re.split(r"(?<=[.!?])\s+|\s+[•|]\s+", text)
        if part.strip(" -:;,.")
    ]
    for part in parts:
        if any(signal in part.lower() for signal in CRITICALITY_SIGNALS):
            return _truncate(part, 180)
    return _truncate(text, 180)


def _normalize_legal_match_text(value: str) -> str:
    lowered = value.lower()
    return re.sub(r"[^\w]+", " ", lowered, flags=re.UNICODE).strip()


def _company_search_reservation_evidence(
    posting: JobPosting,
    *,
    fetcher: Fetcher,
    cache: dict[str, CompanyReservationEvidenceCacheEntry],
    fetch_budget: list[int],
) -> ReservationEvidence | None:
    identity_keys = sorted(_company_identity_keys(posting.company))
    if not identity_keys:
        return None
    cache_key = f"company-search:{identity_keys[0]}"
    if cache_key in cache:
        return _adjacent_evidence_for_posting(posting, cache[cache_key].evidence)
    if not fetch_budget or fetch_budget[0] <= 0:
        return None
    fetch_budget[0] -= 1

    evidence: ReservationEvidence | None = None
    try:
        evidence = _work_ua_company_search_reservation_evidence(
            posting,
            fetcher=fetcher,
        )
    except Exception:
        evidence = None

    entry = CompanyReservationEvidenceCacheEntry(
        checked_at=_utc_now_iso(),
        evidence=evidence,
    )
    cache[cache_key] = entry
    if evidence is not None:
        for identity_key in identity_keys:
            cache[_company_cache_key(identity_key)] = entry
    return _adjacent_evidence_for_posting(posting, evidence)


def _work_ua_company_search_reservation_evidence(
    posting: JobPosting,
    *,
    fetcher: Fetcher,
) -> ReservationEvidence | None:
    if not posting.company:
        return None
    url = f"https://www.work.ua/jobs-remote/?search={quote_plus(posting.company)}&deferment=1"
    candidates = _parse_work_ua_html(fetcher(url), source_query=url)
    for candidate in candidates[:COMPANY_RESERVATION_DISCOVERY_LIMIT]:
        if candidate.url == posting.url:
            continue
        if not _same_company_identity(posting.company, candidate.company):
            continue
        if any(signal in _score_text(candidate).lower() for signal in WAR_SIGNALS):
            continue
        evidence = _reservation_evidence_from_posting(candidate)
        if evidence is not None:
            return replace(evidence, kind="adjacent")
    return None


def _same_company_identity(left: str, right: str) -> bool:
    return bool(_company_identity_keys(left) & _company_identity_keys(right))


def _adjacent_evidence_for_posting(
    posting: JobPosting,
    evidence: ReservationEvidence | None,
) -> ReservationEvidence | None:
    if evidence is None or evidence.source_url == posting.url:
        return None
    return replace(evidence, kind="adjacent")


def _freshness_hidden_reason(
    posting: JobPosting,
    *,
    mode: str,
    now: datetime,
    max_age_hours: int,
    source_watermarks: dict[str, datetime],
) -> str:
    if not _job_mode_config(mode).apply_freshness_gate:
        return ""
    age_hours = _posting_age_hours(posting, now=now)
    if age_hours is None:
        return "undated"
    if age_hours > max_age_hours:
        return "stale"
    if not _is_after_source_watermark(posting, source_watermarks=source_watermarks):
        return "watermark"
    return ""


def _load_source_watermarks(storage: Storage) -> dict[str, datetime]:
    raw = storage.get_state(JOBS_SOURCE_WATERMARKS_STATE_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, datetime] = {}
    for source_query, value in payload.items():
        key = str(source_query).strip()
        if not key:
            continue
        parsed = _parse_posting_datetime(str(value or ""))
        if parsed is not None:
            result[key] = parsed
    return result


def _save_source_watermarks(storage: Storage, watermarks: dict[str, datetime]) -> None:
    payload = {
        source_query: watermarks[source_query].replace(microsecond=0).isoformat()
        for source_query in sorted(watermarks)
    }
    storage.set_state(
        JOBS_SOURCE_WATERMARKS_STATE_KEY,
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
    )


def _advance_source_watermarks(
    storage: Storage,
    postings: list[JobPosting],
    watermarks: dict[str, datetime],
) -> None:
    for posting in postings:
        if not posting.source_query:
            continue
        posted = _posting_datetime(posting)
        if posted is None:
            continue
        current = watermarks.get(posting.source_query)
        if current is None or posted > current:
            watermarks[posting.source_query] = posted
    _save_source_watermarks(storage, watermarks)


def _load_seen_candidates(storage: Storage) -> dict[str, dict[str, Any]]:
    raw = storage.get_state(JOBS_SEEN_CANDIDATES_STATE_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for fingerprint, value in payload.items():
        key = str(fingerprint).strip()
        if not key or not isinstance(value, dict):
            continue
        result[key] = value
    return result


def _save_seen_candidates(storage: Storage, seen: dict[str, dict[str, Any]]) -> None:
    capped = sorted(
        seen.items(),
        key=lambda item: str(item[1].get("last_seen") or ""),
    )[-SEEN_CANDIDATES_LIMIT:]
    storage.set_state(
        JOBS_SEEN_CANDIDATES_STATE_KEY,
        json.dumps(dict(capped), ensure_ascii=False, sort_keys=True),
    )


def _load_company_verification_queue(storage: Storage) -> dict[str, dict[str, Any]]:
    raw = storage.get_state(JOBS_COMPANY_VERIFICATION_QUEUE_STATE_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for fingerprint, value in payload.items():
        key = str(fingerprint).strip()
        if not key or not isinstance(value, dict):
            continue
        result[key] = value
    return result


def record_company_verification_evidence(
    *,
    storage: Storage,
    company: str,
    evidence_type: str,
    source_url: str,
    quote: str,
    reviewer_note: str = "",
    source: str = "manual",
    company_legal_name: str = "",
    company_edrpou: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    company = str(company or "").strip()
    evidence_type = str(evidence_type or "").strip()
    source_url = str(source_url or "").strip()
    quote = re.sub(r"\s+", " ", str(quote or "")).strip()
    reviewer_note = re.sub(r"\s+", " ", str(reviewer_note or "")).strip()
    source = str(source or "manual").strip() or "manual"
    company_legal_name = re.sub(r"\s+", " ", str(company_legal_name or "")).strip()
    company_edrpou = re.sub(r"\D+", "", str(company_edrpou or "")).strip()
    identities = sorted(_company_identity_keys(company))
    error = _company_verification_evidence_validation_error(
        company=company,
        identities=identities,
        evidence_type=evidence_type,
        source_url=source_url,
        quote=quote,
        reviewer_note=reviewer_note,
    )
    if error:
        return {"stored": False, "error": error, "record": {}}

    ledger = _load_company_verification_evidence(storage)
    now_iso = now.replace(microsecond=0).isoformat()
    key = _company_verification_evidence_key(
        company_identity=identities[0],
        evidence_type=evidence_type,
        source_url=source_url,
        quote=quote,
    )
    previous = ledger.get(key, {})
    first_seen = str(previous.get("first_seen") or now_iso)
    record = {
        "company": company,
        "company_identities": identities,
        "company_identity": identities[0],
        "company_legal_name": company_legal_name,
        "company_edrpou": company_edrpou,
        "evidence_type": evidence_type,
        "source": source,
        "source_url": source_url,
        "quote": _truncate(quote, 240),
        "reviewer_note": _truncate(reviewer_note, 240),
        "first_seen": first_seen,
        "last_verified": now_iso,
        "expires_at": (
            now + COMPANY_RESERVATION_EVIDENCE_POSITIVE_TTL
        ).replace(microsecond=0).isoformat(),
    }
    ledger[key] = record
    _save_company_verification_evidence(storage, ledger)
    return {"stored": True, "error": "", "record": record}


def _company_verification_evidence_validation_error(
    *,
    company: str,
    identities: list[str],
    evidence_type: str,
    source_url: str,
    quote: str,
    reviewer_note: str,
) -> str:
    if evidence_type not in COMPANY_VERIFICATION_EVIDENCE_TYPES:
        return "unsupported_evidence_type"
    if not company or not identities:
        return "missing_company_identity"
    if not quote:
        return "missing_quote"
    if evidence_type != "sector_hint" and not source_url:
        return "missing_source_url"
    if evidence_type == "employer_verified_manual" and not reviewer_note:
        return "missing_reviewer_note"
    return ""


def _company_verification_evidence_key(
    *,
    company_identity: str,
    evidence_type: str,
    source_url: str,
    quote: str,
) -> str:
    source_key = source_url or hashlib.sha256(quote.encode("utf-8")).hexdigest()[:16]
    return f"{company_identity}|{evidence_type}|{source_key}"


def _load_company_verification_evidence(storage: Storage) -> dict[str, dict[str, Any]]:
    raw = storage.get_state(JOBS_COMPANY_VERIFICATION_EVIDENCE_STATE_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    rows = payload.values() if isinstance(payload, dict) else payload
    if not isinstance(rows, list) and not isinstance(payload, dict):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        evidence_type = str(row.get("evidence_type") or "").strip()
        source_url = str(row.get("source_url") or "").strip()
        quote = str(row.get("quote") or "").strip()
        identities = _company_verification_record_identities(row)
        if not identities or not evidence_type or not quote:
            continue
        key = _company_verification_evidence_key(
            company_identity=identities[0],
            evidence_type=evidence_type,
            source_url=source_url,
            quote=quote,
        )
        row["company_identities"] = identities
        result[key] = row
    return result


def _save_company_verification_evidence(
    storage: Storage,
    ledger: dict[str, dict[str, Any]],
) -> None:
    capped = sorted(
        ledger.items(),
        key=lambda item: str(item[1].get("last_verified") or ""),
    )[-RESERVATION_EVIDENCE_LEDGER_LIMIT:]
    storage.set_state(
        JOBS_COMPANY_VERIFICATION_EVIDENCE_STATE_KEY,
        json.dumps(dict(capped), ensure_ascii=False, sort_keys=True),
    )


def _company_verification_record_identities(row: dict[str, Any]) -> list[str]:
    identities: list[str] = []
    raw_identities = row.get("company_identities")
    if isinstance(raw_identities, list):
        for value in raw_identities:
            identity = str(value).strip()
            if identity:
                identities.append(identity)
    identity = str(row.get("company_identity") or "").strip()
    if identity:
        identities.append(identity)
    company = str(row.get("company") or "").strip()
    identities.extend(sorted(_company_identity_keys(company)))
    return _dedupe_strings(identities)


def _company_verification_evidence_for_posting(
    posting: JobPosting,
    ledger: dict[str, dict[str, Any]],
    *,
    now: datetime | None = None,
) -> ReservationEvidence | None:
    posting_identities = _company_identity_keys(posting.company)
    if not posting_identities:
        return None
    best: dict[str, Any] | None = None
    for row in ledger.values():
        evidence_type = str(row.get("evidence_type") or "").strip()
        if evidence_type not in COMPANY_VERIFICATION_ALERTABLE_EVIDENCE_TYPES:
            continue
        if not _company_verification_record_is_fresh(row, now=now):
            continue
        row_identities = set(_company_verification_record_identities(row))
        if not posting_identities & row_identities:
            continue
        if best is None or str(row.get("last_verified") or "") > str(
            best.get("last_verified") or ""
        ):
            best = row
    if best is None:
        return None
    source_url = str(best.get("source_url") or "").strip()
    quote = str(best.get("quote") or "").strip()
    evidence_type = str(best.get("evidence_type") or "").strip()
    if not source_url or not quote:
        return None
    return ReservationEvidence(kind=evidence_type, source_url=source_url, quote=quote)


def _company_verification_record_is_fresh(
    row: dict[str, Any],
    *,
    now: datetime | None = None,
) -> bool:
    expires_at = str(row.get("expires_at") or "").strip()
    if not expires_at:
        return True
    try:
        expires = datetime.fromisoformat(expires_at)
    except ValueError:
        return False
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    reference = (now or datetime.now(UTC)).astimezone(UTC)
    return expires > reference


def _company_verification_hints_for_company(
    company: str,
    ledger: dict[str, dict[str, Any]],
    *,
    now: datetime | None = None,
) -> list[str]:
    company_identities = _company_identity_keys(company)
    hints: list[str] = []
    for row in ledger.values():
        evidence_type = str(row.get("evidence_type") or "").strip()
        if evidence_type not in COMPANY_VERIFICATION_HINT_EVIDENCE_TYPES:
            continue
        if not _company_verification_record_is_fresh(row, now=now):
            continue
        row_identities = set(_company_verification_record_identities(row))
        if company_identities & row_identities:
            hints.append(evidence_type)
    return _dedupe_strings(hints)


def _update_company_verification_evidence_from_scored(
    *,
    storage: Storage,
    scored: list[ScoredJob],
    now: datetime,
) -> None:
    for item in scored:
        for evidence in item.reservation_evidence:
            if evidence.kind not in COMPANY_VERIFICATION_ALERTABLE_EVIDENCE_TYPES:
                continue
            record_company_verification_evidence(
                storage=storage,
                company=item.posting.company,
                evidence_type=evidence.kind,
                source_url=evidence.source_url,
                quote=evidence.quote,
                source="scored",
                now=now,
            )


def _save_company_verification_queue(
    storage: Storage,
    queue: dict[str, dict[str, Any]],
) -> None:
    capped = sorted(
        queue.items(),
        key=lambda item: str(item[1].get("last_seen") or ""),
    )[-COMPANY_VERIFICATION_QUEUE_LIMIT:]
    storage.set_state(
        JOBS_COMPANY_VERIFICATION_QUEUE_STATE_KEY,
        json.dumps(dict(capped), ensure_ascii=False, sort_keys=True),
    )


def _update_company_verification_queue_from_seen(
    *,
    storage: Storage,
    now: datetime,
) -> None:
    previous_queue = _load_company_verification_queue(storage)
    verification_evidence = _load_company_verification_evidence(storage)
    seen = _load_seen_candidates(storage)
    now_iso = now.replace(microsecond=0).isoformat()
    queue: dict[str, dict[str, Any]] = {}
    for fingerprint, row in seen.items():
        if row.get("status") != "waiting_reservation_evidence":
            continue
        company = str(row.get("company") or "").strip()
        identities = sorted(_company_identity_keys(company))
        if not company or not identities:
            continue
        previous = previous_queue.get(fingerprint)
        first_queued = (
            str(previous.get("first_queued") or "")
            if isinstance(previous, dict)
            else ""
        )
        queue[fingerprint] = {
            "fingerprint": fingerprint,
            "first_queued": first_queued or now_iso,
            "last_seen": str(row.get("last_seen") or now_iso),
            "recheck_after": str(row.get("recheck_after") or ""),
            "posted_at": str(row.get("posted_at") or ""),
            "source": str(row.get("source") or ""),
            "source_query": str(row.get("source_query") or ""),
            "title": str(row.get("title") or ""),
            "company": company,
            "company_identities": identities,
            "url": str(row.get("url") or ""),
            "score": int(row.get("score") or 0),
            "remote": bool(row.get("remote")),
            "reason": "missing_reservation_evidence",
            "status": "queued",
            "verification_hints": _company_verification_hints_for_company(
                company,
                verification_evidence,
                now=now,
            ),
        }
    _save_company_verification_queue(storage, queue)


def _company_verification_queue_company_count(
    queue: dict[str, dict[str, Any]],
) -> int:
    identities: set[str] = set()
    for row in queue.values():
        raw_identities = row.get("company_identities")
        if not isinstance(raw_identities, list):
            continue
        for value in raw_identities:
            identity = str(value).strip()
            if identity:
                identities.add(identity)
    return len(identities)


def _company_verification_queue_top(
    queue: dict[str, dict[str, Any]],
    *,
    limit: int = 3,
) -> list[dict[str, Any]]:
    rows = sorted(
        queue.values(),
        key=lambda row: (
            int(row.get("score") or 0),
            str(row.get("last_seen") or ""),
            str(row.get("title") or ""),
        ),
        reverse=True,
    )
    result: list[dict[str, Any]] = []
    for row in rows[: max(0, limit)]:
        hints = row.get("verification_hints")
        result.append(
            {
                "company": str(row.get("company") or ""),
                "title": str(row.get("title") or ""),
                "score": int(row.get("score") or 0),
                "url": str(row.get("url") or ""),
                "verification_hints": hints if isinstance(hints, list) else [],
            }
        )
    return result


def _company_verification_evidence_counts(storage: Storage) -> dict[str, int]:
    raw = storage.get_state(JOBS_COMPANY_VERIFICATION_EVIDENCE_STATE_KEY)
    counts: dict[str, int] = {"total": 0}
    if not raw:
        return counts
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return counts
    if isinstance(payload, dict):
        rows = list(payload.values())
    elif isinstance(payload, list):
        rows = payload
    else:
        return counts
    for row in rows:
        if not isinstance(row, dict):
            continue
        evidence_type = str(row.get("evidence_type") or "unknown").strip()
        if not evidence_type:
            evidence_type = "unknown"
        counts["total"] += 1
        counts[evidence_type] = counts.get(evidence_type, 0) + 1
    return dict(sorted(counts.items()))


def _update_seen_candidates(
    *,
    storage: Storage,
    scored: list[ScoredJob],
    now: datetime,
    selected_fingerprints: set[str],
    sent_before: set[str],
    freshness_reasons: list[str],
    min_score: int,
) -> None:
    seen = _load_seen_candidates(storage)
    now_iso = now.replace(microsecond=0).isoformat()
    for item, freshness_reason in zip(scored, freshness_reasons, strict=True):
        posting = item.posting
        fingerprint = job_fingerprint(posting)
        if not fingerprint:
            continue
        previous = seen.get(fingerprint)
        first_seen = str(previous.get("first_seen") or now_iso) if previous else now_iso
        gate_decision = _job_gate_decision(item, min_score=min_score)
        status = _candidate_status(
            item,
            selected_fingerprints=selected_fingerprints,
            sent_before=sent_before,
            freshness_reason=freshness_reason,
            min_score=min_score,
        )
        recheck_after = _candidate_recheck_after(status=status, now=now)
        seen[fingerprint] = {
            "first_seen": first_seen,
            "last_seen": now_iso,
            "posted_at": posting.posted_at,
            "source": posting.source,
            "source_query": posting.source_query,
            "title": posting.title,
            "company": posting.company,
            "url": posting.url,
            "score": item.score,
            "reservation_confidence": item.reservation_confidence,
            "remote": _has_remote_signal(posting),
            "primary_block": gate_decision.primary_block,
            "status": status,
            "decision": _seen_decision(
                item,
                selected_fingerprints=selected_fingerprints,
                sent_before=sent_before,
                freshness_reason=freshness_reason,
                min_score=min_score,
            ),
        }
        if recheck_after:
            seen[fingerprint]["recheck_after"] = recheck_after
    _save_seen_candidates(storage, seen)


def _candidate_status_counts(
    *,
    scored: list[ScoredJob],
    freshness_reasons: list[str],
    selected_fingerprints: set[str],
    sent_before: set[str],
    min_score: int,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item, freshness_reason in zip(scored, freshness_reasons, strict=True):
        status = _candidate_status(
            item,
            selected_fingerprints=selected_fingerprints,
            sent_before=sent_before,
            freshness_reason=freshness_reason,
            min_score=min_score,
        )
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _candidate_status(
    item: ScoredJob,
    *,
    selected_fingerprints: set[str],
    sent_before: set[str],
    freshness_reason: str,
    min_score: int,
) -> str:
    fingerprint = job_fingerprint(item.posting)
    if fingerprint in selected_fingerprints:
        return "selected"
    if fingerprint in sent_before and _is_alertable_job(item, min_score=min_score):
        return "qualified_duplicate"
    if freshness_reason:
        return f"blocked_{freshness_reason}"
    decision = _job_gate_decision(item, min_score=min_score)
    if decision.alertable:
        return "qualified"
    if decision.primary_block == "defense":
        return "blocked_defense"
    if decision.primary_block == "remote":
        return "blocked_no_remote"
    if decision.primary_block == "reservation":
        if decision.remote == "pass" and item.score >= min_score - 14:
            return "waiting_reservation_evidence"
        return "blocked_no_reservation"
    if decision.primary_block == "score":
        return "blocked_low_score"
    return "hidden"


def _candidate_recheck_after(*, status: str, now: datetime) -> str:
    if status != "waiting_reservation_evidence":
        return ""
    return (now + timedelta(days=3)).replace(microsecond=0).isoformat()


def _seen_decision(
    item: ScoredJob,
    *,
    selected_fingerprints: set[str],
    sent_before: set[str],
    freshness_reason: str,
    min_score: int,
) -> str:
    fingerprint = job_fingerprint(item.posting)
    if item.disqualifiers:
        return "blocked"
    if freshness_reason:
        return freshness_reason
    if fingerprint in selected_fingerprints:
        return "selected"
    if fingerprint in sent_before:
        return "duplicate"
    if _is_alertable_job(item, min_score=min_score):
        return "qualified"
    if item.score >= min_score and item.reservation_confidence == "unknown":
        return "without_reservation"
    if not _has_remote_signal(item.posting):
        return "non_remote"
    return "hidden"


def _near_miss_payloads(
    *,
    scored: list[ScoredJob],
    freshness_reasons: list[str],
    selected_fingerprints: set[str],
    sent_before: set[str],
    min_score: int,
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[tuple[tuple[float, float, int, str], dict[str, Any]]] = []
    for item, freshness_reason in zip(scored, freshness_reasons, strict=True):
        if item.score < min_score:
            continue
        fingerprint = job_fingerprint(item.posting)
        if fingerprint in selected_fingerprints:
            continue
        reason = _near_miss_reason(
            item,
            sent_before=sent_before,
            freshness_reason=freshness_reason,
            min_score=min_score,
        )
        if not reason:
            continue
        posting = item.posting
        timestamp = _posting_timestamp(posting)
        payload = {
            "title": posting.title,
            "company": posting.company,
            "source": posting.source,
            "url": posting.url,
            "score": item.score,
            "reason": reason,
        }
        rows.append(
            (
                (
                    float(item.score),
                    timestamp,
                    int(item.agent_delegate_pct),
                    posting.title,
                ),
                payload,
            )
        )
    rows.sort(key=lambda row: row[0], reverse=True)
    return [payload for _key, payload in rows[: max(0, limit)]]


def _near_miss_reason(
    item: ScoredJob,
    *,
    sent_before: set[str],
    freshness_reason: str,
    min_score: int,
) -> str:
    fingerprint = job_fingerprint(item.posting)
    if item.disqualifiers:
        return "blocked: " + ", ".join(item.disqualifiers[:2])
    if freshness_reason == "stale":
        return "старше окна свежести"
    if freshness_reason == "undated":
        return "нет даты публикации"
    if freshness_reason == "watermark":
        return "не новее source watermark"
    if fingerprint in sent_before and _is_alertable_job(item, min_score=min_score):
        return "уже отправлялось"
    if item.reservation_confidence == "unknown":
        return "нет direct/adjacent брони"
    if not _has_remote_signal(item.posting):
        return "нет remote"
    if not _is_alertable_job(item, min_score=min_score):
        return "скрыто фильтром"
    return ""


def _load_company_reservation_evidence_cache(
    storage: Storage,
) -> dict[str, CompanyReservationEvidenceCacheEntry]:
    raw = storage.get_state(JOBS_COMPANY_RESERVATION_EVIDENCE_STATE_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    result: dict[str, CompanyReservationEvidenceCacheEntry] = {}
    for url, value in payload.items():
        if not isinstance(value, dict):
            continue
        source_url = str(url).strip()
        if not source_url:
            continue
        checked_at = str(value.get("checked_at") or "").strip()
        raw_evidence = value.get("evidence")
        evidence: ReservationEvidence | None = None
        if raw_evidence is not None:
            evidence = _reservation_evidence_from_payload(raw_evidence)
            if evidence is None:
                continue
        if not _is_company_reservation_cache_entry_fresh(
            checked_at,
            evidence=evidence,
        ):
            continue
        result[source_url] = CompanyReservationEvidenceCacheEntry(
            checked_at=checked_at,
            evidence=evidence,
        )
    return result


def _save_company_reservation_evidence_cache(
    storage: Storage,
    cache: dict[str, CompanyReservationEvidenceCacheEntry],
) -> None:
    if not cache:
        storage.set_state(JOBS_COMPANY_RESERVATION_EVIDENCE_STATE_KEY, "{}")
        return
    capped = sorted(
        cache.items(),
        key=lambda item: item[1].checked_at,
    )[-COMPANY_RESERVATION_EVIDENCE_CACHE_LIMIT:]
    payload = {
        url: {
            "checked_at": entry.checked_at,
            "evidence": (
                _reservation_evidence_payload(entry.evidence)
                if entry.evidence is not None
                else None
            ),
        }
        for url, entry in capped
    }
    storage.set_state(
        JOBS_COMPANY_RESERVATION_EVIDENCE_STATE_KEY,
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
    )


def _update_reservation_evidence_ledger(
    *,
    storage: Storage,
    scored: list[ScoredJob],
    company_reservation_cache: dict[str, CompanyReservationEvidenceCacheEntry],
    now: datetime,
) -> None:
    ledger = _load_reservation_evidence_ledger(storage)
    now_iso = now.replace(microsecond=0).isoformat()
    for item in scored:
        identities = sorted(_company_identity_keys(item.posting.company))
        if not identities:
            continue
        for evidence in item.reservation_evidence:
            for identity in identities:
                key = _reservation_ledger_key(
                    company_identity=identity,
                    evidence_type=evidence.kind,
                    source_url=evidence.source_url,
                )
                previous = ledger.get(key, {})
                first_seen = str(previous.get("first_seen") or now_iso)
                ledger[key] = {
                    "company": item.posting.company,
                    "company_identity": identity,
                    "source": item.posting.source,
                    "source_url": evidence.source_url,
                    "quote": evidence.quote,
                    "evidence_type": evidence.kind,
                    "confidence": _reservation_evidence_confidence(evidence.kind),
                    "first_seen": first_seen,
                    "last_verified": now_iso,
                    "expires_at": (
                        now + COMPANY_RESERVATION_EVIDENCE_POSITIVE_TTL
                    ).replace(microsecond=0).isoformat(),
                    "negative_reason": "",
                }
    for cache_key, entry in company_reservation_cache.items():
        if entry.evidence is not None:
            continue
        if cache_key.startswith("company-search:"):
            identity = cache_key.removeprefix("company-search:")
        elif cache_key.startswith("company:"):
            identity = cache_key.removeprefix("company:")
        else:
            continue
        key = _reservation_ledger_key(
            company_identity=identity,
            evidence_type="negative",
            source_url=cache_key,
        )
        previous = ledger.get(key, {})
        first_seen = str(previous.get("first_seen") or now_iso)
        ledger[key] = {
            "company": "",
            "company_identity": identity,
            "source": "company-discovery",
            "source_url": cache_key,
            "quote": "",
            "evidence_type": "negative",
            "confidence": 0.0,
            "first_seen": first_seen,
            "last_verified": now_iso,
            "expires_at": (
                now + COMPANY_RESERVATION_EVIDENCE_NEGATIVE_TTL
            ).replace(microsecond=0).isoformat(),
            "negative_reason": "no direct or adjacent reservation evidence found",
        }
    capped = sorted(
        ledger.values(),
        key=lambda entry: str(entry.get("last_verified") or ""),
    )[-RESERVATION_EVIDENCE_LEDGER_LIMIT:]
    storage.set_state(
        JOBS_RESERVATION_EVIDENCE_LEDGER_STATE_KEY,
        json.dumps(capped, ensure_ascii=False, sort_keys=True),
    )


def _load_reservation_evidence_ledger(storage: Storage) -> dict[str, dict[str, Any]]:
    raw = storage.get_state(JOBS_RESERVATION_EVIDENCE_LEDGER_STATE_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        company_identity = str(item.get("company_identity") or "").strip()
        evidence_type = str(item.get("evidence_type") or "").strip()
        source_url = str(item.get("source_url") or "").strip()
        if not company_identity or not evidence_type or not source_url:
            continue
        result[
            _reservation_ledger_key(
                company_identity=company_identity,
                evidence_type=evidence_type,
                source_url=source_url,
            )
        ] = item
    return result


def _reservation_ledger_key(
    *,
    company_identity: str,
    evidence_type: str,
    source_url: str,
) -> str:
    return f"{company_identity}|{evidence_type}|{source_url}"


def _reservation_evidence_confidence(kind: str) -> float:
    if kind == "direct":
        return 0.95
    if kind == "adjacent":
        return 0.82
    return 0.5


def _is_company_reservation_cache_entry_fresh(
    checked_at: str,
    *,
    evidence: ReservationEvidence | None,
) -> bool:
    try:
        checked = datetime.fromisoformat(checked_at)
    except ValueError:
        return False
    if checked.tzinfo is None:
        checked = checked.replace(tzinfo=UTC)
    ttl = (
        COMPANY_RESERVATION_EVIDENCE_POSITIVE_TTL
        if evidence is not None
        else COMPANY_RESERVATION_EVIDENCE_NEGATIVE_TTL
    )
    return datetime.now(UTC) - checked <= ttl


def _load_diia_city_registry(*, storage: Storage, fetcher: Fetcher) -> set[str]:
    cached = _load_cached_diia_city_registry(storage)
    if cached is not None:
        return cached

    names: set[str] = set()
    first_page = _fetch_diia_city_registry_page(fetcher=fetcher, page=1)
    names.update(_diia_city_names_from_payload(first_page))
    meta = first_page.get("meta") if isinstance(first_page, dict) else {}
    last_page = int(meta.get("last_page") or 1) if isinstance(meta, dict) else 1
    last_page = max(1, min(last_page, 300))
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(_fetch_diia_city_registry_page, fetcher=fetcher, page=page)
            for page in range(2, last_page + 1)
        ]
        for future in as_completed(futures):
            try:
                names.update(_diia_city_names_from_payload(future.result()))
            except Exception:
                continue

    storage.set_state(
        JOBS_DIIA_CITY_REGISTRY_STATE_KEY,
        json.dumps(
            {
                "fetched_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
                "names": sorted(names),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
    )
    return names


def _load_cached_diia_city_registry(storage: Storage) -> set[str] | None:
    raw = storage.get_state(JOBS_DIIA_CITY_REGISTRY_STATE_KEY)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    fetched_at = str(payload.get("fetched_at") or "")
    try:
        fetched = datetime.fromisoformat(fetched_at)
    except ValueError:
        return None
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=UTC)
    if datetime.now(UTC) - fetched > DIIA_CITY_REGISTRY_TTL:
        return None
    names = payload.get("names")
    if not isinstance(names, list):
        return None
    return {str(name) for name in names if str(name).strip()}


def _fetch_diia_city_registry_page(*, fetcher: Fetcher, page: int) -> dict[str, Any]:
    text = fetcher(DIIA_CITY_REGISTRY_URL.format(page=page))
    payload = json.loads(text)
    return payload if isinstance(payload, dict) else {}


def _diia_city_names_from_payload(payload: dict[str, Any]) -> set[str]:
    rows = payload.get("data")
    if not isinstance(rows, list):
        return set()
    names: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("residenceStatus") or "").strip().lower() != "active":
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        names.update(_company_match_keys(name))
    return names


def _is_diia_city_resident(company: str, registry: set[str]) -> bool:
    keys = _company_match_keys(company)
    if any(key in registry for key in keys):
        return True
    for key in keys:
        if len(key) < 6 or key in GENERIC_COMPANY_TOKENS:
            continue
        for resident_key in registry:
            if len(resident_key) < 6 or resident_key in GENERIC_COMPANY_TOKENS:
                continue
            if key in resident_key or resident_key in key:
                return True
    return False


def _company_match_keys(value: str) -> set[str]:
    normalized = _normalize_company_name(value)
    if not normalized:
        return set()
    keys = {normalized, _latinize_ukrainian(normalized)}
    tokens = [
        token
        for token in normalized.split()
        if len(token) >= 3 and token not in GENERIC_COMPANY_TOKENS
    ]
    for token in tokens:
        keys.add(token)
        keys.add(_latinize_ukrainian(token))
    if len(tokens) == 1 and len(tokens[0]) >= 5:
        keys.add(tokens[0])
        keys.add(_latinize_ukrainian(tokens[0]))
    if len(tokens) >= 2:
        keys.add(" ".join(tokens[:2]))
        keys.add(_latinize_ukrainian(" ".join(tokens[:2])))
    return {key for key in keys if len(key) >= 3}


def _normalize_company_name(value: str) -> str:
    text = _strip_html(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^\w\s-]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\b(товариство|обмеженою|відповідальністю|тов|tov|llc|ltd|inc|company|компанія|компанії)\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _latinize_ukrainian(value: str) -> str:
    mapping = {
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "h",
        "ґ": "g",
        "д": "d",
        "е": "e",
        "є": "ie",
        "ж": "zh",
        "з": "z",
        "и": "y",
        "і": "i",
        "ї": "i",
        "й": "i",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "kh",
        "ц": "ts",
        "ч": "ch",
        "ш": "sh",
        "щ": "shch",
        "ю": "iu",
        "я": "ia",
        "ь": "",
        "ъ": "",
        "ы": "y",
        "э": "e",
    }
    return "".join(mapping.get(char, char) for char in value)


def _has_reservation_signal(text: str) -> bool:
    clean = _strip_non_employee_reservation_noise(text)
    return any(signal in clean for signal in RESERVATION_SIGNALS)


def _strip_non_employee_reservation_noise(text: str) -> str:
    clean = str(text or "").lower()
    for pattern in NON_EMPLOYEE_RESERVATION_PATTERNS:
        clean = re.sub(pattern, " ", clean, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", clean)


def _reservation_evidence_from_posting(posting: JobPosting) -> ReservationEvidence | None:
    return _reservation_evidence_from_text(
        _score_text(posting),
        kind=posting.reservation_evidence_kind or "direct",
        source_url=posting.url,
    )


def _reservation_evidence_from_text(
    text: str,
    *,
    kind: str,
    source_url: str,
) -> ReservationEvidence | None:
    quote = _reservation_quote(text)
    if not quote:
        return None
    return ReservationEvidence(kind=kind, source_url=source_url, quote=quote)


def _reservation_quote(text: str) -> str:
    clean = re.sub(r"\s+", " ", _strip_html(text)).strip()
    if not clean:
        return ""
    parts = [
        part.strip(" -:;,.")
        for part in re.split(r"(?<=[.!?])\s+|\s+[•|]\s+", clean)
        if part.strip(" -:;,.")
    ]
    for part in parts:
        if _has_reservation_signal(part.lower()):
            return _truncate(part, 180)
    searchable = _strip_non_employee_reservation_noise(clean)
    lowered = searchable.lower()
    for signal in RESERVATION_SIGNALS:
        index = lowered.find(signal)
        if index < 0:
            continue
        start = max(0, index - 70)
        end = min(len(searchable), index + len(signal) + 90)
        return _truncate(searchable[start:end].strip(" -:;,."), 180)
    return ""


def _education_requirement(text: str) -> str:
    if any(signal in text for signal in NO_DEGREE_REQUIRED_SIGNALS):
        return "not_required"
    if any(signal in text for signal in EQUIVALENT_EXPERIENCE_SIGNALS):
        return "equivalent_experience"
    if any(signal in text for signal in DEGREE_REQUIRED_SIGNALS):
        return "required"
    if any(signal in text for signal in DEGREE_PREFERRED_SIGNALS):
        return "preferred"
    return "not_mentioned"


def render_jobs_digest(items: list[ScoredJob]) -> str:
    if not items:
        return ""
    lines = ["Вакансии / новые кандидаты"]
    for index, item in enumerate(items, start=1):
        posting = item.posting
        company = f" — {posting.company}" if posting.company else ""
        lines.append("")
        lines.append(f"{index}. {posting.title}{company}")
        lines.append(
            f"fit {item.score}/100; бронь: {_reservation_label(item.reservation_confidence)}; "
            f"образование: {_education_label(item.education_requirement)}; "
            f"remote: {_remote_label(posting)}"
        )
        if item.reservation_confidence == "direct" and item.score < DEFAULT_MIN_SCORE:
            lines.append("fit низкий: показано из-за прямой брони и remote")
        lines.append(f"AI-автономность: ~{item.agent_delegate_pct}% ({item.agent_delegate_label})")
        if item.reasons:
            lines.append("почему: " + "; ".join(item.reasons[:4]))
        if item.reservation_evidence:
            evidence = item.reservation_evidence[0]
            lines.append(
                "доказательство брони: "
                f"{_reservation_evidence_label(evidence.kind)} — "
                f"{_truncate(evidence.quote, 150)}"
            )
            if evidence.source_url and evidence.source_url != posting.url:
                lines.append(f"источник брони: {evidence.source_url}")
        if posting.salary:
            lines.append(f"зарплата: {posting.salary}")
        if posting.summary:
            lines.append("суть: " + _truncate(_strip_html(posting.summary), 220))
        lines.append(posting.url)
    lines.append("")
    lines.append("Бронь не гарантия. С рекрутером проверять прямо.")
    return "\n".join(lines).strip()


def job_fingerprint(posting: JobPosting) -> str:
    canonical = _canonical_url(posting.url)
    basis = canonical or f"{posting.source}|{posting.company}|{posting.title}"
    return hashlib.sha256(basis.lower().encode("utf-8")).hexdigest()[:20]


def _fetch_url(url: str, *, max_bytes: int | None = None) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/html,application/rss+xml",
        },
    )
    with urlopen(request, timeout=18, context=_https_context()) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        if max_bytes is not None:
            return response.read(max(1, int(max_bytes))).decode(charset, errors="ignore")
        return response.read().decode(charset, errors="ignore")


def _https_context() -> ssl.SSLContext:
    global _HTTPS_CONTEXT
    if _HTTPS_CONTEXT is not None:
        return _HTTPS_CONTEXT
    cafile = ""
    try:
        certifi = importlib.import_module("certifi")
        where = getattr(certifi, "where", None)
        if callable(where):
            cafile = str(where()).strip()
    except Exception:
        cafile = ""
    _HTTPS_CONTEXT = (
        ssl.create_default_context(cafile=cafile)
        if cafile
        else ssl.create_default_context()
    )
    return _HTTPS_CONTEXT


def _fetch_with_budget(fetcher: Fetcher, url: str, *, max_bytes: int) -> str:
    if fetcher is _fetch_url:
        return _fetch_url(url, max_bytes=max_bytes)
    return fetcher(url)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _dou_feed_url(query: str) -> str:
    return f"https://jobs.dou.ua/vacancies/feeds/?search={quote_plus(query)}&remote"


def _dou_search_url(query: str) -> str:
    return f"https://jobs.dou.ua/vacancies/?search={quote_plus(query)}&remote"


def _dou_xhr_url(query: str, *, count: int) -> str:
    return (
        "https://jobs.dou.ua/vacancies/xhr-load/?"
        f"{urlencode({'search': query, 'count': max(0, int(count))})}"
    )


def _djinni_feed_url(query: str) -> str:
    base = "https://djinni.co/jobs/rss/?region=eu&employment=remote"
    query = query.strip()
    if not query:
        return base
    return f"{base}&keywords={quote_plus(query)}"


def _fetch_dou_xhr_postings(*, fetcher: Fetcher, query: str) -> list[JobPosting]:
    postings: list[JobPosting] = []
    count = 0
    for _ in range(DOU_XHR_PAGE_LIMIT):
        raw = _fetch_dou_xhr_payload(fetcher=fetcher, query=query, count=count)
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            break
        page_postings = _parse_dou_xhr_html(
            str(payload.get("html") or ""),
            source_query=f"xhr:{query}",
        )
        if not page_postings:
            break
        postings.extend(page_postings)
        if bool(payload.get("last")):
            break
        next_count = int(payload.get("num") or 0)
        if next_count <= count:
            next_count = count + len(page_postings)
        count = next_count
    return postings


def _fetch_dou_xhr_payload(*, fetcher: Fetcher, query: str, count: int) -> str:
    if fetcher is not _fetch_url:
        return fetcher(_dou_xhr_url(query, count=count))
    page_url = _dou_search_url(query)
    jar = CookieJar()
    opener = build_opener(
        HTTPCookieProcessor(jar),
        HTTPSHandler(context=_https_context()),
    )
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html",
    }
    page_response = opener.open(Request(page_url, headers=headers), timeout=18)
    page_charset = page_response.headers.get_content_charset() or "utf-8"
    page_text = page_response.read(160_000).decode(page_charset, errors="ignore")
    csrf = _dou_csrf_token(page_text)
    xhr_headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Referer": page_url,
        "X-Requested-With": "XMLHttpRequest",
    }
    if csrf:
        xhr_headers["X-CSRFToken"] = csrf
    body = urlencode({"search": query, "count": max(0, int(count))}).encode()
    xhr_response = opener.open(
        Request("https://jobs.dou.ua/vacancies/xhr-load/", data=body, headers=xhr_headers),
        timeout=18,
    )
    xhr_charset = xhr_response.headers.get_content_charset() or "utf-8"
    return xhr_response.read(220_000).decode(xhr_charset, errors="ignore")


def _dou_csrf_token(text: str) -> str:
    patterns = (
        r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)',
        r'csrfmiddlewaretoken["\']?\s*[:=]\s*["\']([^"\']+)',
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match is not None:
            return match.group(1).strip()
    return ""


def _parse_dou_rss(text: str, *, source_query: str) -> list[JobPosting]:
    root = ET.fromstring(text)
    postings: list[JobPosting] = []
    for item in root.findall("./channel/item"):
        title = _node_text(item, "title")
        link = _node_text(item, "link")
        description = _node_text(item, "description")
        pub_date = _parse_pub_date(_node_text(item, "pubDate"))
        company, location = _split_dou_title(title)
        postings.append(
            JobPosting(
                source="DOU",
                title=title,
                company=company,
                url=link,
                summary=description,
                location=location,
                remote_mode="remote" if "віддал" in title.lower() else "",
                posted_at=pub_date,
                source_query=source_query,
            )
        )
    return postings


def _parse_dou_xhr_html(text: str, *, source_query: str) -> list[JobPosting]:
    postings: list[JobPosting] = []
    blocks = re.findall(
        r'<li\b[^>]*class="[^"]*\bl-vacancy\b[^"]*"[^>]*>.*?</li>',
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for block in blocks:
        title_match = re.search(
            r'<a\b[^>]*class="[^"]*\bvt\b[^"]*"[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if title_match is None:
            continue
        title = _strip_html(title_match.group("title"))
        url = unescape(title_match.group("href")).strip()
        if not url.startswith("http"):
            url = urljoin("https://jobs.dou.ua", url)
        company_match = re.search(
            r'<a\b[^>]*class="[^"]*\bcompany\b[^"]*"[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<company>.*?)</a>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        company = _strip_html(company_match.group("company")) if company_match else ""
        company_url = (
            urljoin("https://jobs.dou.ua", unescape(company_match.group("href")).strip())
            if company_match
            else ""
        )
        location = _dou_xhr_text_by_class(block, "cities")
        summary = _dou_xhr_text_by_class(block, "sh-info")
        date_text = _dou_xhr_text_by_class(block, "date")
        postings.append(
            JobPosting(
                source="DOU",
                title=title,
                company=company,
                url=url,
                summary=summary,
                location=location,
                remote_mode="remote" if _has_remote_text(f"{title} {summary} {location}") else "",
                posted_at=date_text,
                source_query=source_query,
                company_url=company_url,
            )
        )
    return postings


def _dou_xhr_text_by_class(block: str, class_name: str) -> str:
    match = re.search(
        rf'<[^>]+class="[^"]*\b{re.escape(class_name)}\b[^"]*"[^>]*>(?P<value>.*?)</[^>]+>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return _strip_html(match.group("value")) if match is not None else ""


def _fetch_djinni_postings(*, fetcher: Fetcher, source_query: str) -> list[JobPosting]:
    return _parse_djinni_rss(fetcher(source_query), source_query=source_query)


def _parse_djinni_rss(text: str, *, source_query: str) -> list[JobPosting]:
    root = ET.fromstring(text)
    postings: list[JobPosting] = []
    for item in root.findall("./channel/item"):
        title = _node_text(item, "title")
        link = _node_text(item, "link")
        description = _node_text(item, "description")
        pub_date = _parse_pub_date(_node_text(item, "pubDate"))
        summary = _strip_html(description)
        if not title or not link:
            continue
        company = _djinni_company_from_summary(summary)
        postings.append(
            JobPosting(
                source="Djinni",
                title=title,
                company=company,
                url=urljoin("https://djinni.co", link),
                summary=summary,
                location="Remote" if _has_remote_text(summary) else "",
                remote_mode="remote" if _has_remote_text(f"{title} {summary}") else "",
                posted_at=pub_date,
                source_query=source_query,
            )
        )
    return postings


def _enrich_djinni_posting(posting: JobPosting, *, fetcher: Fetcher) -> JobPosting:
    if posting.company:
        return posting
    try:
        detail = fetcher(posting.url)
    except Exception:
        return posting
    company = _djinni_company_from_detail(detail)
    if not company:
        return posting
    return replace(posting, company=company)


def _djinni_company_from_detail(text: str) -> str:
    match = re.search(
        r'<a\b[^>]*href="(?:https://djinni\.co)?/jobs/company-[^"]+/"[^>]*class="[^"]*\btext-secondary\b[^"]*"[^>]*>(?P<company>.*?)</a>',
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return ""
    return _strip_html(match.group("company"))


def _djinni_company_from_summary(summary: str) -> str:
    clean = _strip_html(summary)
    if not clean:
        return ""
    first = re.split(r"\s+[·•|]\s+|\s+-\s+", clean, maxsplit=1)[0].strip()
    lowered = first.lower()
    if not first or len(first) > 80:
        return ""
    if any(signal in lowered for signal in ("remote", "full time", "part time", "about ", "we ")):
        return ""
    if any(signal in lowered for signal in ROLE_SIGNALS) or any(
        signal in lowered for signal in STACK_SIGNALS
    ):
        return ""
    return first


def _has_remote_text(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(signal in lowered for signal in REMOTE_SIGNALS)


def _fetch_happy_monday_postings(*, fetcher: Fetcher, source_query: str) -> list[JobPosting]:
    listing = _fetch_with_budget(
        fetcher,
        source_query,
        max_bytes=HAPPY_MONDAY_LISTING_BYTES,
    )
    postings = _parse_happy_monday_listing(listing, source_query=source_query)
    if not postings:
        return []
    return _enrich_happy_monday_postings(postings, fetcher=fetcher)


def _enrich_happy_monday_postings(
    postings: list[JobPosting],
    *,
    fetcher: Fetcher,
) -> list[JobPosting]:
    head = postings[:HAPPY_MONDAY_DETAIL_LIMIT]
    if len(head) <= 1:
        return [
            *(_enrich_happy_monday_posting(posting, fetcher=fetcher) for posting in head),
            *postings[HAPPY_MONDAY_DETAIL_LIMIT:],
        ]
    enriched = list(head)
    max_workers = max(1, min(HAPPY_MONDAY_DETAIL_WORKERS, len(head)))
    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {
            executor.submit(_enrich_happy_monday_posting, posting, fetcher=fetcher): index
            for index, posting in enumerate(head)
        }
        for future in as_completed(futures):
            index = futures[future]
            try:
                enriched[index] = future.result()
            except Exception:
                enriched[index] = head[index]
    finally:
        executor.shutdown(wait=True)
    enriched.extend(postings[HAPPY_MONDAY_DETAIL_LIMIT:])
    return enriched


def _parse_happy_monday_listing(text: str, *, source_query: str) -> list[JobPosting]:
    postings: list[JobPosting] = []
    for url in _matching_links(
        text,
        base_url="https://happymonday.ua",
        path_pattern=r"/jobs/\d+(?:\?[^\"'#<\s]+)?",
    ):
        block = _html_fragment_around_url(text, url, base_url="https://happymonday.ua")
        title = _anchor_text_for_url(block, url, base_url="https://happymonday.ua")
        if not title:
            title = _first_heading_text(block)
        if not title:
            continue
        company, company_url = _company_from_happy_monday_block(block)
        summary = _strip_html(block)
        posted_at = _first_datetime_attr(block)
        location = "Remote" if _has_remote_text(block) else _location_hint_from_text(block)
        postings.append(
            JobPosting(
                source="Happy Monday",
                title=title,
                company=company,
                url=url,
                summary=_truncate(summary, 900),
                location=location,
                remote_mode="remote" if _has_remote_text(f"{title} {summary} {location}") else "",
                posted_at=posted_at,
                source_query=source_query,
                company_url=company_url,
            )
        )
    return postings


def _enrich_happy_monday_posting(posting: JobPosting, *, fetcher: Fetcher) -> JobPosting:
    try:
        detail = _fetch_with_budget(
            fetcher,
            posting.url,
            max_bytes=HAPPY_MONDAY_DETAIL_BYTES,
        )
    except Exception:
        return posting
    fragment = _detail_fragment(detail)
    detail_text = _join_nonempty_texts(
        [_strip_html(fragment), _extract_detail_text_fallback(detail)],
        limit=1800,
    )
    if not detail_text:
        return posting
    title = _first_heading_text(fragment) or posting.title
    company, company_url = _company_from_happy_monday_block(fragment)
    summary = _join_nonempty_texts([posting.summary, detail_text], limit=1400)
    location = posting.location or ("Remote" if _has_remote_text(detail_text) else _location_hint_from_text(detail_text))
    return replace(
        posting,
        title=title,
        company=company or posting.company,
        summary=summary,
        location=location,
        remote_mode="remote" if _has_remote_text(f"{title} {summary} {location}") else posting.remote_mode,
        posted_at=_first_datetime_attr(fragment) or posting.posted_at,
        company_url=company_url or posting.company_url,
    )


def _company_from_happy_monday_block(block: str) -> tuple[str, str]:
    for href, label in _anchors(block):
        absolute = urljoin("https://happymonday.ua", href)
        if "/company/" not in absolute:
            continue
        company = _strip_html(label)
        if company:
            return company, absolute
    company = _class_text(block, ("company", "employer"))
    return company, ""


def _fetch_jobs_ua_postings(*, fetcher: Fetcher, source_query: str) -> list[JobPosting]:
    listing = _fetch_with_budget(fetcher, source_query, max_bytes=JOBS_UA_LISTING_BYTES)
    list_postings = _parse_jobs_ua_listing(listing, source_query=source_query)
    if list_postings:
        return _enrich_jobs_ua_candidates(list_postings, fetcher=fetcher)
    urls = _matching_links(
        listing,
        base_url="https://jobs.ua",
        path_pattern=r"/job-[^\"'#<\s]+-\d+",
    )
    if not urls and ("JobPosting" in listing or "b-vacancy-full" in listing):
        posting = _parse_jobs_ua_detail(listing, source_query=source_query, url=source_query)
        return [posting] if posting is not None else []
    postings: list[JobPosting] = []
    for url in urls[:JOBS_UA_DETAIL_LIMIT]:
        try:
            detail = _fetch_with_budget(fetcher, url, max_bytes=JOBS_UA_DETAIL_BYTES)
        except Exception:
            continue
        posting = _parse_jobs_ua_detail(detail, source_query=source_query, url=url)
        if posting is not None:
            postings.append(posting)
    return postings


def _parse_jobs_ua_listing(text: str, *, source_query: str) -> list[JobPosting]:
    postings: list[JobPosting] = []
    blocks = re.split(
        r'(?=<li\b[^>]*class=["\'][^"\']*\bb-vacancy__item\b)',
        text,
        flags=re.IGNORECASE,
    )
    for block in blocks:
        if "b-vacancy__top__title" not in block:
            continue
        block = block.split("</li>", 1)[0]
        match = re.search(
            r'<a\b[^>]*class=(["\'])[^"\']*\bb-vacancy__top__title\b[^"\']*\1[^>]*href=(["\'])(?P<href>.*?)\2[^>]*>(?P<title>.*?)</a>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match is None:
            continue
        url = urljoin("https://jobs.ua", unescape(match.group("href")).strip())
        title = _strip_html(match.group("title"))
        if not title or not url:
            continue
        summary = _strip_html(block)
        postings.append(
            JobPosting(
                source="Jobs.ua",
                title=title,
                company=_jobs_ua_listing_company(block),
                url=url,
                summary=_truncate(summary, 900),
                location=_jobs_ua_listing_location(block),
                remote_mode="remote" if _has_remote_text(summary) else "",
                posted_at=_jobs_ua_visible_posted_at(block),
                source_query=source_query,
            )
        )
    return postings


def _enrich_jobs_ua_candidates(
    postings: list[JobPosting],
    *,
    fetcher: Fetcher,
) -> list[JobPosting]:
    result: list[JobPosting] = []
    detail_fetches = 0
    for posting in postings:
        should_fetch_detail = (
            detail_fetches < JOBS_UA_DETAIL_LIMIT
            and (_has_remote_signal(posting) or _has_reservation_signal(_score_text(posting).lower()))
        )
        if not should_fetch_detail:
            result.append(posting)
            continue
        try:
            detail = _fetch_with_budget(fetcher, posting.url, max_bytes=JOBS_UA_DETAIL_BYTES)
        except Exception:
            result.append(posting)
            continue
        detail_fetches += 1
        enriched = _parse_jobs_ua_detail(detail, source_query=posting.source_query, url=posting.url)
        result.append(enriched if enriched is not None else posting)
    return result


def _jobs_ua_listing_company(block: str) -> str:
    match = re.search(
        r'<span\b[^>]*class=(["\'])[^"\']*\blink__hidden\b[^"\']*\1[^>]*title=(["\'])(?P<title>.*?)\2',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is not None:
        return _strip_html(match.group("title"))
    match = re.search(
        r'<span\b[^>]*class=(["\'])[^"\']*\blink__hidden\b[^"\']*\1[^>]*>(?P<value>.*?)</span>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return _strip_html(match.group("value")) if match is not None else ""


def _jobs_ua_listing_location(block: str) -> str:
    city_match = re.search(
        r'<a\b[^>]*href=(["\'])https://jobs\.ua/city/[^"\']+\1[^>]*>(?P<city>.*?)</a>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if city_match is not None:
        return _strip_html(city_match.group("city"))
    return _location_hint_from_text(block)


def _parse_jobs_ua_detail(
    text: str,
    *,
    source_query: str,
    url: str,
) -> JobPosting | None:
    node = _jobs_ua_jobposting_jsonld(text)
    fragment = _jobs_ua_detail_fragment(text)
    visible_text = _strip_html(fragment)
    title = _jsonld_text(node.get("title") if node else "") or _first_heading_text(fragment)
    if not title:
        return None
    company = _jobs_ua_jsonld_company(node) if node else ""
    if not company:
        company = _jobs_ua_visible_company(fragment)
    description = _jsonld_text(node.get("description") if node else "")
    summary = _join_nonempty_texts(
        [description, visible_text, _extract_detail_text_fallback(text)],
        limit=1400,
    )
    location = _jobs_ua_jsonld_location(node) if node else ""
    if not location:
        location = _location_hint_from_text(visible_text)
    remote_text = _jsonld_text(node.get("jobLocationType") if node else "")
    remote_mode = "remote" if "TELECOMMUTE" in remote_text.upper() or _has_remote_text(summary) else ""
    return JobPosting(
        source="Jobs.ua",
        title=title,
        company=company,
        url=url,
        summary=summary,
        location=location,
        remote_mode=remote_mode,
        posted_at=(
            _jsonld_text(node.get("datePosted") if node else "")
            or _first_datetime_attr(fragment)
            or _jobs_ua_visible_posted_at(fragment)
        ),
        source_query=source_query,
    )


def _jobs_ua_jobposting_jsonld(text: str) -> dict[str, Any] | None:
    for raw in re.findall(
        r'<script\b[^>]*type=(["\'])application/ld\+json\1[^>]*>(.*?)</script>',
        text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        payload = unescape(raw[1]).strip()
        if not payload:
            continue
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            continue
        node = _find_jsonld_type(parsed, "JobPosting")
        if node is not None:
            return node
    return None


def _find_jsonld_type(value: Any, type_name: str) -> dict[str, Any] | None:
    if isinstance(value, dict):
        raw_type = value.get("@type")
        types = raw_type if isinstance(raw_type, list) else [raw_type]
        if any(str(item).lower() == type_name.lower() for item in types):
            return value
        graph = value.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                found = _find_jsonld_type(item, type_name)
                if found is not None:
                    return found
        for item in value.values():
            if isinstance(item, dict | list):
                found = _find_jsonld_type(item, type_name)
                if found is not None:
                    return found
    elif isinstance(value, list):
        for item in value:
            found = _find_jsonld_type(item, type_name)
            if found is not None:
                return found
    return None


def _jobs_ua_jsonld_company(node: dict[str, Any]) -> str:
    organization = node.get("hiringOrganization")
    if isinstance(organization, dict):
        return _jsonld_text(organization.get("name"))
    if isinstance(organization, list):
        for item in organization:
            if isinstance(item, dict):
                name = _jsonld_text(item.get("name"))
                if name:
                    return name
            name = _jsonld_text(item)
            if name:
                return name
    return _jsonld_text(organization)


def _jobs_ua_jsonld_location(node: dict[str, Any]) -> str:
    locations = node.get("jobLocation")
    values = locations if isinstance(locations, list) else [locations]
    for item in values:
        if not isinstance(item, dict):
            continue
        address = item.get("address")
        if isinstance(address, dict):
            locality = _jsonld_text(address.get("addressLocality"))
            region = _jsonld_text(address.get("addressRegion"))
            location = ", ".join(part for part in (locality, region) if part)
            if location:
                return location
        name = _jsonld_text(item.get("name"))
        if name:
            return name
    return ""


def _jobs_ua_visible_company(fragment: str) -> str:
    match = re.search(
        r"Компанія:\s*</span>\s*<span[^>]*class=[\"'][^\"']*\bcontrol\b[^\"']*[\"'][^>]*>(?P<value>.*?)</span>",
        fragment,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is not None:
        value = match.group("value")
        anchors = _anchors(value)
        if anchors:
            return _strip_html(anchors[0][1])
        return _strip_html(value)
    return _class_text(fragment, ("company", "employer"))


def _jobs_ua_visible_posted_at(fragment: str) -> str:
    match = re.search(
        r'<span\b[^>]*class=(["\'])[^"\']*\bb-vacancy-full__tech__item\b[^"\']*\1[^>]*>\s*<i\b[^>]*\bfa-refresh\b[^>]*></i>\s*&nbsp;\s*(?P<date>[^<]+)',
        fragment,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is not None:
        return _strip_html(match.group("date"))
    match = re.search(
        r"(?:Опубліковано|Опубликовано|Оновлено|Обновлено):\s*</?[^>]*>\s*(?P<date>\d{1,2}\s+[а-яіїєґ]+\s+20\d{2})",
        fragment,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return _strip_html(match.group("date")) if match is not None else ""


def _jobs_ua_detail_fragment(text: str) -> str:
    marker = "b-vacancy-full"
    start = text.find(marker)
    if start < 0:
        return _detail_fragment(text)
    start = max(0, text.rfind("<div", 0, start))
    end = text.find("b-company__subscribe", start)
    if end < 0:
        end = text.find("b-read-more__list", start)
    if end < 0:
        end = min(len(text), start + 12000)
    return text[start:end]


def _matching_links(text: str, *, base_url: str, path_pattern: str) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    pattern = re.compile(
        r'href=(["\'])(?P<href>(?:https?://[^"\']+)?' + path_pattern + r')\1',
        flags=re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        url = urljoin(base_url, unescape(match.group("href")).strip())
        if url in seen:
            continue
        seen.add(url)
        result.append(url)
    return result


def _html_fragment_around_url(text: str, url: str, *, base_url: str) -> str:
    needles = [url, urlsplit(url).path]
    if urlsplit(url).query:
        needles.append(f"{urlsplit(url).path}?{urlsplit(url).query}")
    indexes = [text.find(needle) for needle in needles if needle and text.find(needle) >= 0]
    if not indexes:
        return ""
    index = min(indexes)
    start_window = max(0, index - 2500)
    start = max(
        text.rfind("<article", start_window, index),
        text.rfind("<li", start_window, index),
        text.rfind("<div", start_window, index),
    )
    if start < 0:
        start = start_window
    end_candidates = [
        pos + len(close)
        for close in ("</article>", "</li>")
        if (pos := text.find(close, index)) >= 0
    ]
    if end_candidates:
        end = min(end_candidates)
    else:
        next_link = _next_matching_link_index(
            text,
            start=index + 1,
            base_url=base_url,
            current_url=url,
        )
        end = next_link if next_link >= 0 else min(len(text), index + 3000)
    return text[start:end]


def _next_matching_link_index(
    text: str,
    *,
    start: int,
    base_url: str,
    current_url: str,
) -> int:
    for match in re.finditer(r'href=(["\'])(?P<href>[^"\']+)\1', text[start:], flags=re.IGNORECASE):
        url = urljoin(base_url, unescape(match.group("href")).strip())
        if url != current_url and ("/jobs/" in url or "jobs.ua/job-" in url):
            return start + match.start()
    return -1


def _anchors(text: str) -> list[tuple[str, str]]:
    return [
        (unescape(match.group("href")).strip(), match.group("label"))
        for match in re.finditer(
            r'<a\b[^>]*href=(["\'])(?P<href>.*?)\1[^>]*>(?P<label>.*?)</a>',
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
    ]


def _anchor_text_for_url(block: str, url: str, *, base_url: str) -> str:
    for href, label in _anchors(block):
        if urljoin(base_url, href) == url:
            return _strip_html(label)
    return ""


def _first_heading_text(text: str) -> str:
    match = re.search(r"<h1\b[^>]*>(.*?)</h1>", text, flags=re.IGNORECASE | re.DOTALL)
    if match is not None:
        return _strip_html(match.group(1))
    match = re.search(r"<h2\b[^>]*>(.*?)</h2>", text, flags=re.IGNORECASE | re.DOTALL)
    return _strip_html(match.group(1)) if match is not None else ""


def _first_datetime_attr(text: str) -> str:
    match = re.search(r'\bdatetime=(["\'])(?P<value>.*?)\1', text, flags=re.IGNORECASE | re.DOTALL)
    if match is not None:
        return _strip_html(match.group("value"))
    match = re.search(r'\bdatePosted=(["\'])(?P<value>.*?)\1', text, flags=re.IGNORECASE | re.DOTALL)
    if match is not None:
        return _strip_html(match.group("value"))
    match = re.search(
        r'<[^>]+\bitemprop=(["\'])datePosted\1[^>]*>(?P<value>.*?)</[^>]+>',
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is not None:
        return _strip_html(match.group("value"))
    return ""


def _class_text(text: str, class_tokens: tuple[str, ...]) -> str:
    for token in class_tokens:
        pattern = (
            r'<[^>]*class=(["\'])[^"\']*\b'
            + re.escape(token)
            + r'\b[^"\']*\1[^>]*>(?P<value>.*?)</[^>]+>'
        )
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match is not None:
            value = _strip_html(match.group("value"))
            if value:
                return value
    return ""


def _detail_fragment(text: str) -> str:
    for tag in ("main", "article"):
        match = re.search(
            rf"<{tag}\b[^>]*>(?P<value>.*?)</{tag}>",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match is not None:
            return match.group("value")
    return text


def _extract_detail_text_fallback(text: str) -> str:
    try:
        module = importlib.import_module("trafilatura")
    except Exception:
        return ""
    extract = getattr(module, "extract", None)
    if not callable(extract):
        return ""
    try:
        extracted = extract(
            text,
            include_comments=False,
            include_tables=False,
            favor_recall=True,
        )
    except Exception:
        return ""
    if not isinstance(extracted, str):
        return ""
    return _strip_html(extracted)


def _location_hint_from_text(text: str) -> str:
    clean = _strip_html(text)
    for location in ("Київ", "Львів", "Дніпро", "Одеса", "Харків", "Україна", "Ukraine"):
        if location.lower() in clean.lower():
            return location
    return ""


def _jsonld_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str | int | float):
        return str(value).strip()
    if isinstance(value, list):
        return " ".join(part for part in (_jsonld_text(item) for item in value) if part).strip()
    if isinstance(value, dict):
        for key in ("name", "value", "@value"):
            text = _jsonld_text(value.get(key))
            if text:
                return text
    return ""


def _join_nonempty_texts(values: list[str], *, limit: int) -> str:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _strip_html(value)
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return _truncate(" ".join(result), limit)


def _parse_work_ua_html(text: str, *, source_query: str) -> list[JobPosting]:
    postings: list[JobPosting] = []
    blocks = re.split(r'(?=<a\s+name="\d+"></a>\s*<div\b[^>]*\bjob-link\b)', text)
    for block in blocks:
        if "job-link" not in block:
            continue
        block = block.split('<a name="', 1)[0] if block.count('<a name="') > 1 else block
        title_match = re.search(
            r"<h2[^>]*>\s*<a\b[^>]*href=\"(?P<href>/jobs/\d+/[^\"]*|/jobs/\d+/)\"[^>]*>(?P<title>.*?)</a>",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if title_match is None:
            continue
        title = _strip_html(title_match.group("title"))
        href = title_match.group("href")
        url = urljoin("https://www.work.ua", href)
        strongs = [
            _strip_html(value)
            for value in re.findall(r'<span\s+class="strong-600"[^>]*>(.*?)</span>', block, flags=re.IGNORECASE | re.DOTALL)
        ]
        salary = next((value for value in strongs if "грн" in value.lower() or "$" in value), "")
        company = next(
            (
                value
                for value in strongs
                if value != salary
                and "грн" not in value.lower()
                and len(value) <= 90
            ),
            "",
        )
        company_href = ""
        company_match = re.search(
            r'href="(?P<href>/jobs/by-company/[^"]+|/employer/[^"]+)"[^>]*>\s*<span\s+class="strong-600"',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if company_match is not None:
            company_href = urljoin("https://www.work.ua", company_match.group("href"))
        summary_match = re.search(
            r'<p\b[^>]*class="[^"]*\bellipsis\b[^"]*"[^>]*>(?P<summary>.*?)</p>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        summary_parts = [_strip_html(summary_match.group("summary"))] if summary_match else []
        has_reservation_badge = _work_ua_card_has_reservation_badge(block)
        if has_reservation_badge and not _has_reservation_signal(
            " ".join(summary_parts).lower()
        ):
            summary_parts.append("Бронювання працівників.")
        summary = " ".join(part for part in summary_parts if part)
        posted_match = re.search(r'<time\b[^>]*datetime="([^"]+)"', block, flags=re.IGNORECASE)
        posted_at = posted_match.group(1).strip() if posted_match else ""
        location = "Дистанційно" if "Дистанційно" in block else ""
        postings.append(
            JobPosting(
                source="Work.ua",
                title=title,
                company=company,
                url=url,
                summary=summary,
                location=location,
                salary=salary,
                remote_mode="remote" if location else "",
                posted_at=posted_at,
                source_query=source_query,
                company_url=company_href,
                reservation_evidence_kind=(
                    "platform_badge" if has_reservation_badge else ""
                ),
            )
        )
    return postings


def _work_ua_card_has_reservation_badge(block: str) -> bool:
    return re.search(
        r"""<span\b[^>]*\bclass=(["'])[^"']*\bglyphicon-deferment\b[^"']*\1""",
        block,
        flags=re.IGNORECASE,
    ) is not None


def _fetch_robota_ua_postings(*, fetcher: Fetcher, source_query: str) -> list[JobPosting]:
    documents: list[dict[str, Any]] = []
    seen_documents: set[str] = set()
    for page in range(1, ROBOTA_UA_PAGE_LIMIT + 1):
        payload = json.loads(fetcher(_robota_ua_page_url(source_query, page=page)))
        page_documents = payload.get("documents") if isinstance(payload, dict) else None
        if not isinstance(page_documents, list) or not page_documents:
            break
        added = 0
        for item in page_documents:
            if not isinstance(item, dict):
                continue
            key = _robota_document_key(item)
            if not key or key in seen_documents:
                continue
            seen_documents.add(key)
            documents.append(item)
            added += 1
        if added == 0:
            break
    postings: list[JobPosting] = []
    detail_fetches = 0
    for item in documents:
        detail = None
        if detail_fetches < ROBOTA_UA_DETAIL_LIMIT:
            detail_fetches += 1
            detail = _fetch_robota_ua_detail(fetcher=fetcher, item=item)
        source = detail or item
        if not _robota_is_remote(source):
            continue
        posting = _robota_posting_from_item(source, source_query=source_query)
        if posting is not None:
            postings.append(posting)
    return postings


def _robota_ua_page_url(source_query: str, *, page: int) -> str:
    split = urlsplit(source_query)
    params = dict(parse_qsl(split.query, keep_blank_values=True))
    params.setdefault("count", str(ROBOTA_UA_PAGE_SIZE))
    params["page"] = str(max(1, int(page)))
    return urlunsplit((split.scheme, split.netloc, split.path, urlencode(params), split.fragment))


def _robota_document_key(item: dict[str, Any]) -> str:
    vacancy_id = str(item.get("id") or "").strip()
    if vacancy_id:
        return vacancy_id
    title = str(item.get("name") or "").strip().lower()
    company = str(item.get("companyName") or "").strip().lower()
    return f"{company}|{title}" if title or company else ""


def _fetch_robota_ua_detail(*, fetcher: Fetcher, item: dict[str, Any]) -> dict[str, Any] | None:
    vacancy_id = int(item.get("id") or 0)
    if not vacancy_id:
        return None
    try:
        payload = json.loads(fetcher(f"https://api.robota.ua/vacancy?id={vacancy_id}"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _robota_posting_from_item(item: dict[str, Any], *, source_query: str) -> JobPosting | None:
    vacancy_id = int(item.get("id") or 0)
    company_id = int(item.get("notebookId") or 0)
    title = str(item.get("name") or "").strip()
    company = str(item.get("companyName") or "").strip()
    if not vacancy_id or not company_id or not title:
        return None
    badge_names = _robota_badge_names(item)
    has_reservation_badge = any(
        _has_reservation_signal(name.lower()) for name in badge_names
    )
    summary_parts = [
        str(item.get("description") or "").strip(),
        str(item.get("shortDescription") or "").strip(),
        " ".join(badge_names),
    ]
    city = str(item.get("cityName") or "").strip()
    url = f"https://robota.ua/company{company_id}/vacancy{vacancy_id}"
    return JobPosting(
        source="Robota.ua",
        title=title,
        company=company,
        url=url,
        summary=_strip_html(" ".join(part for part in summary_parts if part)),
        location=city,
        salary=_robota_salary_label(item),
        remote_mode="remote",
        posted_at=str(item.get("date") or "").strip(),
        source_query=source_query,
        company_url=f"https://api.robota.ua/companies/{company_id}/published-vacancies",
        reservation_evidence_kind="platform_badge" if has_reservation_badge else "",
    )


def _robota_is_remote(item: dict[str, Any]) -> bool:
    if int(item.get("scheduleId") or 0) == 3:
        return True
    text = " ".join(
        str(item.get(key) or "")
        for key in ("name", "cityName", "description", "shortDescription")
    ).lower()
    return any(signal in text for signal in REMOTE_SIGNALS)


def _robota_badge_names(item: dict[str, Any]) -> list[str]:
    badges = item.get("badges")
    if not isinstance(badges, list):
        return []
    names: list[str] = []
    for badge in badges:
        if not isinstance(badge, dict):
            continue
        name = str(badge.get("name") or "").strip()
        if name:
            names.append(name)
    return _dedupe_strings(names)


def _robota_salary_label(item: dict[str, Any]) -> str:
    comment = str(item.get("salaryComment") or "").strip()
    salary = int(item.get("salary") or 0)
    salary_from = int(item.get("salaryFrom") or 0)
    salary_to = int(item.get("salaryTo") or 0)
    if salary:
        return f"{salary} грн"
    if salary_from and salary_to:
        return f"{salary_from}-{salary_to} грн"
    if salary_from:
        return f"от {salary_from} грн"
    if salary_to:
        return f"до {salary_to} грн"
    return comment


def _dedupe_postings(postings: list[JobPosting]) -> list[JobPosting]:
    result: list[JobPosting] = []
    seen: set[str] = set()
    for posting in postings:
        if not posting.title.strip() or (posting.source != "internal" and not posting.url.strip()):
            continue
        key = _canonical_url(posting.url) or f"{posting.source}|{posting.company}|{posting.title}"
        normalized = re.sub(r"\s+", " ", key.lower()).strip()
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(posting)
    return result


def _matched_stack(lowered: str) -> list[str]:
    stack: list[str] = []
    for signal in STACK_SIGNALS:
        if signal in lowered:
            stack.append(signal)
    return _dedupe_strings(stack)


def _agent_delegate_pct(lowered: str, title_lowered: str) -> int:
    score = 30
    high_matches = [signal for signal in AGENT_AUTONOMY_HIGH_SIGNALS if signal in lowered]
    mid_matches = [signal for signal in AGENT_AUTONOMY_MID_SIGNALS if signal in lowered]
    low_matches = [signal for signal in AGENT_AUTONOMY_LOW_SIGNALS if signal in lowered]
    management_matches = [
        signal for signal in AGENT_AUTONOMY_MANAGEMENT_SIGNALS if signal in lowered
    ]
    security_matches = [
        signal for signal in AGENT_AUTONOMY_SECURITY_CAP_SIGNALS if signal in lowered
    ]
    strict_compliance_matches = [
        signal for signal in AGENT_AUTONOMY_STRICT_COMPLIANCE_CAP_SIGNALS if signal in lowered
    ]

    score += min(50, 10 * len(high_matches))
    score += min(25, 5 * len(mid_matches))
    score -= min(60, 15 * len(low_matches))
    score -= min(40, 20 * len(management_matches))
    score -= min(60, 30 * len(security_matches))

    if any(signal in title_lowered for signal in ("automation", "ai", "llm", "data", "backend")):
        score += 8
    if any(signal in title_lowered for signal in ("manager", "lead", "head", "director")):
        score -= 12
    if any(signal in lowered for signal in ("remote", "віддал", "дистанц", "удален")):
        score += 5

    if _has_management_title(title_lowered):
        score = min(score, 20)
    if strict_compliance_matches:
        score = min(score, 10)
    elif security_matches:
        score = min(score, 20)
    if any(signal in lowered for signal in AGENT_AUTONOMY_UNCLEAR_CAP_SIGNALS):
        score = min(score, 50)

    return max(0, min(100, score))


def _has_management_title(title_lowered: str) -> bool:
    return any(
        signal in title_lowered
        for signal in (
            "project manager",
            "product manager",
            "product owner",
            "team lead",
            "tech lead",
            "technical lead",
            "lead engineer",
            "head of",
            "director",
            "менеджер проект",
            "менеджер продукт",
            "керівник проект",
            "керівник продукт",
        )
    )


def _agent_delegate_label(pct: int) -> str:
    if pct <= 30:
        return "low_autonomy"
    if pct <= 60:
        return "AI_assisted"
    if pct <= 80:
        return "high_delegation"
    return "agent_first"


def _score_text(posting: JobPosting) -> str:
    return " ".join(
        part
        for part in (
            posting.title,
            posting.company,
            posting.summary,
            posting.location,
            posting.remote_mode,
        )
        if part
    )


def _remote_label(posting: JobPosting) -> str:
    if _has_remote_signal(posting):
        return "да"
    return "проверить"


def _reservation_label(value: str) -> str:
    return {
        "direct": "упомянута",
        "adjacent": "есть в соседних вакансиях компании",
        "official_career_claim": "на career page компании",
        "employer_verified_manual": "подтверждена вручную",
        "official_criticality_decision": "official criticality evidence",
        "diia-city": "Diia.City resident",
        "unknown": "неясно",
    }.get(value, value or "неясно")


def _reservation_evidence_label(value: str) -> str:
    return {
        "direct": "вакансия",
        "platform_badge": "бейдж платформы",
        "adjacent": "соседние вакансии компании",
        "official_career_claim": "career page компании",
        "employer_verified_manual": "ручная HR-проверка",
        "official_criticality_decision": "official criticality page",
    }.get(value, value or "источник")


def _education_label(value: str) -> str:
    return {
        "not_required": "не требуется",
        "equivalent_experience": "опыт вместо диплома",
        "preferred": "желательна",
        "required": "требуется",
        "not_mentioned": "не указано",
    }.get(value, value or "неясно")


def _scored_payload(item: ScoredJob) -> dict[str, Any]:
    posting = item.posting
    return {
        "fingerprint": job_fingerprint(posting),
        "source": posting.source,
        "title": posting.title,
        "company": posting.company,
        "url": posting.url,
        "score": item.score,
        "reservation_confidence": item.reservation_confidence,
        "education_requirement": item.education_requirement,
        "diia_city_resident": item.diia_city_resident,
        "agent_delegate_pct": item.agent_delegate_pct,
        "agent_delegate_label": item.agent_delegate_label,
        "reservation_evidence": [
            _reservation_evidence_payload(evidence)
            for evidence in item.reservation_evidence
        ],
        "reasons": list(item.reasons),
        "stack": list(item.stack),
    }


def _reservation_evidence_payload(evidence: ReservationEvidence) -> dict[str, str]:
    return {
        "kind": evidence.kind,
        "source_url": evidence.source_url,
        "quote": evidence.quote,
    }


def _reservation_evidence_from_payload(payload: Any) -> ReservationEvidence | None:
    if not isinstance(payload, dict):
        return None
    kind = str(payload.get("kind") or "").strip()
    source_url = str(payload.get("source_url") or "").strip()
    quote = str(payload.get("quote") or "").strip()
    if not kind or not source_url or not quote:
        return None
    return ReservationEvidence(kind=kind, source_url=source_url, quote=quote)


def _load_sent_fingerprints(storage: Storage) -> set[str]:
    return set(_decode_json_list(storage.get_state(JOBS_SENT_STATE_KEY)))


def _save_sent_fingerprints(storage: Storage, sent: set[str]) -> None:
    capped = sorted(sent)[-600:]
    storage.set_state(JOBS_SENT_STATE_KEY, json.dumps(capped, ensure_ascii=True))


def _decode_json_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip()]


def _node_text(parent: ET.Element, tag: str) -> str:
    node = parent.find(tag)
    if node is None or node.text is None:
        return ""
    return node.text.strip()


def _split_dou_title(title: str) -> tuple[str, str]:
    parts = [part.strip() for part in title.rsplit(" в ", 1)]
    if len(parts) != 2:
        return "", ""
    company_location = parts[1]
    chunks = [chunk.strip() for chunk in company_location.split(",")]
    company = chunks[0] if chunks else ""
    location = ", ".join(chunks[1:]) if len(chunks) > 1 else ""
    return company, location


def _parse_pub_date(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return value
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat()


def _strip_html(value: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", value, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _canonical_url(url: str) -> str:
    if not url:
        return ""
    split = urlsplit(url)
    return urlunsplit((split.scheme, split.netloc, split.path.rstrip("/"), "", ""))


def _truncate(text: str, limit: int) -> str:
    normalized = re.sub(r"\s+", " ", text).strip()
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 3)].rstrip(" ,.;:-") + "..."


def _dedupe_strings(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value.strip())
    return result
