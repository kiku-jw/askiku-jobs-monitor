from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import tempfile
import time
import types
from threading import Lock
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from askiku_jobs.jobs import (
    JobPosting,
    JOBS_COMPANY_RESERVATION_EVIDENCE_STATE_KEY,
    JOBS_SEEN_CANDIDATES_STATE_KEY,
    JOBS_SOURCE_WATERMARKS_STATE_KEY,
    _fetch_happy_monday_postings,
    _fetch_robota_ua_postings,
    _parse_jobs_ua_detail,
    _parse_work_ua_html,
    drain_jobs_alerts,
    fetch_job_sources,
    jobs_status_panel,
    score_job,
)
from askiku_jobs.storage import Storage


TEST_NOW = datetime(2026, 6, 4, 12, 0, tzinfo=UTC)


GOOD_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в ProductLab, віддалено</title>
    <link>https://jobs.dou.ua/companies/productlab/vacancies/1/?utm_source=jobsrss</link>
    <description>Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL. Є бронювання співробітників.</description>
    <pubDate>Thu, 04 Jun 2026 10:17:43 +0000</pubDate>
  </item>
  <item>
    <title>Robotics Software Engineer в Farsight Vision, віддалено</title>
    <link>https://jobs.dou.ua/companies/farsight/vacancies/2/?utm_source=jobsrss</link>
    <description>Python and ROS2. Your work will strengthen defense capability and support the Ukrainian Armed Forces.</description>
    <pubDate>Thu, 04 Jun 2026 09:00:00 +0000</pubDate>
  </item>
</channel></rss>
"""

DOU_XHR_JSON = json.dumps(
    {
        "html": """
        <li class="l-vacancy">
          <div class="date">4 червня</div>
          <div class="title">
            <a class="vt" href="https://jobs.dou.ua/companies/xhrlab/vacancies/777/?from=list_hot">Python Backend Engineer</a>
            <strong>в&nbsp;<a class="company" href="https://jobs.dou.ua/companies/xhrlab/vacancies/">XHRLab</a></strong>
            <span class="cities bi bi-geo-alt-fill">Київ, віддалено</span>
          </div>
          <div class="sh-info">Remote Python, FastAPI, LLM automation. Є бронювання працівників.</div>
        </li>
        """,
        "last": True,
        "num": 1,
    }
)

GOOD_WORK_HTML = """
<a name="7890001"></a>
<div tabindex="0" class="card card-hover card-visited wordwrap job-link">
  <h2><a tabindex="-1" href="/jobs/7890001/" title="Python Automation Engineer">Python Automation Engineer</a></h2>
  <div><span class="strong-600">80 000 грн</span></div>
  <div class="mt-xs">
    <span class="mr-xs"><span class="strong-600">OpsAI</span></span><span>Дистанційно</span>
  </div>
  <p class="ellipsis ellipsis-line ellipsis-line-3 text-default-7 mb-0">
    Python, FastAPI, LLM, API automation, PostgreSQL. Компанія має бронювання.
  </p>
  <time datetime="2026-06-04 17:09:32">вчора</time>
</div>
"""

WORK_HTML_WITH_SIDEBAR_RESERVATION_FILTER = """
<a name="8096745"></a>
<div tabindex="0" class="card card-hover card-visited wordwrap job-link">
  <h2><a tabindex="-1" href="/jobs/8096745/" title="Python developer (Telegram bot)">Python developer (Telegram bot)</a></h2>
  <div><span class="strong-600">35 000 - 57 000 грн</span></div>
  <div class="mt-xs">
    <span class="mr-xs"><span class="strong-600">Doreagency</span></span><span>Дистанційно</span>
  </div>
  <p class="ellipsis ellipsis-line ellipsis-line-3 text-default-7 mb-0">
    Python Telegram Developer / Backend Developer для сервісу Contenta.
  </p>
</div>
<aside>
  <label><input type="checkbox" value="1"><span>Бронювання працівників</span></label>
</aside>
"""

WORK_HTML_WITH_RESERVATION_BADGE = """
<a name="7890002"></a>
<div tabindex="0" class="card card-hover card-visited wordwrap job-link">
  <span class="label label-green-100 cursor-p">
    <span class="glyphicon glyphicon-deferment text-default-5"></span>
    <span>Бронювання</span>
  </span>
  <h2><a tabindex="-1" href="/jobs/7890002/" title="Python Backend Engineer">Python Backend Engineer</a></h2>
  <div class="mt-xs">
    <span class="mr-xs"><span class="strong-600">ReservedOps</span></span><span>Дистанційно</span>
  </div>
  <p class="ellipsis ellipsis-line ellipsis-line-3 text-default-7 mb-0">
    Python, FastAPI, LLM, API automation.
  </p>
</div>
"""

WORK_HTML_NO_RESERVATION_FOR_COMPANY_DISCOVERY = """
<a name="7890003"></a>
<div tabindex="0" class="card card-hover card-visited wordwrap job-link">
  <h2><a tabindex="-1" href="/jobs/7890003/" title="AI Automation Engineer">AI Automation Engineer</a></h2>
  <div class="mt-xs">
    <span class="mr-xs"><span class="strong-600">NoSignal Labs</span></span><span>Дистанційно</span>
  </div>
  <p class="ellipsis ellipsis-line ellipsis-line-3 text-default-7 mb-0">
    Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL.
  </p>
  <time datetime="2026-06-04 17:09:32">вчора</time>
</div>
"""

WORK_COMPANY_DISCOVERY_WITH_RESERVATION = """
<a name="7890004"></a>
<div tabindex="0" class="card card-hover card-visited wordwrap job-link">
  <span class="label label-green-100 cursor-p">
    <span class="glyphicon glyphicon-deferment text-default-5"></span>
    <span>Бронювання</span>
  </span>
  <h2><a tabindex="-1" href="/jobs/7890004/" title="Backend Developer">Backend Developer</a></h2>
  <div class="mt-xs">
    <span class="mr-xs"><span class="strong-600">NoSignal Labs</span></span><span>Дистанційно</span>
  </div>
  <p class="ellipsis ellipsis-line ellipsis-line-3 text-default-7 mb-0">
    Python, API automation. Компанія підтримує бронювання працівників.
  </p>
</div>
"""

NO_RESERVATION_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в NoSignal Labs, віддалено</title>
    <link>https://jobs.dou.ua/companies/no-signal/vacancies/10/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL.</description>
    <pubDate>Thu, 04 Jun 2026 10:17:43 +0000</pubDate>
  </item>
</channel></rss>
"""

ADJACENT_RESERVATION_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в ProductLab, віддалено</title>
    <link>https://jobs.dou.ua/companies/productlab/vacancies/11/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL.</description>
    <pubDate>Thu, 04 Jun 2026 10:17:43 +0000</pubDate>
  </item>
</channel></rss>
"""

DIIA_CITY_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>Python Developer (AI/Automation) в Софтсерв Технології, віддалено</title>
    <link>https://jobs.dou.ua/companies/softserve/vacancies/12/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, automation, PostgreSQL.</description>
    <pubDate>Thu, 04 Jun 2026 10:17:43 +0000</pubDate>
  </item>
</channel></rss>
"""

NON_REMOTE_RESERVED_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в OfficeLab, Київ</title>
    <link>https://jobs.dou.ua/companies/officelab/vacancies/31/?utm_source=jobsrss</link>
    <description>Python, FastAPI, LLM, RAG, API automation, PostgreSQL. Є бронювання співробітників.</description>
    <pubDate>Thu, 04 Jun 2026 10:17:43 +0000</pubDate>
  </item>
</channel></rss>
"""

EMPTY_WORK_HTML = "<html><body>No jobs</body></html>"
EMPTY_RSS = """<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel /></rss>"""
EMPTY_ROBOTA_JSON = '{"documents":[]}'

HAPPY_MONDAY_LISTING_HTML = """
<main>
  <article class="jobs-search__item">
    <a class="job-card__title" href="/jobs/1854323?hj">AI Automation Engineer</a>
    <a class="job-card__company" href="/company/productlab/vacancies">ProductLab</a>
    <span class="job-card__location">віддалено</span>
    <time datetime="2026-06-04">4 червня 2026</time>
    <p>Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL.</p>
  </article>
</main>
"""

HAPPY_MONDAY_DETAIL_HTML = """
<main>
  <h1>AI Automation Engineer</h1>
  <a href="/company/productlab/vacancies">ProductLab</a>
  <time datetime="2026-06-04T09:30:00+02:00">4 червня 2026</time>
  <section>
    <p>Full remote. Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL.</p>
    <p>Можливість бронювання після випробувального терміну.</p>
  </section>
</main>
"""

HAPPY_MONDAY_NON_REMOTE_DETAIL_HTML = """
<main>
  <h1>AI Automation Engineer</h1>
  <a href="/company/officelab/vacancies">OfficeLab</a>
  <time datetime="2026-06-04T09:30:00+02:00">4 червня 2026</time>
  <section>
    <p>Київ, офіс. Python, FastAPI, LLM, RAG, API automation, PostgreSQL.</p>
    <p>Можливість бронювання працівників.</p>
  </section>
</main>
"""

JOBS_UA_LISTING_HTML = """
<main>
  <li class="b-vacancy__item js-item_list" id="3845598">
    <div class="b-vacancy__top">
      <h3><a class="b-vacancy__top__title js-item_title" href="/job-spetsalst-po-api-ntegratsyah-3845598">
        Спеціаліст по API інтеграціях
      </a></h3>
    </div>
    <div class="b-vacancy__tech">
      <span class="b-vacancy__tech__item"><span class="link__hidden" title="Resteq">Resteq</span></span>
      <span class="b-vacancy__tech__item"><a class="link__hidden" href="https://jobs.ua/city/kiev_jobs">Київ</a></span>
    </div>
    <span class="b-vacancy__tech__item"><span class="caption">Освіта:</span>&nbsp;<span class="black-text">не має значення</span></span>
    <span class="b-vacancy__tech__item"><span class="caption">Графік роботи:</span>&nbsp;<span class="black-text">віддалена робота</span></span>
    <div class="grey-light b-text"><p>Python, PostgreSQL, API automation.</p></div>
  </li>
</main>
"""

JOBS_UA_DETAIL_JSONLD_HTML = """
<main>
  <script type="application/ld+json">
  {
    "@context": "https://schema.org",
    "@type": "JobPosting",
    "title": "Спеціаліст по API інтеграціях",
    "datePosted": "2026-06-04",
    "description": "<p>Віддалена робота. Python, PostgreSQL, API automation.</p><p>Є бронювання працівників.</p>",
    "hiringOrganization": {
      "@type": "Organization",
      "name": "Resteq"
    },
    "jobLocationType": "TELECOMMUTE",
    "jobLocation": {
      "@type": "Place",
      "address": {
        "@type": "PostalAddress",
        "addressLocality": "Київ",
        "addressCountry": "UA"
      }
    }
  }
  </script>
  <div class="b-vacancy-full js-item_full" id="3845598">
    <h1 class="default__full-title js-item_title">Спеціаліст по API інтеграціях</h1>
    <div class="b-vacancy-full__row"><span class="label">Компанія:</span><span class="control">Resteq</span></div>
    <div class="b-vacancy-full__row"><span class="label">Графік роботи:</span><span class="control">віддалена робота</span></div>
    <div class="b-vacancy-full__block b-text"><p>Є бронювання працівників.</p></div>
  </div>
</main>
"""

JOBS_UA_DIIA_ONLY_DETAIL_HTML = """
<main>
  <script type="application/ld+json">
  {
    "@context": "https://schema.org",
    "@type": "JobPosting",
    "title": "Python API Automation Engineer",
    "datePosted": "2026-06-04",
    "description": "<p>Remote Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL automation.</p>",
    "hiringOrganization": {"@type": "Organization", "name": "Софтсерв Технології"},
    "jobLocationType": "TELECOMMUTE",
    "jobLocation": {"@type": "Place", "address": {"addressLocality": "Київ", "addressCountry": "UA"}}
  }
  </script>
</main>
"""

JOBS_UA_VISIBLE_DETAIL_HTML = """
<main>
  <div class="b-vacancy-full js-item_full" id="3845598">
    <h1 class="default__full-title js-item_title">Спеціаліст по API інтеграціях</h1>
    <div class="b-vacancy-full__tech-wrapper">
      <span class="b-vacancy-full__tech__item m-r-1"><i class="fa fa-refresh"></i>&nbsp; 3 березня 2025</span>
      <span class="b-vacancy-full__tech__item m-r-1"><i class="fa fa-map-marker"></i>&nbsp;<a class="link__hidden" href="https://jobs.ua/vacancy/kiev">Київ</a></span>
    </div>
    <div class="b-vacancy-full__block">
      <div class="b-vacancy-full__row">
        <span class="label">Компанія:</span>
        <span class="control">
          <a href="https://jobs.ua/company-resteq-1638575">Resteq</a>
          <span class="for_print"> (https://jobs.ua/company-resteq-1638575) </span>
        </span>
      </div>
    </div>
    <div class="b-vacancy-full__block">
      <div class="b-vacancy-full__row"><span class="label">Графік роботи:</span><span class="control">віддалена робота</span></div>
    </div>
    <div class="b-vacancy-full__block b-text">
      <p>Python, PostgreSQL, API automation. Є бронювання працівників.</p>
    </div>
  </div>
</main>
"""

DJINNI_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer</title>
    <link>https://djinni.co/jobs/123456-ai-automation-engineer/</link>
    <description>
      ProductLab · Full Remote · Ukraine · Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL.
    </description>
    <pubDate>Thu, 04 Jun 2026 10:17:43 +0000</pubDate>
  </item>
</channel></rss>
"""

PRODUCTLAB_DOU_RESERVED_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>QA Engineer в ProductLab, віддалено</title>
    <link>https://jobs.dou.ua/companies/productlab/vacancies/99/?utm_source=jobsrss</link>
    <description>Remote QA automation. Доступне бронювання працівників.</description>
    <pubDate>Thu, 04 Jun 2026 09:17:43 +0000</pubDate>
  </item>
</channel></rss>
"""

EDUCATION_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в NoDegree Lab, віддалено</title>
    <link>https://jobs.dou.ua/companies/nodegree/vacancies/21/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL. Є бронювання. Degree not required.</description>
    <pubDate>Thu, 04 Jun 2026 10:17:43 +0000</pubDate>
  </item>
  <item>
    <title>Python Backend Engineer в Diploma Gate, віддалено</title>
    <link>https://jobs.dou.ua/companies/diploma/vacancies/22/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, PostgreSQL. Є бронювання. Bachelor's degree required.</description>
    <pubDate>Thu, 04 Jun 2026 09:17:43 +0000</pubDate>
  </item>
</channel></rss>
"""

FRESHNESS_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в OldStrong, віддалено</title>
    <link>https://jobs.dou.ua/companies/oldstrong/vacancies/41/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL. Є бронювання співробітників.</description>
    <pubDate>Sun, 31 May 2026 10:00:00 +0000</pubDate>
  </item>
  <item>
    <title>Python Backend Engineer в FreshFit, віддалено</title>
    <link>https://jobs.dou.ua/companies/freshfit/vacancies/42/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, API automation, PostgreSQL. Є бронювання співробітників.</description>
    <pubDate>Thu, 04 Jun 2026 10:00:00 +0000</pubDate>
  </item>
</channel></rss>
"""

OLD_RESERVED_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в OldStrong, віддалено</title>
    <link>https://jobs.dou.ua/companies/oldstrong/vacancies/41/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL. Є бронювання співробітників.</description>
    <pubDate>Sun, 31 May 2026 10:00:00 +0000</pubDate>
  </item>
</channel></rss>
"""

UNDATED_RESERVED_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в NoDate, віддалено</title>
    <link>https://jobs.dou.ua/companies/nodate/vacancies/43/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL. Є бронювання співробітників.</description>
  </item>
</channel></rss>
"""

FRESH_ONLY_DOU_RSS = """<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0"><channel>
  <item>
    <title>AI Automation Engineer в FreshOnly, віддалено</title>
    <link>https://jobs.dou.ua/companies/freshonly/vacancies/44/?utm_source=jobsrss</link>
    <description>Remote Python, FastAPI, LLM, RAG, Telegram API, PostgreSQL. Є бронювання співробітників.</description>
    <pubDate>Thu, 04 Jun 2026 10:00:00 +0000</pubDate>
  </item>
</channel></rss>
"""


def _diia_registry_payload(name: str) -> str:
    return json.dumps(
        {
            "data": [
                {
                    "legalForm": "ТОВ",
                    "name": name,
                    "edrpou": "38821791",
                    "residenceDate": "15.02.2022",
                    "residenceStatus": "active",
                    "startupFlag": "unknown",
                    "incomeEligibility": "unknown",
                    "decisionLink": [],
                }
            ],
            "links": {},
            "meta": {"current_page": 1, "last_page": 1, "per_page": 20, "total": 1},
        }
    )


class JobsMonitorTests(unittest.TestCase):
    def test_work_ua_sidebar_reservation_filter_does_not_mark_last_card_as_reserved(self) -> None:
        postings = _parse_work_ua_html(
            WORK_HTML_WITH_SIDEBAR_RESERVATION_FILTER,
            source_query="https://www.work.ua/jobs-remote-python/?advs=1",
        )

        self.assertEqual(len(postings), 1)
        self.assertNotIn("Бронювання працівників.", postings[0].summary)
        self.assertEqual(score_job(postings[0]).reservation_confidence, "unknown")

    def test_work_ua_reservation_badge_marks_card_as_reserved(self) -> None:
        postings = _parse_work_ua_html(
            WORK_HTML_WITH_RESERVATION_BADGE,
            source_query="https://www.work.ua/jobs-remote-python/?advs=1&deferment=1",
        )

        self.assertEqual(len(postings), 1)
        self.assertIn("Бронювання працівників.", postings[0].summary)
        self.assertEqual(score_job(postings[0]).reservation_confidence, "direct")

    def test_fetch_job_sources_parses_djinni_remote_posting(self) -> None:
        def fetcher(url: str) -> str:
            if "djinni.co" in url:
                return DJINNI_RSS
            if "jobs.dou.ua" in url:
                return '<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel /></rss>'
            return EMPTY_WORK_HTML

        postings = [
            posting
            for posting in fetch_job_sources(fetcher=fetcher, mode="new")
            if posting.source == "Djinni"
        ]

        self.assertEqual(len(postings), 1)
        posting = postings[0]
        self.assertEqual(posting.title, "AI Automation Engineer")
        self.assertEqual(posting.company, "ProductLab")
        self.assertEqual(posting.remote_mode, "remote")
        self.assertEqual(posting.posted_at, "2026-06-04T10:17:43+00:00")

    def test_fetch_job_sources_parses_dou_xhr_posting_in_heavy_mode(self) -> None:
        fetched_urls: list[str] = []

        def fetcher(url: str) -> str:
            fetched_urls.append(url)
            if "jobs.dou.ua/vacancies/xhr-load/" in url and "count=0" in url:
                return DOU_XHR_JSON
            if "jobs.dou.ua" in url:
                return EMPTY_RSS
            if "api.robota.ua/vacancy/search" in url:
                return EMPTY_ROBOTA_JSON
            return EMPTY_WORK_HTML

        postings = [
            posting
            for posting in fetch_job_sources(fetcher=fetcher, mode="heavy")
            if posting.url == "https://jobs.dou.ua/companies/xhrlab/vacancies/777/?from=list_hot"
        ]

        self.assertEqual(len(postings), 1)
        self.assertTrue(any("jobs.dou.ua/vacancies/xhr-load/" in url for url in fetched_urls))
        self.assertEqual(postings[0].source, "DOU")
        self.assertEqual(postings[0].company, "XHRLab")
        self.assertEqual(postings[0].remote_mode, "remote")
        self.assertIn("бронювання працівників", postings[0].summary.lower())
        self.assertTrue(postings[0].source_query.startswith("xhr:"))

    def test_fetch_job_sources_isolates_dou_xhr_failures(self) -> None:
        def fetcher(url: str) -> str:
            if "jobs.dou.ua/vacancies/xhr-load/" in url:
                raise RuntimeError("xhr failed")
            if "jobs.dou.ua" in url:
                return GOOD_DOU_RSS
            if "api.robota.ua/vacancy/search" in url:
                return EMPTY_ROBOTA_JSON
            return EMPTY_WORK_HTML

        postings = fetch_job_sources(fetcher=fetcher, mode="heavy")

        self.assertTrue(any(posting.source == "DOU" for posting in postings))
        self.assertTrue(
            any(
                posting.source == "internal" and "DOU XHR:" in posting.summary
                for posting in postings
            )
        )

    def test_fetch_job_sources_skips_dou_xhr_in_light_mode(self) -> None:
        fetched_urls: list[str] = []

        def fetcher(url: str) -> str:
            fetched_urls.append(url)
            if "jobs.dou.ua" in url:
                return EMPTY_RSS
            if "api.robota.ua/vacancy/search" in url:
                return EMPTY_ROBOTA_JSON
            return EMPTY_WORK_HTML

        fetch_job_sources(fetcher=fetcher, mode="light")

        self.assertFalse(any("jobs.dou.ua/vacancies/xhr-load/" in url for url in fetched_urls))

    def test_fetch_job_sources_parses_happy_monday_with_detail_evidence(self) -> None:
        def fetcher(url: str) -> str:
            if "happymonday.ua/jobs/1854323" in url:
                return HAPPY_MONDAY_DETAIL_HTML
            if "happymonday.ua/jobs-search" in url:
                return HAPPY_MONDAY_LISTING_HTML
            if "jobs.dou.ua" in url or "djinni.co" in url:
                return EMPTY_RSS
            if "api.robota.ua/vacancy/search" in url:
                return EMPTY_ROBOTA_JSON
            return EMPTY_WORK_HTML

        postings = [
            posting
            for posting in fetch_job_sources(fetcher=fetcher, mode="new")
            if posting.source == "Happy Monday"
        ]

        self.assertEqual(len(postings), 1)
        posting = postings[0]
        self.assertEqual(posting.title, "AI Automation Engineer")
        self.assertEqual(posting.company, "ProductLab")
        self.assertEqual(posting.url, "https://happymonday.ua/jobs/1854323?hj")
        self.assertEqual(posting.remote_mode, "remote")
        self.assertEqual(posting.posted_at, "2026-06-04T09:30:00+02:00")
        self.assertIn("Можливість бронювання", posting.summary)
        self.assertEqual(score_job(posting).reservation_confidence, "direct")

    def test_happy_monday_detail_enrichment_fetches_details_concurrently(self) -> None:
        listing = (
            HAPPY_MONDAY_LISTING_HTML
            + HAPPY_MONDAY_LISTING_HTML.replace("1854323", "1854324").replace(
                "AI Automation Engineer",
                "Backend Automation Engineer",
            )
        )
        active = 0
        max_active = 0
        lock = Lock()

        def fetcher(url: str) -> str:
            nonlocal active, max_active
            if "happymonday.ua/jobs-search" in url:
                return listing
            if "happymonday.ua/jobs/" not in url:
                return EMPTY_WORK_HTML
            with lock:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.05)
            with lock:
                active -= 1
            return HAPPY_MONDAY_DETAIL_HTML.replace("1854323", url.rsplit("/", 1)[-1])

        postings = _fetch_happy_monday_postings(
            fetcher=fetcher,
            source_query="https://happymonday.ua/jobs-search",
        )

        self.assertEqual(len(postings), 2)
        self.assertGreaterEqual(max_active, 2)

    def test_fetch_job_sources_parses_jobs_ua_jsonld_detail(self) -> None:
        def fetcher(url: str) -> str:
            if "jobs.ua/job-spetsalst-po-api-ntegratsyah-3845598" in url:
                return JOBS_UA_DETAIL_JSONLD_HTML
            if "jobs.ua/vacancy" in url:
                return JOBS_UA_LISTING_HTML
            if "jobs.dou.ua" in url or "djinni.co" in url:
                return EMPTY_RSS
            if "api.robota.ua/vacancy/search" in url:
                return EMPTY_ROBOTA_JSON
            return EMPTY_WORK_HTML

        postings = [
            posting
            for posting in fetch_job_sources(fetcher=fetcher, mode="new")
            if posting.source == "Jobs.ua"
        ]

        self.assertEqual(len(postings), 1)
        posting = postings[0]
        self.assertEqual(posting.title, "Спеціаліст по API інтеграціях")
        self.assertEqual(posting.company, "Resteq")
        self.assertEqual(posting.location, "Київ")
        self.assertEqual(posting.remote_mode, "remote")
        self.assertEqual(posting.posted_at, "2026-06-04")
        self.assertIn("Є бронювання працівників", posting.summary)

    def test_jobs_ua_visible_detail_fallback_extracts_clean_company_and_date(self) -> None:
        posting = _parse_jobs_ua_detail(
            JOBS_UA_VISIBLE_DETAIL_HTML,
            source_query="https://jobs.ua/vacancy/rabota-api",
            url="https://jobs.ua/job-spetsalst-po-api-ntegratsyah-3845598",
        )

        self.assertIsNotNone(posting)
        assert posting is not None
        self.assertEqual(posting.company, "Resteq")
        self.assertEqual(posting.location, "Київ")
        self.assertEqual(posting.remote_mode, "remote")
        self.assertEqual(posting.posted_at, "3 березня 2025")
        self.assertEqual(score_job(posting).reservation_confidence, "direct")

    def test_jobs_ua_detail_uses_optional_text_extraction_for_hidden_evidence(self) -> None:
        fake_trafilatura = types.SimpleNamespace(
            extract=lambda _html, **_kwargs: (
                "Remote Python, FastAPI, LLM automation. Є бронювання працівників."
            )
        )
        previous = sys.modules.get("trafilatura")
        sys.modules["trafilatura"] = fake_trafilatura
        try:
            posting = _parse_jobs_ua_detail(
                """
                <main>
                  <div class="b-vacancy-full js-item_full">
                    <h1 class="default__full-title js-item_title">Python API Automation Engineer</h1>
                    <div class="b-vacancy-full__row">
                      <span class="label">Компанія:</span><span class="control">Fallback Labs</span>
                    </div>
                  </div>
                </main>
                """,
                source_query="https://jobs.ua/vacancy/rabota-api",
                url="https://jobs.ua/job-api-777",
            )
        finally:
            if previous is None:
                sys.modules.pop("trafilatura", None)
            else:
                sys.modules["trafilatura"] = previous

        self.assertIsNotNone(posting)
        assert posting is not None
        self.assertIn("бронювання працівників", posting.summary.lower())
        self.assertEqual(posting.remote_mode, "remote")
        self.assertEqual(score_job(posting).reservation_confidence, "direct")

    def test_fetch_job_sources_isolates_happy_monday_and_jobs_ua_failures(self) -> None:
        def fetcher(url: str) -> str:
            if "happymonday.ua" in url:
                raise RuntimeError("happy monday unavailable")
            if "jobs.ua" in url:
                raise RuntimeError("jobs.ua unavailable")
            if "jobs.dou.ua" in url or "djinni.co" in url:
                return EMPTY_RSS
            if "api.robota.ua/vacancy/search" in url:
                return EMPTY_ROBOTA_JSON
            return EMPTY_WORK_HTML

        postings = fetch_job_sources(fetcher=fetcher, mode="new")
        problems = [
            posting.summary
            for posting in postings
            if posting.source == "internal" and posting.title == "source problems"
        ]

        self.assertEqual(len(problems), 1)
        self.assertIn("Happy Monday:", problems[0])
        self.assertIn("Jobs.ua:", problems[0])

    def test_score_job_prefers_ai_automation_and_reservation(self) -> None:
        posting = JobPosting(
            source="test",
            title="AI Automation Engineer",
            company="ProductLab",
            url="https://example.ua/jobs/1",
            summary="Remote Python FastAPI LLM RAG Telegram API PostgreSQL. Є бронювання.",
            remote_mode="remote",
        )

        scored = score_job(posting)

        self.assertGreaterEqual(scored.score, 70)
        self.assertEqual(scored.reservation_confidence, "direct")
        self.assertFalse(scored.disqualifiers)
        self.assertGreaterEqual(scored.agent_delegate_pct, 75)

    def test_score_job_penalizes_low_agent_autonomy_work(self) -> None:
        posting = JobPosting(
            source="test",
            title="Project Manager",
            company="OpsCorp",
            url="https://example.ua/jobs/pm",
            summary="Remote client calls, stakeholder management, roadmap ownership. Є бронювання.",
            remote_mode="remote",
        )

        scored = score_job(posting)

        self.assertLessEqual(scored.agent_delegate_pct, 40)

    def test_score_job_caps_management_autonomy_even_with_technical_terms(self) -> None:
        posting = JobPosting(
            source="test",
            title="Product Manager for AI Automation",
            company="OpsCorp",
            url="https://example.ua/jobs/pm-ai",
            summary=(
                "Remote Python scripts, API automation, ETL pipeline, Docker, dashboard. "
                "Own roadmap, stakeholder engagement, cross-functional planning. Є бронювання."
            ),
            remote_mode="remote",
        )

        scored = score_job(posting)

        self.assertLessEqual(scored.agent_delegate_pct, 20)

    def test_score_job_caps_agent_autonomy_for_compliance_risk(self) -> None:
        posting = JobPosting(
            source="test",
            title="Data Engineer",
            company="FinData",
            url="https://example.ua/jobs/compliance",
            summary=(
                "Remote Python scripts, ETL pipeline, SQL query automation, data processing. "
                "PCI DSS, GDPR compliance, security audit. Є бронювання."
            ),
            remote_mode="remote",
        )

        scored = score_job(posting)

        self.assertLessEqual(scored.agent_delegate_pct, 20)

    def test_score_job_blocks_clearance_roles(self) -> None:
        posting = JobPosting(
            source="test",
            title="Backend Engineer",
            company="Restricted Systems",
            url="https://example.ua/jobs/clearance",
            summary="Remote Python API automation. Secret clearance required. Є бронювання.",
            remote_mode="remote",
        )

        scored = score_job(posting)

        self.assertTrue(scored.disqualifiers)
        self.assertFalse(scored.is_alertable)

    def test_score_job_prefers_roles_without_degree_requirement(self) -> None:
        baseline = JobPosting(
            source="test",
            title="AI Automation Engineer",
            company="ProductLab",
            url="https://example.ua/jobs/degree-neutral",
            summary="Remote Python API automation. Є бронювання.",
            remote_mode="remote",
        )
        no_degree = JobPosting(
            source="test",
            title="AI Automation Engineer",
            company="ProductLab",
            url="https://example.ua/jobs/no-degree",
            summary="Remote Python API automation. Є бронювання. Degree not required.",
            remote_mode="remote",
        )

        baseline_scored = score_job(baseline)
        no_degree_scored = score_job(no_degree)

        self.assertEqual(no_degree_scored.education_requirement, "not_required")
        self.assertGreater(no_degree_scored.score, baseline_scored.score)
        self.assertIn("degree not required", no_degree_scored.reasons)

    def test_score_job_marks_explicit_degree_requirement_as_risk_not_gate(self) -> None:
        posting = JobPosting(
            source="test",
            title="AI Automation Engineer",
            company="ProductLab",
            url="https://example.ua/jobs/degree-required",
            summary="Remote Python FastAPI LLM RAG Telegram API PostgreSQL. Є бронювання. Bachelor's degree required.",
            remote_mode="remote",
        )

        scored = score_job(posting)

        self.assertEqual(scored.education_requirement, "required")
        self.assertTrue(scored.is_alertable)

    def test_score_job_treats_explicit_remote_with_optional_office_as_remote_risk(self) -> None:
        posting = JobPosting(
            source="test",
            title="Software Engineer",
            company="RemoteOptional",
            url="https://example.ua/jobs/remote-office-optional",
            summary="Remote work. Office optional. Python API. Є бронювання.",
        )

        scored = score_job(posting)

        self.assertTrue(scored.is_alertable)
        self.assertIn("risk: remote unclear/optional office", scored.reasons)

    def test_score_job_blocks_war_related_roles(self) -> None:
        posting = JobPosting(
            source="test",
            title="Robotics Software Engineer",
            company="Farsight Vision",
            url="https://example.ua/jobs/2",
            summary="Python ROS2 work for defense capability and Ukrainian Armed Forces.",
            remote_mode="remote",
        )

        scored = score_job(posting)

        self.assertEqual(scored.score, 0)
        self.assertTrue(scored.disqualifiers)

    def test_drain_jobs_alerts_dedupes_sent_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return GOOD_DOU_RSS
                return GOOD_WORK_HTML

            first = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)
            second = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(first["count"], 2)
            self.assertIn("AI Automation Engineer", first["message"])
            self.assertIn("Python Automation Engineer", first["message"])
            self.assertNotIn("Robotics Software Engineer", first["message"])
            self.assertEqual(second["count"], 0)

            status = jobs_status_panel(storage)
            self.assertIn("Последний прогон:", status)
            self.assertIn("Djinni RSS", status)
            self.assertIn("Уже отправлено: 2", status)

    def test_drain_jobs_alerts_includes_direct_reservation_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return GOOD_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            item = next(item for item in result["items"] if item["source"] == "DOU")
            evidence = item["reservation_evidence"][0]
            self.assertEqual(evidence["kind"], "direct")
            self.assertEqual(
                evidence["source_url"],
                "https://jobs.dou.ua/companies/productlab/vacancies/1/?utm_source=jobsrss",
            )
            self.assertIn("бронювання співробітників", evidence["quote"].lower())
            self.assertIn("доказательство брони:", result["message"])

    def test_drain_jobs_alerts_hides_perfect_fit_without_reservation_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return NO_RESERVATION_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["without_reservation"], 1)
            near_misses = result["status"]["near_misses"]
            self.assertEqual(near_misses[0]["reason"], "нет direct/adjacent брони")
            self.assertEqual(near_misses[0]["title"], "AI Automation Engineer в NoSignal Labs, віддалено")

            status = jobs_status_panel(storage)
            self.assertIn("Почти кандидаты:", status)
            self.assertIn("нет direct/adjacent брони", status)
            self.assertIn("AI Automation Engineer в NoSignal Labs", status)

    def test_drain_jobs_alerts_soft_passes_low_fit_direct_reserved_remote_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()
            rss = """<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel>
              <item>
                <title>Software Engineer в LowFit Direct, віддалено</title>
                <link>https://jobs.dou.ua/companies/lowfit/vacancies/45/?utm_source=jobsrss</link>
                <description>Remote Python. Є бронювання працівників.</description>
                <pubDate>Thu, 04 Jun 2026 10:17:43 +0000</pubDate>
              </item>
            </channel></rss>
            """

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return rss
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 1)
            self.assertIn("fit низкий", result["message"])
            self.assertEqual(result["items"][0]["reservation_confidence"], "direct")

    def test_company_discovery_promotes_good_work_ua_near_miss_with_adjacent_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()
            discovery_fetches = 0

            def fetcher(url: str) -> str:
                nonlocal discovery_fetches
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url or "djinni.co" in url:
                    return EMPTY_RSS
                if "api.robota.ua/vacancy/search" in url:
                    return EMPTY_ROBOTA_JSON
                if "happymonday.ua" in url or "jobs.ua" in url:
                    return EMPTY_WORK_HTML
                if "www.work.ua/jobs-remote/" in url and "NoSignal+Labs" in url:
                    discovery_fetches += 1
                    return WORK_COMPANY_DISCOVERY_WITH_RESERVATION
                if "www.work.ua/jobs-remote/" in url:
                    return WORK_HTML_NO_RESERVATION_FOR_COMPANY_DISCOVERY
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 1)
            self.assertEqual(discovery_fetches, 1)
            item = result["items"][0]
            self.assertEqual(item["reservation_confidence"], "adjacent")
            evidence = item["reservation_evidence"][0]
            self.assertEqual(evidence["kind"], "adjacent")
            self.assertEqual(evidence["source_url"], "https://www.work.ua/jobs/7890004/")
            self.assertIn("бронювання працівників", evidence["quote"].lower())

    def test_company_discovery_can_promote_multiple_adjacent_matches_per_heavy_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()
            company_items = "\n".join(
                f"""
                <item>
                  <title>AI Automation Engineer в MultiFit {i}, віддалено</title>
                  <link>https://jobs.dou.ua/companies/multifit{i}/vacancies/{i}/?utm_source=jobsrss</link>
                  <description>Remote Python FastAPI LLM RAG Telegram API PostgreSQL automation.</description>
                  <pubDate>Thu, 04 Jun 2026 10:{i:02d}:00 +0000</pubDate>
                </item>
                """
                for i in range(3)
            )
            rss = f"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel>{company_items}</channel></rss>
            """

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return rss
                if "www.work.ua/jobs-remote/" in url:
                    for i in range(3):
                        if f"MultiFit+{i}" in url:
                            return WORK_COMPANY_DISCOVERY_WITH_RESERVATION.replace(
                                "NoSignal Labs",
                                f"MultiFit {i}",
                            )
                    return EMPTY_WORK_HTML
                if "api.robota.ua/vacancy/search" in url:
                    return EMPTY_ROBOTA_JSON
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 3)
            self.assertEqual(
                {item["reservation_confidence"] for item in result["items"]},
                {"adjacent"},
            )

    def test_company_discovery_budget_targets_best_near_misses_first(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()
            discovery_urls: list[str] = []
            low_items = "\n".join(
                f"""
                <item>
                  <title>Backend Engineer в LowCompany{i}, віддалено</title>
                  <link>https://jobs.dou.ua/companies/low{i}/vacancies/{i}/?utm_source=jobsrss</link>
                  <description>Remote Python API automation PostgreSQL.</description>
                  <pubDate>Thu, 04 Jun 2026 10:{i:02d}:00 +0000</pubDate>
                </item>
                """
                for i in range(12)
            )
            rss = f"""<?xml version="1.0" encoding="utf-8"?>
            <rss version="2.0"><channel>
              {low_items}
              <item>
                <title>AI Automation Engineer в TopFit Labs, віддалено</title>
                <link>https://jobs.dou.ua/companies/topfit/vacancies/99/?utm_source=jobsrss</link>
                <description>Remote Python FastAPI LLM RAG Telegram API PostgreSQL automation.</description>
                <pubDate>Thu, 04 Jun 2026 10:59:00 +0000</pubDate>
              </item>
            </channel></rss>
            """

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return rss
                if "www.work.ua/jobs-remote/" in url and "TopFit+Labs" in url:
                    discovery_urls.append(url)
                    return WORK_COMPANY_DISCOVERY_WITH_RESERVATION.replace(
                        "NoSignal Labs",
                        "TopFit Labs",
                    )
                if "www.work.ua/jobs-remote/" in url:
                    if "LowCompany" in url:
                        discovery_urls.append(url)
                    return EMPTY_WORK_HTML
                if "api.robota.ua/vacancy/search" in url:
                    return EMPTY_ROBOTA_JSON
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 1)
            self.assertIn("TopFit Labs", result["items"][0]["title"])
            self.assertTrue(any("TopFit+Labs" in url for url in discovery_urls))
            self.assertLessEqual(len(discovery_urls), 12)

    def test_light_mode_skips_company_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()
            discovery_fetches = 0

            def fetcher(url: str) -> str:
                nonlocal discovery_fetches
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url or "djinni.co" in url:
                    return EMPTY_RSS
                if "api.robota.ua/vacancy/search" in url:
                    return EMPTY_ROBOTA_JSON
                if "happymonday.ua" in url or "jobs.ua" in url:
                    return EMPTY_WORK_HTML
                if "www.work.ua/jobs-remote/" in url and "NoSignal+Labs" in url:
                    discovery_fetches += 1
                    return WORK_COMPANY_DISCOVERY_WITH_RESERVATION
                if "www.work.ua/jobs-remote/" in url:
                    return WORK_HTML_NO_RESERVATION_FOR_COMPANY_DISCOVERY
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=5,
                mode="light",
                now=TEST_NOW,
            )

            self.assertEqual(result["count"], 0)
            self.assertEqual(discovery_fetches, 0)
            self.assertEqual(result["status"]["company_discovery_fetches"], 0)

    def test_status_records_hard_gate_counts_source_health_and_evidence_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return GOOD_DOU_RSS
                if "api.robota.ua/vacancy/search" in url:
                    return EMPTY_ROBOTA_JSON
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)
            status = result["status"]

            self.assertIn("gate_counts", status)
            self.assertGreaterEqual(status["gate_counts"]["alertable"], 1)
            self.assertGreaterEqual(status["gate_counts"]["blocked"], 1)
            self.assertIn("source_health", status)
            dou_health = [
                row for row in status["source_health"]
                if row["source"] == "DOU" and row["source_query"] == "python бронювання"
            ][0]
            self.assertGreaterEqual(dou_health["fetched"], 1)
            self.assertGreaterEqual(dou_health["direct_reservation"], 1)
            ledger_raw = storage.get_state("jobs.reservation_evidence_ledger")
            self.assertIsNotNone(ledger_raw)
            ledger = json.loads(ledger_raw or "[]")
            self.assertTrue(
                any(
                    entry["company_identity"] == "productlab"
                    and entry["evidence_type"] == "direct"
                    and entry["source_url"].startswith("https://jobs.dou.ua/")
                    and entry["expires_at"]
                    for entry in ledger
                )
            )

    def test_drain_jobs_alerts_hides_non_remote_even_with_reservation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return NON_REMOTE_RESERVED_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["non_remote_hidden"], 1)

    def test_happy_monday_reservation_still_requires_remote_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "happymonday.ua/jobs/1854323" in url:
                    return HAPPY_MONDAY_NON_REMOTE_DETAIL_HTML
                if "happymonday.ua/jobs-search" in url:
                    return HAPPY_MONDAY_LISTING_HTML.replace("віддалено", "Київ")
                if "jobs.dou.ua" in url or "djinni.co" in url:
                    return EMPTY_RSS
                if "api.robota.ua/vacancy/search" in url:
                    return EMPTY_ROBOTA_JSON
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["non_remote_hidden"], 1)

    def test_jobs_ua_diia_city_without_reservation_stays_hidden(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"СОФТСЕРВ ТЕХНОЛОГІЇ"')
                if "jobs.ua/job-spetsalst-po-api-ntegratsyah-3845598" in url:
                    return JOBS_UA_DIIA_ONLY_DETAIL_HTML
                if "jobs.ua/vacancy" in url:
                    return JOBS_UA_LISTING_HTML
                if "jobs.dou.ua" in url or "djinni.co" in url:
                    return EMPTY_RSS
                if "api.robota.ua/vacancy/search" in url:
                    return EMPTY_ROBOTA_JSON
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["diia_city_only_hidden"], 1)

    def test_drain_jobs_alerts_accepts_adjacent_company_reservation_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if url == "https://jobs.dou.ua/companies/productlab/vacancies/":
                    return "Інша вакансія цієї компанії: доступне бронювання працівників."
                if "jobs.dou.ua" in url:
                    return ADJACENT_RESERVATION_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 1)
            self.assertIn("бронь: есть в соседних вакансиях компании", result["message"])
            evidence = result["items"][0]["reservation_evidence"][0]
            self.assertEqual(evidence["kind"], "adjacent")
            self.assertEqual(evidence["source_url"], "https://jobs.dou.ua/companies/productlab/vacancies/")
            self.assertIn("доступне бронювання працівників", evidence["quote"].lower())

    def test_adjacent_reservation_ignores_physical_armor_words(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if url == "https://jobs.dou.ua/companies/productlab/vacancies/":
                    return "Безпека офісу: охорона, броньовані вікна й двері, система захисту."
                if "jobs.dou.ua" in url:
                    return ADJACENT_RESERVATION_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["without_reservation"], 1)
            raw_cache = storage.get_state(JOBS_COMPANY_RESERVATION_EVIDENCE_STATE_KEY)
            self.assertIsNotNone(raw_cache)
            cache = json.loads(raw_cache or "{}")
            self.assertIsNone(cache["https://jobs.dou.ua/companies/productlab/vacancies/"]["evidence"])

    def test_company_reservation_evidence_cache_reuses_adjacent_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()
            company_url = "https://jobs.dou.ua/companies/productlab/vacancies/"
            company_fetches = 0

            def fetcher(url: str) -> str:
                nonlocal company_fetches
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if url == company_url:
                    company_fetches += 1
                    if company_fetches > 1:
                        raise AssertionError("company reservation evidence cache was not reused")
                    return "Інша вакансія цієї компанії: доступне бронювання працівників."
                if "jobs.dou.ua" in url:
                    return ADJACENT_RESERVATION_DOU_RSS
                return EMPTY_WORK_HTML

            first = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, dry_run=True, now=TEST_NOW)
            second = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, dry_run=True, now=TEST_NOW)

            self.assertEqual(first["count"], 1)
            self.assertEqual(second["count"], 1)
            self.assertEqual(company_fetches, 1)
            raw_cache = storage.get_state(JOBS_COMPANY_RESERVATION_EVIDENCE_STATE_KEY)
            self.assertIsNotNone(raw_cache)
            self.assertIn(company_url, raw_cache or "")
            status = jobs_status_panel(storage)
            self.assertIn("Company evidence cache: 1 entries, 1 with бронь", status)

    def test_drain_jobs_alerts_accepts_diia_city_company_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"СОФТСЕРВ ТЕХНОЛОГІЇ"')
                if "jobs.dou.ua" in url:
                    return DIIA_CITY_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["diia_city_only_hidden"], 1)

    def test_adjacent_reservation_signal_takes_priority_over_diia_city_context(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"PRODUCTLAB"')
                if url == "https://jobs.dou.ua/companies/productlab/vacancies/":
                    return "Інша вакансія цієї компанії: доступне бронювання працівників."
                if "jobs.dou.ua" in url:
                    return ADJACENT_RESERVATION_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 1)
            self.assertIn("бронь: есть в соседних вакансиях компании", result["message"])
            self.assertEqual(result["items"][0]["reservation_confidence"], "adjacent")
            self.assertTrue(result["items"][0]["diia_city_resident"])

    def test_djinni_posting_uses_cross_source_company_reservation_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "djinni.co" in url:
                    return DJINNI_RSS
                if "jobs.dou.ua" in url:
                    return PRODUCTLAB_DOU_RESERVED_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 1)
            self.assertEqual(result["items"][0]["source"], "Djinni")
            self.assertEqual(result["items"][0]["reservation_confidence"], "adjacent")
            evidence = result["items"][0]["reservation_evidence"][0]
            self.assertEqual(evidence["kind"], "adjacent")
            self.assertIn("jobs.dou.ua", evidence["source_url"])
            self.assertIn("бронювання працівників", evidence["quote"].lower())

    def test_fetch_job_sources_queries_work_ua_deferment_and_robota_ua_reservation(self) -> None:
        fetched_urls: list[str] = []

        def fetcher(url: str) -> str:
            fetched_urls.append(url)
            if "jobs.dou.ua" in url:
                return '<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel /></rss>'
            return EMPTY_WORK_HTML

        fetch_job_sources(fetcher=fetcher)

        self.assertTrue(any("work.ua" in url and "deferment=1" in url for url in fetched_urls))
        self.assertTrue(any("robota.ua" in url and "isReservation=true" in url for url in fetched_urls))
        work_urls = [url for url in fetched_urls if "work.ua" in url]
        self.assertTrue(any("search=backend" in url.lower() for url in work_urls))
        self.assertTrue(any("search=llm" in url.lower() for url in work_urls))
        self.assertTrue(any("search=fastapi" in url.lower() for url in work_urls))
        self.assertTrue(any("search=telegram" in url.lower() for url in work_urls))
        self.assertTrue(any("search=n8n" in url.lower() for url in work_urls))
        self.assertTrue(any("search=rpa" in url.lower() for url in work_urls))
        self.assertTrue(any("djinni.co" in url for url in fetched_urls))

    def test_drain_jobs_alerts_accepts_robota_ua_reservation_badge(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            robota_payload = json.dumps(
                {
                    "documents": [
                        {
                            "id": 101,
                            "notebookId": 202,
                            "name": "AI Automation Engineer",
                            "companyName": "Robota Product",
                            "cityName": "Київ",
                            "date": "2026-06-04T10:00:00",
                            "shortDescription": "Python, FastAPI, LLM, RAG, automation.",
                            "badges": [{"id": 1, "name": "Бронирование сотрудников"}],
                        }
                    ]
                }
            )
            robota_detail = json.dumps(
                {
                    "id": 101,
                    "notebookId": 202,
                    "name": "AI Automation Engineer",
                    "companyName": "Robota Product",
                    "cityName": "Київ",
                    "date": "2026-06-04T10:00:00",
                    "scheduleId": 3,
                    "description": "Remote Python, FastAPI, LLM, RAG, automation.",
                    "badges": [{"id": 1, "name": "Бронирование сотрудников"}],
                }
            )

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return '<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel /></rss>'
                if "api.robota.ua/vacancy/search" in url:
                    return robota_payload
                if "api.robota.ua/vacancy?id=101" in url:
                    return robota_detail
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 1)
            self.assertIn("AI Automation Engineer", result["message"])
            self.assertIn("AI-автономность:", result["message"])
            self.assertIn("(agent_first)", result["message"])
            self.assertEqual(result["items"][0]["source"], "Robota.ua")
            self.assertEqual(result["items"][0]["reservation_confidence"], "direct")
            self.assertGreaterEqual(result["items"][0]["agent_delegate_pct"], 75)
            self.assertEqual(result["items"][0]["agent_delegate_label"], "agent_first")

    def test_robota_ua_postings_paginates_dedupes_and_respects_detail_cap(self) -> None:
        def item(vacancy_id: int) -> dict[str, object]:
            return {
                "id": vacancy_id,
                "notebookId": 900 + vacancy_id,
                "name": f"Python Backend Engineer {vacancy_id}",
                "companyName": f"Robota Product {vacancy_id}",
                "cityName": "Київ",
                "date": "2026-06-04T10:00:00",
                "scheduleId": 3,
                "shortDescription": "Remote Python, FastAPI, API automation.",
                "badges": [{"id": 1, "name": "Бронирование сотрудников"}],
            }

        page1 = [item(vacancy_id) for vacancy_id in range(100, 121)]
        page2 = [item(120), item(130)]
        search_urls: list[str] = []
        detail_ids: list[int] = []

        def fetcher(url: str) -> str:
            if "api.robota.ua/vacancy/search" in url:
                search_urls.append(url)
                if "page=2" in url:
                    return json.dumps({"documents": page2})
                if "page=3" in url:
                    return EMPTY_ROBOTA_JSON
                return json.dumps({"documents": page1})
            if "api.robota.ua/vacancy?id=" in url:
                vacancy_id = int(url.rsplit("=", 1)[1])
                detail_ids.append(vacancy_id)
                detail = item(vacancy_id)
                detail["description"] = "Remote Python, FastAPI, LLM, automation."
                return json.dumps(detail)
            return EMPTY_ROBOTA_JSON

        postings = _fetch_robota_ua_postings(
            fetcher=fetcher,
            source_query=(
                "https://api.robota.ua/vacancy/search?"
                "keyWords=python&scheduleId=3&isReservation=true"
            ),
        )

        self.assertTrue(any("page=2" in url for url in search_urls))
        self.assertTrue(all("scheduleId=3" in url for url in search_urls))
        self.assertTrue(all("isReservation=true" in url for url in search_urls))
        self.assertEqual(len(detail_ids), 20)
        self.assertEqual(len({posting.url for posting in postings}), 22)
        self.assertTrue(any("vacancy130" in posting.url for posting in postings))

    def test_drain_jobs_alerts_hides_robota_ua_when_detail_is_not_remote(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            robota_payload = json.dumps(
                {
                    "documents": [
                        {
                            "id": 101,
                            "notebookId": 202,
                            "name": "AI Automation Engineer",
                            "companyName": "Robota Product",
                            "cityName": "Київ",
                            "date": "2026-06-04T10:00:00",
                            "shortDescription": "Python, FastAPI, LLM, RAG, automation.",
                            "badges": [{"id": 1, "name": "Бронирование сотрудников"}],
                        }
                    ]
                }
            )
            robota_detail = json.dumps(
                {
                    "id": 101,
                    "notebookId": 202,
                    "name": "AI Automation Engineer",
                    "companyName": "Robota Product",
                    "cityName": "Київ",
                    "date": "2026-06-04T10:00:00",
                    "scheduleId": 1,
                    "description": "Office Python, FastAPI, LLM, RAG, automation.",
                    "badges": [{"id": 1, "name": "Бронирование сотрудников"}],
                }
            )

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return '<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel /></rss>'
                if "api.robota.ua/vacancy/search" in url:
                    return robota_payload
                if "api.robota.ua/vacancy?id=101" in url:
                    return robota_detail
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 0)

    def test_drain_jobs_alerts_shows_education_label_without_hiding_required_degree(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return EDUCATION_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(storage=storage, fetcher=fetcher, limit=5, now=TEST_NOW)

            self.assertEqual(result["count"], 2)
            self.assertIn("AI Automation Engineer", result["message"])
            self.assertIn("Python Backend Engineer", result["message"])
            self.assertIn("образование: не требуется", result["message"])
            self.assertIn("образование: требуется", result["message"])
            self.assertEqual(result["status"]["required_education_hidden"], 0)
            self.assertEqual(result["status"]["required_education_risk"], 1)

    def test_new_mode_hides_stale_reserved_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return OLD_RESERVED_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=5,
                now=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["stale_hidden"], 1)

    def test_new_mode_hides_undated_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return UNDATED_RESERVED_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=5,
                now=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["undated_hidden"], 1)

    def test_backfill_mode_can_preview_stale_and_undated_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return OLD_RESERVED_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=5,
                dry_run=True,
                mode="backfill",
                now=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(result["count"], 1)
            self.assertEqual(result["status"]["mode"], "backfill")
            self.assertEqual(result["status"]["stale_hidden"], 0)

    def test_new_mode_ranks_fresh_candidate_before_higher_score_stale_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return FRESHNESS_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=1,
                now=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(result["count"], 1)
            self.assertIn("FreshFit", result["message"])
            self.assertNotIn("OldStrong", result["message"])
            self.assertEqual(result["status"]["stale_hidden"], 1)

    def test_new_mode_hides_jobs_at_or_before_source_watermark(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()
            storage.set_state(
                JOBS_SOURCE_WATERMARKS_STATE_KEY,
                json.dumps({"python бронювання": "2026-06-04T10:00:00+00:00"}),
            )

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return FRESH_ONLY_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=5,
                now=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(result["count"], 0)
            self.assertEqual(result["status"]["watermark_hidden"], 1)

    def test_new_mode_updates_seen_cache_and_source_watermarks_after_real_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return FRESH_ONLY_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=5,
                now=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(result["count"], 1)
            seen_raw = storage.get_state(JOBS_SEEN_CANDIDATES_STATE_KEY)
            watermarks_raw = storage.get_state(JOBS_SOURCE_WATERMARKS_STATE_KEY)
            self.assertIsNotNone(seen_raw)
            self.assertIsNotNone(watermarks_raw)
            self.assertIn("FreshOnly", seen_raw or "")
            self.assertIn("python бронювання", watermarks_raw or "")
            seen = json.loads(seen_raw or "{}")
            self.assertTrue(any(row.get("status") == "selected" for row in seen.values()))

    def test_seen_candidates_records_recheckable_and_blocked_statuses(self) -> None:
        blocked_rss = NO_RESERVATION_DOU_RSS.replace(
            "</channel></rss>",
            """
              <item>
                <title>Python Backend Engineer в DefenseLab, віддалено</title>
                <link>https://jobs.dou.ua/companies/defenselab/vacancies/45/?utm_source=jobsrss</link>
                <description>Remote Python, FastAPI, API automation. Є бронювання співробітників. Miltech defense systems.</description>
                <pubDate>Thu, 04 Jun 2026 10:00:00 +0000</pubDate>
              </item>
            </channel></rss>
            """,
        )
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return blocked_rss
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=5,
                now=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(result["count"], 0)
            seen = json.loads(storage.get_state(JOBS_SEEN_CANDIDATES_STATE_KEY) or "{}")
            statuses = {row.get("title"): row for row in seen.values()}
            no_signal = statuses["AI Automation Engineer в NoSignal Labs, віддалено"]
            blocked = statuses["Python Backend Engineer в DefenseLab, віддалено"]

            self.assertEqual(no_signal["decision"], "without_reservation")
            self.assertEqual(no_signal["status"], "waiting_reservation_evidence")
            self.assertEqual(no_signal["primary_block"], "reservation")
            self.assertEqual(no_signal["reservation_confidence"], "unknown")
            self.assertTrue(no_signal["remote"])
            self.assertIn("recheck_after", no_signal)
            self.assertEqual(blocked["status"], "blocked_defense")
            self.assertEqual(blocked["primary_block"], "defense")
            self.assertGreaterEqual(
                result["status"]["candidate_status_counts"]["waiting_reservation_evidence"],
                1,
            )
            self.assertGreaterEqual(
                result["status"]["candidate_status_counts"]["blocked_defense"],
                1,
            )

    def test_dry_run_does_not_advance_seen_cache_or_source_watermarks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            storage = Storage(Path(tmp) / "asmonday.sqlite3")
            storage.initialize()

            def fetcher(url: str) -> str:
                if "city-backend.diia.gov.ua" in url:
                    return _diia_registry_payload('"OTHER COMPANY"')
                if "jobs.dou.ua" in url:
                    return FRESH_ONLY_DOU_RSS
                return EMPTY_WORK_HTML

            result = drain_jobs_alerts(
                storage=storage,
                fetcher=fetcher,
                limit=5,
                dry_run=True,
                now=datetime(2026, 6, 4, 12, 0, tzinfo=UTC),
            )

            self.assertEqual(result["count"], 1)
            self.assertIsNone(storage.get_state(JOBS_SEEN_CANDIDATES_STATE_KEY))
            self.assertIsNone(storage.get_state(JOBS_SOURCE_WATERMARKS_STATE_KEY))

    def test_fetch_job_sources_uses_broad_queries_in_new_mode(self) -> None:
        new_urls: list[str] = []
        backfill_urls: list[str] = []

        def new_fetcher(url: str) -> str:
            new_urls.append(url)
            if "jobs.dou.ua" in url:
                return '<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel /></rss>'
            return EMPTY_WORK_HTML

        def backfill_fetcher(url: str) -> str:
            backfill_urls.append(url)
            if "jobs.dou.ua" in url:
                return '<?xml version="1.0" encoding="utf-8"?><rss version="2.0"><channel /></rss>'
            return EMPTY_WORK_HTML

        fetch_job_sources(fetcher=new_fetcher, mode="new")
        fetch_job_sources(fetcher=backfill_fetcher, mode="backfill")

        self.assertTrue(any("python+remote" in url for url in new_urls))
        self.assertTrue(any(url == "https://www.work.ua/jobs-remote-python/?advs=1" for url in new_urls))
        self.assertTrue(any("python+remote" in url for url in backfill_urls))
        self.assertTrue(any(url == "https://www.work.ua/jobs-remote-python/?advs=1" for url in backfill_urls))


if __name__ == "__main__":
    unittest.main()
