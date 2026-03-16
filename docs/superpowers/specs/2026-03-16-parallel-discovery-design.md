# Parallel Multi-Agent Discovery

## Goal

Replace the single sequential discover agent with a supervisor + parallel worker pattern to reduce wall-clock time and eliminate Bedrock timeouts.

## Current State

`stages/discover.py` runs one pydantic-ai Agent (currently Sonnet/GPT-5-nano) that sequentially: reads the company profile, searches the web, fetches sitemaps, probes URLs, and saves results. Each tool call blocks the next. A complex company can take 10+ minutes and hit Bedrock connection timeouts.

## Design Rationale

This design uses a structured supervisor→worker topology with no inter-agent communication. Research confirms this is the recommended pattern — unstructured peer-to-peer agent networks amplify errors up to 17x (Google DeepMind, Dec 2025). Workers don't need to talk to each other because they operate on independent domains (primary website vs SEC EDGAR vs subsidiary sites). The supervisor is the single point of quality control.

## Design

### Architecture

**Workers are smart collectors. Supervisor is the editor.**

Workers are domain specialists — they deeply explore their assigned area using all discovery skills (sitemap analysis, path probing, site structure understanding, search). They bring back URLs with rich annotations (category, notes about the page content, scraper hints). They save broadly — anything potentially relevant.

The supervisor has the big picture. It plans the work, then reviews everything workers found across all areas. It filters noise, identifies gaps, and decides if more collection is needed. This split ensures no context is lost — individual workers understand their domain deeply, the supervisor understands the whole.

Three phases per discovery run:

1. **Plan** — Supervisor agent (Opus) reads the company profile, optionally uses tools to understand the landscape (max 20 tool calls for reconnaissance), outputs a structured `DiscoverPlan` with N task assignments.

2. **Execute** — N worker agents (configurable model) run in parallel via `asyncio.gather(return_exceptions=True)`. Each worker gets the full tool set and the full `DISCOVER_SYSTEM` prompt but with a narrowed focus. Workers save all candidate URLs with rich annotations to the `discovered_urls` DB table. Workers are encouraged to collect broadly — better to bring back too much than miss something.

3. **Review & Filter** — Supervisor reviews the full annotated URL list across all workers. **No tools during review** — it operates purely on the annotated URL list to keep costs controlled. Removes noise/duplicates, identifies coverage gaps. Can spawn additional workers for uncovered areas. Max 2 review rounds. The supervisor's filtering decisions are the final word on what stays.

### Structured Types

```python
class DiscoverTask(BaseModel):
    focus: str                  # "primary_website", "subsidiary_sites", "regulatory", etc.
    instructions: str           # specific guidance for this worker
    search_queries: list[str]   # starting queries to kick off with

class DiscoverPlan(BaseModel):
    tasks: list[DiscoverTask]   # empty when done=True
    done: bool = False          # True = supervisor is satisfied, stop discovering
    remove_urls: list[str] = [] # raw URLs to remove (review phase only, empty during plan)
```

Note: `remove_urls` contains **raw URLs** (not hashes). The implementation computes hashes via `url_hash()` before passing to the DB delete function. This is simpler for the LLM.

### Supervisor Structured Output

The supervisor uses pydantic-ai's `output_type=DiscoverPlan` for structured output. For Bedrock models, pydantic-ai handles this via tool-use extraction. For OpenAI models, it uses native structured outputs.

During the **plan phase**, the supervisor returns `DiscoverPlan(tasks=[...], done=False, remove_urls=[])`.
During the **review phase**, it returns either:
- `DiscoverPlan(done=True, tasks=[], remove_urls=[...])` — filter and finish
- `DiscoverPlan(done=False, tasks=[...], remove_urls=[...])` — filter and spawn more workers

### Supervisor Prompt

The supervisor gets the same company context doc + a new `DISCOVER_SUPERVISOR_PROMPT` that instructs it to:
- Read the company profile (scale, industry, subsidiaries, asset types, geographic footprint)
- Optionally use tools to understand the web landscape before planning (max 20 tool calls)
- Break discovery into parallel tasks appropriate for the company's complexity
- Each task should target a distinct domain/source area (no overlap between workers)
- Return structured `DiscoverPlan` output

**Plan phase:** Supervisor has access to all tools (web_search, fetch_sitemap, crawl_page, map_domain, probe_urls, save_urls, get_saved_urls) for initial reconnaissance. Max 20 tool calls. Tool calls count against the global budget.

**Review phase:** Supervisor has NO tools — operates purely on the annotated URL list. This keeps the supervisor focused on editorial decisions, not doing worker-level collection.

### Worker Prompt

Each worker gets the existing `DISCOVER_SYSTEM` prompt (unchanged — it teaches site structure analysis, path probing, store locator detection, annotation best practices) plus a preamble:

```
You are one of several parallel discovery agents. Your specific assignment:
Focus: {task.focus}
Instructions: {task.instructions}
Starting queries: {task.search_queries}

Only work on YOUR assigned focus area. Other agents are handling the rest.

Collect broadly — save any URL that might contain asset information. Annotate
each URL with category and detailed notes about what you found on the page
(e.g. "React SPA store locator, ~500 locations loaded via AJAX",
"SEC 10-K filing with property table in Item 2"). A supervisor will review
and filter your results, so err on the side of including too much rather
than missing something.
```

Workers have all the same tools as today. They use the full discovery skillset: sitemap fetching, path probing, crawling, site structure analysis, search. They annotate URLs with categories and rich context notes.

### Review Loop

After all workers complete, supervisor receives a user message with:

```
Discovery round {N} complete. Here is what was found:

Total URLs saved: {count}
By category:
- facility_page: 45
- regulatory_filing: 12
- store_locator: 3

Full URL list with annotations:
- [facility_page] [primary_website] https://example.com/locations — "Main locations page, lists 484 stores by state"
- [store_locator] [primary_website] https://example.com/store-finder — "React SPA, loads locations via API"
- [regulatory_filing] [regulatory] https://sec.gov/cgi-bin/... — "10-K filing with property table in Item 2"
...

Review these URLs. To remove noise, include the full URL in remove_urls.
Identify gaps — are there subsidiaries, regions, or asset types not yet covered?
If satisfied, set done=True. If more work needed, return additional tasks.
```

Each URL is annotated with `[category]`, `[worker_focus]`, and the worker's notes. This lets the supervisor see which worker found what and make better filtering decisions.

Supervisor returns either:
- `DiscoverPlan(done=True, remove_urls=[...])` — filter and finish
- `DiscoverPlan(tasks=[...], remove_urls=[...])` — filter and spawn more workers

Max 2 review rounds (configurable). A "round" = one batch of workers + one review. So max 3 total worker batches (initial + 2 review rounds).

### Concurrency Constraints

**Max workers per round:** 6 (configurable via `max_discover_workers`).

**API rate limiting:** A shared `ConcurrencyLimiter` (from `pydantic_ai`) is instantiated once and passed to all worker agents via `ConcurrencyLimitedModel`. This caps total concurrent API calls across all workers. Default limit: 10 (matching Exa free plan). Configurable via `max_concurrent_api_calls` in `[pipeline]`.

```python
from pydantic_ai import Agent
from pydantic_ai.models import ConcurrencyLimiter

limiter = ConcurrencyLimiter(max_concurrency=config.max_concurrent_api_calls)
worker_model = ConcurrencyLimitedModel(model, limiter)
```

**Tool call budget:** Supervisor plan phase gets max 20 tool calls. Remaining budget is split across workers: `(max_discover_tool_calls - supervisor_used) / N` per worker. This ensures total tool calls stay within the global budget.

**Worker timeout:** `max_discover_minutes / 2` per worker (e.g. 7.5 min each). Supervisor time (plan + review) counts against the overall `max_discover_minutes` budget.

**Global state:** `tools.init_tools(config, issuer_id, costs)` is called once before any agent starts. Workers share the same `_config`, `_issuer_id`, and `_costs` globals. This is safe because:
- All workers use the same `issuer_id` and `config` (read-only after init)
- `CostTracker` mutations (`+=`) are atomic in asyncio's single-threaded event loop (no `await` between read and write)
- DB writes use short-lived connections per call (existing pattern in `tools.py`), no shared connection

### Config

```toml
[models]
discover.supervisor = "bedrock/us.anthropic.claude-opus-4-6-v1"
discover.worker = "bedrock/us.anthropic.claude-sonnet-4-6"
```

**Resolution logic in `config.py`:**

```python
# In __post_init__, same pattern as profile_models (line 200):
discover_cfg = models.get("discover", {})
if isinstance(discover_cfg, dict):
    self.discover_supervisor_model = _resolve_str(
        "DISCOVER_SUPERVISOR_MODEL", discover_cfg, "supervisor", bedrock_default)
    self.discover_worker_model = _resolve_str(
        "DISCOVER_WORKER_MODEL", discover_cfg, "worker", sonnet)
else:
    # Backward compat: single string → both
    fallback = _resolve_str("DISCOVER_MODEL", models, "discover", bedrock_default)
    self.discover_supervisor_model = os.environ.get("DISCOVER_SUPERVISOR_MODEL") or fallback
    self.discover_worker_model = os.environ.get("DISCOVER_WORKER_MODEL") or fallback
```

- `DISCOVER_MODEL` env var sets BOTH supervisor and worker when new env vars are absent
- `DISCOVER_SUPERVISOR_MODEL` / `DISCOVER_WORKER_MODEL` override individually

New pipeline caps:
```toml
[pipeline]
max_discover_rounds = 2          # max review rounds (supervisor → workers → review)
max_discover_workers = 6         # max parallel workers per round
max_concurrent_api_calls = 10    # shared API concurrency limit (Exa free plan = 10)
```

### Error Handling

- `asyncio.gather(return_exceptions=True)` — one worker failing doesn't kill others
- Worker timeout/tool-call-limit: gracefully stops, URLs saved so far are kept
- Failed workers logged as warnings with focus label, supervisor sees partial results in review
- Supervisor timeout: falls back to whatever URLs were saved (same as current)
- Bedrock connection drops: pydantic-ai's built-in retry handles transient errors
- Supervisor time counts against the overall `max_discover_minutes` budget

### Cost Tracking

Both supervisor agent runs (plan + each review) and all worker agent runs need cost tracking via `costs.track_pydantic_ai(usage, model, stage)`. The stage label should include the agent type:
- Supervisor: `"discover_supervisor"`
- Workers: `"discover_worker:{focus}"`

### Data Flow

```
Company profile + context doc
        |
        v
  Supervisor (Opus) — plan phase [has tools, max 20 calls]
  - reads profile
  - optional reconnaissance (web_search, fetch_sitemap)
  - outputs DiscoverPlan with N tasks (max 6)
        |
        v
  asyncio.gather(                  [shared ConcurrencyLimiter]
    Worker[primary_website],       # deeply explores main site, annotates URLs
    Worker[subsidiary_sites],      # finds + explores subsidiary web presence
    Worker[regulatory_filings],    # searches SEC, EPA, etc.
    ...
  )
  - each uses full discovery skillset (sitemap, probe, crawl, search)
  - each saves URLs with rich annotations (category, notes, scraper hints)
  - each has tool_calls_limit = (remaining_budget)/N, timeout = time_budget/2
  - all write to discovered_urls table
        |
        v
  Supervisor — review & filter phase [NO tools]
  - sees full annotated URL list with worker labels
  - removes noise/duplicates (remove_urls with raw URLs)
  - identifies coverage gaps
  - satisfied? → done
  - gaps? → spawn more workers → repeat (max 2 rounds)
        |
        v
  Return filtered discovered_urls from DB
```

### Files Changed

| File | Change |
|------|--------|
| `stages/discover.py` | Rewrite: supervisor + worker logic, `asyncio.gather`, review loop, URL filtering |
| `stages/prompts.py` | Add `DISCOVER_SUPERVISOR_PROMPT`, `DISCOVER_WORKER_PREAMBLE`, `DISCOVER_REVIEW_TEMPLATE` |
| `config.py` | Add `discover_supervisor_model`, `discover_worker_model`, `max_discover_rounds`, `max_discover_workers`, `max_concurrent_api_calls` |
| `config.toml` | Add `discover.supervisor`, `discover.worker` keys, new pipeline caps |
| `db.py` | Add `delete_discovered_urls(conn, issuer_id, urls)` |

**`delete_discovered_urls` signature:**

```python
def delete_discovered_urls(conn: psycopg.Connection, issuer_id: str, urls: list[str]) -> int:
    """Delete discovered URLs by raw URL. Returns count deleted."""
    if not urls:
        return 0
    hashes = [url_hash(u) for u in urls]
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM discovered_urls WHERE issuer_id = %s AND url_hash = ANY(%s)",
            (issuer_id, hashes),
        )
    conn.commit()
    return cur.rowcount
```

### Verbose Output

With `--verbose`, shows:
- Supervisor planning phase tool calls and task assignments (focus + instructions for each)
- Each worker's tool calls prefixed with focus label (e.g. `[primary_website] [3] fetch_sitemap(...)`)
- Worker completion summaries (URLs found per worker, elapsed time)
- Review phase: URLs removed, gaps identified, additional workers spawned
- Cost per agent (supervisor plan, each worker, supervisor review)

### Known Limitations

- Scraper config fields (`proxy_mode`, `wait_for`, `js_code`, `scan_full_page`, `screenshot`) from worker annotations are not persisted to the `discovered_urls` DB table (pre-existing limitation — columns don't exist). These fields flow through in-memory within a single run but are lost on `--start-from` resume. Tracked as a separate issue.
