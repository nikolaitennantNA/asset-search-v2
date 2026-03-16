# Parallel Multi-Agent Discovery

## Goal

Replace the single sequential discover agent with a supervisor + parallel worker pattern to reduce wall-clock time and eliminate Bedrock timeouts.

## Current State

`stages/discover.py` runs one pydantic-ai Agent (currently Sonnet/GPT-5-nano) that sequentially: reads the company profile, searches the web, fetches sitemaps, probes URLs, and saves results. Each tool call blocks the next. A complex company can take 10+ minutes and hit Bedrock connection timeouts.

## Design

### Architecture

Three phases per discovery run:

1. **Plan** — Supervisor agent (Opus) reads the company profile, optionally uses tools (web_search, fetch_sitemap, etc.) to understand the landscape, then outputs a structured `DiscoverPlan` with N task assignments.

2. **Execute** — N worker agents (configurable model) run in parallel via `asyncio.gather(return_exceptions=True)`. Each worker gets its own tool instances but a narrowed focus from its task assignment. All workers write to the same `discovered_urls` DB table (dedup via `ON CONFLICT`).

3. **Review** — Supervisor reviews what workers found (via `get_saved_urls`), decides if more workers are needed for gaps. Can spawn additional workers. Max 2 review rounds.

### Structured Types

```python
class DiscoverTask(BaseModel):
    focus: str                  # "primary_website", "subsidiary_sites", "regulatory", etc.
    instructions: str           # specific guidance for this worker
    search_queries: list[str]   # starting queries to kick off with

class DiscoverPlan(BaseModel):
    tasks: list[DiscoverTask]
    done: bool = False          # True = supervisor is satisfied, stop discovering
```

### Supervisor Structured Output

The supervisor uses pydantic-ai's `output_type=DiscoverPlan` for structured output. For Bedrock models, pydantic-ai handles this via tool-use extraction. For OpenAI models, it uses native structured outputs.

During the **plan phase**, the supervisor returns `DiscoverPlan(tasks=[...], done=False)`.
During the **review phase**, it returns either `DiscoverPlan(done=True, tasks=[])` or `DiscoverPlan(done=False, tasks=[...])` with additional tasks.

### Supervisor Prompt

The supervisor gets the same company context doc + a new `DISCOVER_SUPERVISOR_PROMPT` that instructs it to:
- Read the company profile (scale, industry, subsidiaries, asset types, geographic footprint)
- Optionally use tools to understand the web landscape
- Break discovery into parallel tasks appropriate for the company's complexity
- Return structured `DiscoverPlan` output

The supervisor has access to all tools (web_search, fetch_sitemap, crawl_page, map_domain, probe_urls, save_urls, get_saved_urls) so it can do initial reconnaissance before planning.

### Worker Prompt

Each worker gets the existing `DISCOVER_SYSTEM` prompt (unchanged) plus a preamble:

```
You are one of several parallel discovery agents. Your specific assignment:
Focus: {task.focus}
Instructions: {task.instructions}
Starting queries: {task.search_queries}

Only work on YOUR assigned focus area. Other agents are handling the rest.
```

Workers have all the same tools as today.

### Review Loop

After all workers complete, supervisor receives a user message with:

```
Discovery round {N} complete. Here is what was found:

Total URLs saved: {count}
By category:
- facility_page: 45
- regulatory_filing: 12
- store_locator: 3

Full URL list:
{url_list}

Are there gaps that need additional workers? If satisfied, set done=True.
If more work needed, return additional tasks.
```

Supervisor returns either:
- `DiscoverPlan(done=True)` — discovery complete
- `DiscoverPlan(tasks=[...])` — additional workers spawned for another round

Max 2 review rounds to prevent runaway costs.

### Concurrency Constraints

**Max workers per round:** 6. Supervisor can create up to 6 tasks per round. Bounds API rate limits and cost.

**Tool call budget per worker:** `max_discover_tool_calls / max_workers` (e.g. 200 / 6 ≈ 33 per worker). Ensures total tool calls across all workers stays within the global budget.

**Worker timeout:** `max_discover_minutes / 2` per worker (e.g. 7.5 min each), so a full round completes within the overall time budget.

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
- `models.discover` can be a string (backward compat) or a dict with `supervisor`/`worker` keys
- If `models.discover` is a string → use it for both supervisor and worker
- If `models.discover` is a dict → read `.supervisor` and `.worker` separately
- Env overrides: `DISCOVER_SUPERVISOR_MODEL`, `DISCOVER_WORKER_MODEL`
- New fields on Config: `discover_supervisor_model`, `discover_worker_model`

New pipeline caps:
```toml
[pipeline]
max_discover_rounds = 2    # max review iterations (supervisor → workers → review)
max_discover_workers = 6   # max parallel workers per round
```

### Error Handling

- `asyncio.gather(return_exceptions=True)` — one worker failing doesn't kill others
- Worker timeout/tool-call-limit: gracefully stops, URLs saved so far are kept
- Failed workers logged as warnings, supervisor sees partial results in review
- Supervisor timeout: falls back to whatever URLs were saved (same as current)
- Bedrock connection drops: pydantic-ai's built-in retry handles transient errors

### Data Flow

```
Company profile + context doc
        |
        v
  Supervisor (Opus) — plan phase
  - reads profile
  - optional tool calls (web_search, fetch_sitemap)
  - outputs DiscoverPlan with N tasks (max 6)
        |
        v
  asyncio.gather(
    Worker[primary_website],
    Worker[subsidiary_sites],
    Worker[regulatory_filings],
    ...
  )
  - each has tool_calls_limit = budget/N, timeout = time_budget/2
  - all write to discovered_urls table
        |
        v
  Supervisor — review phase
  - get_saved_urls() → summary of what was found
  - satisfied? → done
  - gaps? → spawn more workers → repeat (max 2 rounds)
        |
        v
  Return discovered_urls from DB
```

### Files Changed

| File | Change |
|------|--------|
| `stages/discover.py` | Rewrite: supervisor + worker logic, `asyncio.gather`, review loop |
| `stages/prompts.py` | Add `DISCOVER_SUPERVISOR_PROMPT`, `DISCOVER_WORKER_PREAMBLE`, `DISCOVER_REVIEW_TEMPLATE` |
| `config.py` | Add `discover_supervisor_model`, `discover_worker_model`, `max_discover_rounds`, `max_discover_workers` resolution |
| `config.toml` | Add `discover.supervisor`, `discover.worker` keys, new pipeline caps |

### Verbose Output

With `--verbose`, shows:
- Supervisor planning phase and task assignments (focus + instructions for each)
- Each worker's tool calls prefixed with focus label (e.g. `[primary_website] [3] fetch_sitemap(...)`)
- Worker completion summaries (URLs found, elapsed time)
- Review loop decisions (done vs. spawning more workers)
