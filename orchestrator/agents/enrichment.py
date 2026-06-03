import json
import math
import os
import time
import structlog
from groq import AsyncGroq
from .base import BaseAgent
from ..connectors.registry import ConnectorRegistry

log = structlog.get_logger()

MAX_REACT_ITERS = 4

SYSTEM_PROMPT = """You are an elite triage assistant in a
strict ReAct loop. Find the most relevant troubleshooting
runbook or workaround for the reported bug.

Tools available:
1. Action: search_confluence
   Action Input: <2-3 word architectural concept query>
2. Final Answer: [{"title":..., "url":...,
   "excerpt":..., "relevance":"high|medium|low"}]

Rules:
- Think abstractly about the UNDERLYING engineering concept
  not the literal error text
  Example: "OOMKilled pod" → search "memory limit configuration"
  Example: "NullPointerException allocate" → search
           "concurrent allocation thread safety"
  Example: "CTE optimizer wrong results" → search
           "query optimizer correctness"
- Strip line numbers, hex addresses, thread names
- If first search returns nothing, try a broader term
- Maximum 4 search attempts
- Always provide Final Answer as JSON array even if empty

Your output MUST use this exact format:
Thought: <your reasoning>
Action: search_confluence
Action Input: <query>

OR when done:
Final Answer: [{"title": "...", "url": "...",
"excerpt": "...", "relevance": "high"}]"""


class EnrichmentAgent(BaseAgent):
    step_name = "enrichment"

    async def run(self, context: dict) -> dict:
        primary = context.get("primary_ticket") or {}
        title = primary.get("title", "")
        component = primary.get("component", "") or ""
        description = (primary.get("description") or "")[:400]
        error_excerpt = (primary.get("error_excerpt") or "")[:300]
        status = primary.get("status", "")

        # Use faster smaller model for enrichment extraction
        groq_api_key = os.getenv("GROQ_API_KEY", "")
        enrichment_model = "llama-3.1-8b-instant"

        kb_articles = []

        if not groq_api_key:
            context["kb_articles"] = []
            return context

        client = AsyncGroq(api_key=groq_api_key)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Find knowledge base articles for this bug:\n"
                    f"Title: {title}\n"
                    f"Component: {component}\n"
                    f"Status: {status}\n"
                    f"Description: {description}\n"
                    f"Error: {error_excerpt}\n\n"
                    f"Think about the underlying engineering concept "
                    f"and search for architectural patterns, "
                    f"known issues, or runbooks related to this failure."
                ),
            },
        ]

        for iteration in range(MAX_REACT_ITERS):
            try:
                resp = await client.chat.completions.create(
                    model=enrichment_model,
                    messages=messages,
                    temperature=0.0,
                    max_tokens=512,
                )
                reply = resp.choices[0].message.content or ""
                messages.append({"role": "assistant", "content": reply})

                if "Final Answer:" in reply:
                    answer_part = reply.split("Final Answer:")[-1].strip()
                    # Strip markdown code blocks if present
                    answer_part = answer_part.strip("```json").strip("```").strip()
                    try:
                        kb_articles = json.loads(answer_part)
                        if not isinstance(kb_articles, list):
                            kb_articles = []
                    except Exception:
                        kb_articles = []
                    break

                if ("Action: search_confluence" in reply
                        and "Action Input:" in reply):
                    query = (reply.split("Action Input:")[-1]
                             .strip().split("\n")[0].strip())
                    log.info("Enrichment searching Confluence", query=query,
                             iteration=iteration)
                    search_results = await self._search_confluence(query)

                    # If empty results, tell LLM to broaden search
                    if not search_results:
                        obs = ("No results found for that query. "
                               "Try a broader or different architectural "
                               "concept related to the failure.")
                    else:
                        obs = json.dumps(search_results)

                    messages.append({
                        "role": "user",
                        "content": f"Observation: {obs}",
                    })

            except Exception as e:
                log.warning("Enrichment iteration failed",
                            error=str(e), iteration=iteration)
                break

        context["kb_articles"] = kb_articles[:5]
        return context

    def _slice_and_score(self, article_text: str,
                          bug_text: str,
                          last_modified_epoch: float = 0) -> list[str]:
        """
        Split article into paragraphs, apply temporal decay,
        return top 3 high-signal chunks.
        """
        paragraphs = [
            p.strip()
            for p in article_text.split("\n\n")
            if len(p.strip()) > 30
        ]

        if not paragraphs:
            return [article_text[:500]]

        # Temporal decay: 15% weight drop per year of staleness
        current_time = time.time()
        if last_modified_epoch and last_modified_epoch > 0:
            delta_years = (
                (current_time - last_modified_epoch)
                / (365 * 24 * 3600))
            decay = math.exp(-0.15 * delta_years)
        else:
            decay = 0.9  # assume moderately recent

        bug_words = set(bug_text.lower().split())

        scored_chunks = []
        for chunk in paragraphs:
            chunk_words = set(chunk.lower().split())
            if not chunk_words:
                continue
            # Jaccard overlap as lexical score
            overlap = len(bug_words & chunk_words) / len(
                bug_words | chunk_words)
            adjusted = overlap * decay
            # Always keep chunks with explicit fix signals
            if ("workaround" in chunk.lower()
                    or "patch" in chunk.lower()
                    or "fix" in chunk.lower()
                    or adjusted >= 0.10):
                scored_chunks.append((adjusted, chunk))

        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        top = [c for _, c in scored_chunks[:3]]
        return top if top else [article_text[:500]]

    def _lexical_overlap(self, text_a: str, text_b: str) -> float:
        words_a = set(text_a.lower().split())
        words_b = set(text_b.lower().split())
        if not words_a or not words_b:
            return 0.0
        return len(words_a & words_b) / len(words_a | words_b)

    async def _search_confluence(self, query: str) -> list[dict]:
        try:
            confluence = await ConnectorRegistry.get_by_type("confluence")
            if confluence:
                results = await confluence.search(query, max_results=5)
                if results:
                    output = []
                    for t in results:
                        article_text = t.description or ""
                        chunks = self._slice_and_score(
                            article_text, t.title, 0)
                        excerpt = " ... ".join(chunks)[:400]
                        output.append({
                            "title": t.title,
                            "url": t.url,
                            "excerpt": excerpt,
                            "relevance": "medium",
                        })
                    return output
        except Exception as e:
            log.warning("Confluence search failed", error=str(e))

        # Return empty — let LLM decide to refine rather than fake results
        return []
