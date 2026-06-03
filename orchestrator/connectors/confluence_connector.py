import re
import base64
import os
import httpx
import structlog
from .base_connector import BaseConnector
from ..models.ticket import TicketData, ChangeEvent

log = structlog.get_logger()


class ConfluenceConnector(BaseConnector):

    def _headers(self) -> dict:
        """
        Build auth headers.
        - If email + token: Basic Auth (Atlassian Cloud)
        - If token only: Bearer Auth (PAT / server)
        - If neither: no auth header (public wiki)
        """
        headers = {"Accept": "application/json",
                   "Content-Type": "application/json"}
        email = os.getenv("CONFLUENCE_EMAIL", "").strip()
        token = (self.token or "").strip()

        if email and token:
            creds = base64.b64encode(
                f"{email}:{token}".encode()).decode()
            headers["Authorization"] = f"Basic {creds}"
        elif token:
            headers["Authorization"] = f"Bearer {token}"
        # else: public wiki — no Authorization header
        return headers

    def _strip_html(self, text: str) -> str:
        """
        Remove HTML/XHTML tags, decode entities,
        normalize whitespace, cap at 2000 chars.
        """
        # Remove HTML tags (non-greedy)
        clean = re.sub(r'<[^>]+?>', ' ', text or '')
        # Decode common HTML entities
        clean = re.sub(r'&[a-zA-Z0-9#]+;', ' ', clean)
        # Normalize whitespace
        clean = re.sub(r'[\s\t\n\r]+', ' ', clean).strip()
        return clean[:2000]

    def _build_url(self, webui_path: str) -> str:
        """Build absolute browser URL from relative webui path."""
        base = self.base_url.rstrip("/")
        # Public wikis include /confluence in base_url already
        if webui_path.startswith("/wiki"):
            return f"{base}{webui_path}"
        elif "/confluence" in base:
            return f"{base}/wiki{webui_path}"
        else:
            return f"{base}/wiki{webui_path}"

    async def search(self, query: str,
                     max_results: int = 5) -> list[TicketData]:
        space = (self.project_key or "HPEKB").strip()

        if query and query.strip():
            # Escape quotes in query for CQL safety
            safe_query = query.replace('"', '\\"')
            cql = (f'space = "{space}" AND '
                   f'text ~ "{safe_query}" AND type = page '
                   f'ORDER BY lastModified DESC')
        else:
            cql = (f'space = "{space}" AND type = page '
                   f'ORDER BY lastModified DESC')

        url = f"{self.base_url.rstrip('/')}/rest/api/content/search"
        params = {
            "cql": cql,
            "limit": min(max_results, 10),
            "expand": "body.storage,version",
        }

        try:
            async with httpx.AsyncClient(
                    timeout=20,
                    follow_redirects=True) as client:
                resp = await client.get(
                    url,
                    headers=self._headers(),
                    params=params)

                if resp.status_code == 401:
                    log.warning("Confluence auth failed",
                                source=self.source_id,
                                url=url)
                    return []
                if resp.status_code != 200:
                    log.warning("Confluence search failed",
                                status=resp.status_code,
                                source=self.source_id,
                                query=query)
                    return []

                results = resp.json().get("results") or []
                tickets = []

                for r in results:
                    try:
                        body_raw = (r.get("body", {})
                                      .get("storage", {})
                                      .get("value", ""))
                        description = self._strip_html(body_raw)
                        version = r.get("version") or {}
                        when = version.get("when", "")
                        webui = ((r.get("_links") or {})
                                   .get("webui", ""))
                        full_url = self._build_url(webui)

                        tickets.append(TicketData(
                            ticket_id=str(r.get("id", "")),
                            title=r.get("title", ""),
                            description=description,
                            severity="Unknown",
                            status="Published",
                            component=space,
                            assignee="",
                            reporter="",
                            created_at="",
                            updated_at=when,
                            source_id=self.source_id,
                            system_type=self.system_type,
                            url=full_url,
                        ))
                    except Exception as e:
                        log.warning("Confluence result parse error",
                                    error=str(e))
                        continue

                log.info("Confluence search complete",
                         source=self.source_id,
                         space=space,
                         query=query,
                         count=len(tickets))
                return tickets

        except httpx.TimeoutException:
            log.warning("Confluence search timeout",
                        source=self.source_id, query=query)
            return []
        except Exception as e:
            log.warning("Confluence search error",
                        source=self.source_id, error=str(e))
            return []

    async def get(self, article_id: str) -> TicketData | None:
        url = (f"{self.base_url.rstrip('/')}"
               f"/rest/api/content/{article_id}"
               f"?expand=body.storage,version")
        try:
            async with httpx.AsyncClient(
                    timeout=15,
                    follow_redirects=True) as client:
                resp = await client.get(
                    url, headers=self._headers())
                if resp.status_code != 200:
                    return None
                r = resp.json()
                body_raw = (r.get("body", {})
                              .get("storage", {})
                              .get("value", ""))
                webui = ((r.get("_links") or {})
                           .get("webui", ""))
                return TicketData(
                    ticket_id=str(r.get("id", "")),
                    title=r.get("title", ""),
                    description=self._strip_html(body_raw),
                    severity="Unknown",
                    status="Published",
                    component=self.project_key or "",
                    assignee="",
                    reporter="",
                    created_at="",
                    updated_at="",
                    source_id=self.source_id,
                    system_type=self.system_type,
                    url=self._build_url(webui),
                )
        except Exception as e:
            log.warning("Confluence get error",
                        article_id=article_id, error=str(e))
            return None

    async def get_linked_items(self, ticket_id: str) -> list:
        return []

    async def get_changelog(self, ticket_id: str,
                             since: str = "") -> list[ChangeEvent]:
        return []

    async def get_lightweight(self, ticket_id: str) -> dict:
        return {}

    def extract_links(self, raw_payload: dict) -> list[dict]:
        return []
