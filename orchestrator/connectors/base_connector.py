from abc import ABC, abstractmethod
from ..models.ticket import TicketData, ChangeEvent


class BaseConnector(ABC):
    def __init__(self, source_id: str, system_type: str, base_url: str,
                 project_key: str, ticket_prefix: str, token: str = ""):
        self.source_id = source_id
        self.system_type = system_type
        self.base_url = base_url.rstrip("/")
        self.project_key = project_key
        self.ticket_prefix = ticket_prefix
        self.token = token

    @abstractmethod
    async def get(self, ticket_id: str) -> TicketData | None:
        pass

    @abstractmethod
    async def search(self, query: str, max_results: int = 10) -> list[TicketData]:
        pass

    @abstractmethod
    async def get_linked_items(self, ticket_id: str) -> list[dict]:
        pass

    @abstractmethod
    async def get_changelog(self, ticket_id: str, since: str = "") -> list[ChangeEvent]:
        pass

    @abstractmethod
    async def get_lightweight(self, ticket_id: str) -> dict:
        """Fetch only updated_at, severity, status. Returns {} on failure."""

    @abstractmethod
    def extract_links(self, raw_payload: dict) -> list[dict]:
        """
        Parse raw API payload and return explicit outbound references.
        Each reference is a dict with keys:
          raw_id, source, relationship, url (optional)
        Returns empty list if not supported.
        """
