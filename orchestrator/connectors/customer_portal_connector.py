import re
from .base_connector import BaseConnector
from ..models.ticket import TicketData, ChangeEvent

# Realistic HPE customer case data
CUSTOMER_CASES = [
    {"id": "CASE-2891", "customer": "Acme Corp", "title": "StorageController NPE causing VM provisioning failures", "severity": "P0", "component": "StorageController", "keywords": ["StorageController", "NPE", "NullPointerException", "provisioning", "VM", "concurrent"], "impact": "3 production VMs unprovisioned. SLA breach imminent.", "status": "Open"},
    {"id": "CASE-2984", "customer": "GlobalTech", "title": "Kafka consumer rebalancing causing message loss", "severity": "P1", "component": "Kafka", "keywords": ["kafka", "consumer", "rebalancing", "message", "loss", "partition"], "impact": "1 incident reported. Data pipeline delays.", "status": "Open"},
    {"id": "CASE-3012", "customer": "HPE Internal", "title": "Spark SQL CTE optimizer producing wrong results", "severity": "P1", "component": "SQL", "keywords": ["spark", "SQL", "CTE", "optimizer", "NormalizeCTEIds", "InlineCTE", "query"], "impact": "Analytics pipeline producing incorrect aggregations.", "status": "In Progress"},
    {"id": "CASE-3089", "customer": "DataVault Inc", "title": "PySpark connect mode incompatibility with Spark 3.5", "severity": "P2", "component": "PySpark", "keywords": ["PySpark", "connect", "mode", "compatibility", "python", "driver"], "impact": "Migration to Spark 3.5 blocked for 2 teams.", "status": "Open"},
    {"id": "CASE-3145", "customer": "CloudFirst", "title": "Kubernetes pod OOMKilled during high memory workloads", "severity": "P1", "component": "Kubernetes", "keywords": ["kubernetes", "OOMKilled", "memory", "pod", "container", "limit", "heap"], "impact": "Batch jobs failing at 60% completion rate.", "status": "Open"},
    {"id": "CASE-3201", "customer": "FinanceHub", "title": "Firefox WebGL context lost on GPU-accelerated dashboards", "severity": "P2", "component": "WebGL", "keywords": ["firefox", "WebGL", "context", "GPU", "graphics", "canvas", "render"], "impact": "Dashboard rendering broken for finance team.", "status": "Assigned"},
    {"id": "CASE-3267", "customer": "StreamCo", "title": "Structured streaming DSv2 metadata corruption", "severity": "P1", "component": "Streaming", "keywords": ["structured", "streaming", "DSv2", "metadata", "corruption", "checkpoint"], "impact": "Streaming job recovery failing after restart.", "status": "Open"},
    {"id": "CASE-3301", "customer": "NetCore Ltd", "title": "Network fabric link-state oscillation under load", "severity": "P1", "component": "Network", "keywords": ["network", "fabric", "link", "state", "oscillation", "multicast", "traffic"], "impact": "Network instability affecting 12 services.", "status": "In Progress"},
    {"id": "CASE-3412", "customer": "Acme Corp", "title": "Hadoop HDFS DataNode disk write failures", "severity": "P2", "component": "HDFS", "keywords": ["hadoop", "HDFS", "DataNode", "disk", "write", "failure", "storage"], "impact": "Data ingestion pipeline degraded.", "status": "Open"},
    {"id": "CASE-3498", "customer": "TechGiant", "title": "Flink job manager high CPU under backpressure", "severity": "P2", "component": "Flink", "keywords": ["flink", "job", "manager", "CPU", "backpressure", "checkpoint", "latency"], "impact": "Real-time processing SLA at risk.", "status": "Assigned"},
    {"id": "CASE-3521", "customer": "GlobalTech", "title": "JIRA API rate limiting during bulk issue fetch", "severity": "P3", "component": "JIRA", "keywords": ["jira", "API", "rate", "limit", "bulk", "fetch", "throttle"], "impact": "Internal tooling slowdown. No customer impact.", "status": "Open"},
    {"id": "CASE-3598", "customer": "MegaCorp", "title": "VS Code extension crash on large TypeScript projects", "severity": "P2", "component": "VSCode", "keywords": ["vscode", "typescript", "extension", "crash", "memory", "language", "server"], "impact": "Developer productivity reduced.", "status": "Open"},
]


class CustomerPortalConnector(BaseConnector):

    def _score_case(self, case: dict, query: str) -> int:
        if not query:
            return 1
        query_words = set(re.findall(r'\w+', query.lower()))
        case_keywords = set(kw.lower() for kw in case.get("keywords", []))
        case_title_words = set(re.findall(r'\w+', case.get("title", "").lower()))
        all_case_words = case_keywords | case_title_words
        return len(query_words & all_case_words)

    async def search(self, query: str, max_results: int = 5, **kwargs) -> list[TicketData]:
        scored = []
        for case in CUSTOMER_CASES:
            score = self._score_case(case, query)
            if not query or score > 0:
                scored.append((score, case))

        scored.sort(key=lambda x: x[0], reverse=True)
        top_cases = [c for _, c in scored[:max_results]]

        return [
            TicketData(
                ticket_id=case["id"],
                title=case["title"],
                description=case["impact"],
                severity=case["severity"],
                status=case["status"],
                component=case["component"],
                assignee="",
                reporter=case["customer"],
                created_at="",
                updated_at="",
                source_id=self.source_id,
                system_type=self.system_type,
                url=f"https://support.hpe.com/cases/{case['id']}",
            )
            for case in top_cases
        ]

    async def get(self, ticket_id: str) -> TicketData | None:
        for case in CUSTOMER_CASES:
            if case["id"] == ticket_id:
                return TicketData(
                    ticket_id=case["id"],
                    title=case["title"],
                    description=case["impact"],
                    severity=case["severity"],
                    status=case["status"],
                    component=case["component"],
                    assignee="",
                    reporter=case["customer"],
                    created_at="",
                    updated_at="",
                    source_id=self.source_id,
                    system_type=self.system_type,
                    url=f"https://support.hpe.com/cases/{case['id']}",
                )
        return None

    async def get_linked_items(self, ticket_id: str) -> list[dict]:
        return []

    async def get_changelog(self, ticket_id: str, since: str = "") -> list[ChangeEvent]:
        return []

    async def get_lightweight(self, ticket_id: str) -> dict:
        return {}

    def extract_links(self, raw_payload: dict) -> list[dict]:
        return []
