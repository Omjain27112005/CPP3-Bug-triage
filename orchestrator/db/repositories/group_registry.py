from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from ..models import SystemGroupRegistry, BugGroupMapping


async def get_next_group_id(db: AsyncSession) -> str:
    result = await db.execute(
        select(func.count(SystemGroupRegistry.group_id))
    )
    count = result.scalar() or 0
    return f"BT-{(count + 1):03d}"


async def create_group(db: AsyncSession, group_id: str, title: str,
                        priority: str, primary_source_id: str) -> SystemGroupRegistry:
    group = SystemGroupRegistry(
        group_id=group_id,
        title=title,
        priority=priority,
        primary_source_id=primary_source_id,
        status="active",
    )
    db.add(group)
    await db.commit()
    await db.refresh(group)
    return group


async def get_group_for_ticket(db: AsyncSession, raw_ticket_id: str,
                                source_id: str) -> str | None:
    result = await db.execute(
        select(BugGroupMapping.group_id)
        .where(
            BugGroupMapping.raw_ticket_id == raw_ticket_id,
            BugGroupMapping.source_id == source_id,
        )
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return row


async def get_group_for_any_ticket(db: AsyncSession,
                                    tickets: list[dict]) -> str | None:
    for t in tickets:
        gid = await get_group_for_ticket(
            db,
            t.get("ticket_id", ""),
            t.get("source_id", "")
        )
        if gid:
            return gid
    return None


async def add_tickets_to_group(db: AsyncSession, group_id: str,
                                tickets: list[dict]) -> None:
    for t in tickets:
        existing = await get_group_for_ticket(
            db,
            t.get("ticket_id", ""),
            t.get("source_id", "")
        )
        if not existing:
            mapping = BugGroupMapping(
                group_id=group_id,
                raw_ticket_id=t.get("ticket_id", ""),
                source_id=t.get("source_id", ""),
                system_type=t.get("system_type", ""),
            )
            db.add(mapping)
    await db.commit()


async def get_tickets_in_group(db: AsyncSession,
                                group_id: str) -> list[BugGroupMapping]:
    result = await db.execute(
        select(BugGroupMapping)
        .where(BugGroupMapping.group_id == group_id)
    )
    return list(result.scalars().all())
