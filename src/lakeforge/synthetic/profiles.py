"""Fixed generation profiles for the operational source."""

from dataclasses import dataclass
from datetime import UTC, datetime

from lakeforge.domain.models import Site

PROFILE_NAMES = ("tiny", "dev", "large")

SITES: tuple[Site, ...] = (
    Site(
        site_id="SITE-ALPHA",
        site_name="Site Alpha",
        latitude=45.0,
        longitude=-90.0,
        timezone_name="UTC",
    ),
    Site(
        site_id="SITE-BRAVO",
        site_name="Site Bravo",
        latitude=45.05,
        longitude=-89.95,
        timezone_name="UTC",
    ),
)

ASSET_CATALOG: tuple[tuple[str, str], ...] = (
    ("haul_truck", "HT-240"),
    ("excavator", "EX-90"),
    ("wheel_loader", "WL-160"),
    ("dozer", "DZ-120"),
)


@dataclass(frozen=True, slots=True)
class GenerationProfile:
    """Resolved size and clock for one generation run."""

    name: str
    vehicle_count: int
    window_start: datetime
    hours: int
    interval_minutes: int

    @property
    def steps(self) -> int:
        return (self.hours * 60) // self.interval_minutes


_PROFILES: dict[str, GenerationProfile] = {
    "tiny": GenerationProfile(
        name="tiny",
        vehicle_count=4,
        window_start=datetime(2026, 1, 12, 6, 0, tzinfo=UTC),
        hours=6,
        interval_minutes=15,
    ),
    "dev": GenerationProfile(
        name="dev",
        vehicle_count=8,
        window_start=datetime(2026, 1, 12, 0, 0, tzinfo=UTC),
        hours=24,
        interval_minutes=15,
    ),
    "large": GenerationProfile(
        name="large",
        vehicle_count=20,
        window_start=datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
        hours=7 * 24,
        interval_minutes=15,
    ),
}


def resolve_profile(
    name: str,
    *,
    vehicle_count: int | None,
    hours: int | None,
    interval_minutes: int | None,
) -> GenerationProfile:
    """Apply explicit size overrides on top of a named profile."""
    try:
        base = _PROFILES[name]
    except KeyError as exc:
        raise ValueError(f"unknown profile: {name}") from exc
    resolved_vehicles = base.vehicle_count if vehicle_count is None else vehicle_count
    resolved_hours = base.hours if hours is None else hours
    resolved_interval = base.interval_minutes if interval_minutes is None else interval_minutes
    if resolved_vehicles < 1:
        raise ValueError("vehicle_count must be at least 1")
    if resolved_hours < 1:
        raise ValueError("hours must be at least 1")
    if resolved_interval < 1:
        raise ValueError("interval_minutes must be at least 1")
    if (resolved_hours * 60) // resolved_interval < 1:
        raise ValueError("interval_minutes must fit at least once inside the generation window")
    return GenerationProfile(
        name=base.name,
        vehicle_count=resolved_vehicles,
        window_start=base.window_start,
        hours=resolved_hours,
        interval_minutes=resolved_interval,
    )
