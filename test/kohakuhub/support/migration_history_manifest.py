"""Release checkpoints used by the real migration-history matrix."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HistoricalRelease:
    """A released application schema checkpoint and its migration boundary."""

    slug: str
    label: str
    commit: str
    migration_numbers: tuple[int, ...]


HISTORICAL_RELEASES = (
    # The first four migration scripts were introduced together with the
    # same released application schema. Keep one explicit case per number so
    # the compatibility matrix still proves every numbered boundary.
    HistoricalRelease("v001", "001", "ffd17d775d3bbc9e27984d30739ff6567635c2cf", (1,)),
    HistoricalRelease("v002", "002", "ffd17d775d3bbc9e27984d30739ff6567635c2cf", (2,)),
    HistoricalRelease("v003", "003", "ffd17d775d3bbc9e27984d30739ff6567635c2cf", (3,)),
    HistoricalRelease("v004", "004", "ffd17d775d3bbc9e27984d30739ff6567635c2cf", (4,)),
    HistoricalRelease("v005", "005", "4ab73d9a548c81d1033364fbc0f84e5cdebfe631", (5,)),
    HistoricalRelease("v006", "006", "4ab73d9a548c81d1033364fbc0f84e5cdebfe631", (6,)),
    HistoricalRelease("v007", "007", "970fed320a53c0afa452e5d3c4a22dffe57f6911", (7,)),
    HistoricalRelease("v008", "008", "c48997824f6f5f38c635d2d16038cd611da08348", (8,)),
    HistoricalRelease("v009", "009", "77c6cb7c75ed7dfafa4897deb48e718f39725721", (9,)),
    HistoricalRelease("v010", "010", "266e1074f513bae16adeee08cce5e4f4f5e7dc36", (10,)),
    HistoricalRelease("v011", "011", "1224aa5b64430fdc5de8cd5b402ba72c3ece5cd8", (11,)),
    HistoricalRelease("v012", "012", "7ccf28344489788ead4e7b9a83a178e30e22070c", (12,)),
    HistoricalRelease("v013", "013", "bede383535791f541a48bb07d7ee30e1471a4094", (13,)),
    HistoricalRelease("v014", "014", "140cb937ae6297e48be8e980e1c684a17d7370d1", (14,)),
    HistoricalRelease("v015", "015", "dd0f5a8021889f928b20d6fd4d97d7bcd1cff218", (15,)),
    HistoricalRelease("v016", "016", "f9e072707fd632a61b8637e3234567c9144260b9", (16,)),
)

MAIN_RELEASE = HistoricalRelease(
    "main", "main", "4b67eaa537d023820242a8306baf3f519322448a", ()
)


def migration_history_releases() -> tuple[HistoricalRelease, ...]:
    """Return all historical numbered checkpoints followed by ``main``."""

    return HISTORICAL_RELEASES + (MAIN_RELEASE,)


def covered_migration_numbers() -> set[int]:
    """Return the numbered historical migrations represented by the matrix."""

    return {
        number
        for release in HISTORICAL_RELEASES
        for number in release.migration_numbers
    }
