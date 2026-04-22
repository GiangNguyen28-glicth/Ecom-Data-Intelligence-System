from typing import Optional

from psycopg2.extras import Json
from dataclasses import dataclass


@dataclass
class Job:
    id: str
    name: str
    run_id: str
    status: str
    start_time: str
    process_at: str
    metadata: Json

@dataclass
class JobUpdate:
    id: str
    name: Optional[str] = None
    run_id: Optional[str] = None
    status: Optional[str] = None
    start_time: Optional[str] = None
    process_at: Optional[str] = None
    metadata: Optional[Json] = None