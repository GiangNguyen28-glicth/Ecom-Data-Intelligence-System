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
