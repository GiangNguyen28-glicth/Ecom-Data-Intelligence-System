from abc import ABC

from common.constants import REPORT_TABLE
from model.job import Job
from repositories.base_repository import BaseRepository


class JobRepository(BaseRepository[Job]):
    @property
    def table(self) -> str:
        return REPORT_TABLE["JOBS"]

    @property
    def model(self) -> type[Job]:
        return Job
