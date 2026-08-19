"""Market DAG task modules."""

from __future__ import annotations

from abc import ABC, abstractmethod


class Task(ABC):
    """Airflow에서 호출할 task 로직 클래스의 공통 베이스."""

    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    def run(self, **context: object) -> int:
        """Task body."""