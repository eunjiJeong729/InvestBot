"""DW migration task base class."""

from __future__ import annotations

from abc import ABC, abstractmethod


class Task(ABC):
    def __init__(self, name: str) -> None:
        self.name = name

    @abstractmethod
    def run(self, **context: object) -> int:
        """Task body."""
