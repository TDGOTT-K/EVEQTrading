from __future__ import annotations

from typing import Iterable, Sequence, TypeVar

T = TypeVar("T")


def chunked(items: Sequence[T], chunk_size: int) -> list[list[T]]:
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero.")
    return [list(items[index : index + chunk_size]) for index in range(0, len(items), chunk_size)]
