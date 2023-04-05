from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional, Set


class OutOfStock(Exception):
    pass


class Product:
    def __init__(self, sku: str, batches: List[Batch], version_number: int = 0):
        self.sku = sku
        self.batches = batches
        self.version_number = version_number

    def allocate(self, line: OrderLine) -> str:
        try:
            batch = next(b for b in sorted(self.batches) if b.can_allocate(line))
            batch.allocate(line)
            self.version_number += 1
            return batch.reference
        except StopIteration:
            raise OutOfStock(f"Out of stock for sku {line.sku}")


# https://github.com/cosmicpython/code/issues/17
# @dataclass(frozen=True)
@dataclass(unsafe_hash=True)
class OrderLine:
    orderid: str
    sku: str
    qty: int


@dataclass
class Batch:
    reference: str
    sku: str
    qty: int
    eta: Optional[date]
    purchased_quantity: int = field(init=False)
    allocations: Set[OrderLine] = field(default_factory=set)

    def __post_init__(self):
        self.purchased_quantity = self.qty

    def __repr__(self):
        return f"<Batch {self.reference}>"

    def __eq__(self, other):
        if not isinstance(other, Batch):
            return False
        return other.reference == self.reference

    def __gt__(self, other):
        if self.eta is None:
            return False
        if other.eta is None:
            return True
        return self.eta > other.eta

    def __hash__(self):
        return hash(self.reference)

    def allocate(self, line: OrderLine):
        if self.can_allocate(line):
            self.allocations.add(line)

    def deallocate(self, line: OrderLine):
        if line in self.allocations:
            self.allocations.remove(line)

    @property
    def allocated_quantity(self) -> int:
        return sum(line.qty for line in self.allocations)

    @property
    def available_quantity(self) -> int:
        return self.purchased_quantity - self.allocated_quantity

    def can_allocate(self, line: OrderLine) -> bool:
        return self.sku == line.sku and self.available_quantity >= line.qty
