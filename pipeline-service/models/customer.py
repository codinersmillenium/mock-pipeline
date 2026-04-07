from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import Column, String, Text, Date, DECIMAL, TIMESTAMP, Index
from database import Base


class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = (
        Index("idx_customer_email", "email", unique=True),
        {"comment": "Customer master table"},
    )

    customer_id:     str                = Column(String(50),   primary_key=True)
    first_name:      str                = Column(String(100),  nullable=False)
    last_name:       str                = Column(String(100),  nullable=False)
    email:           str                = Column(String(255),  nullable=False, unique=True)
    phone:           Optional[str]      = Column(String(20))
    address:         Optional[str]      = Column(Text)
    date_of_birth:   Optional[date]     = Column(Date)                  # fix: was str | None
    account_balance: Optional[Decimal]  = Column(DECIMAL(15, 2))        # fix: was float | None
    created_at:      Optional[datetime] = Column(TIMESTAMP(timezone=True))  # fix: was str | None

    def __repr__(self) -> str:
        return (
            f"<Customer(customer_id={self.customer_id!r}, "
            f"name={self.first_name} {self.last_name}, "
            f"email={self.email!r})>"
        )