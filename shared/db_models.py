"""SQLAlchemy ORM models and DB session utilities for the platform."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Generator, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from shared.constants import DATABASE_URL


class Base(DeclarativeBase):
    """Base class for all ORM models."""


engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    class_=Session,
)


class Ticker(Base):
    __tablename__ = "tickers"
    __table_args__ = (Index("idx_tickers_symbol", "symbol"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    company_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    index_name: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    is_active: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        nullable=True,
        server_default=text("TRUE"),
    )
    last_synced: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        server_default=func.now(),
    )


class StockPrice(Base):
    __tablename__ = "stock_prices"
    __table_args__ = (
        UniqueConstraint("ticker", "timestamp", name="uq_stock_prices_ticker_timestamp"),
        Index("idx_stock_prices_ticker_timestamp", "ticker", "timestamp"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    close: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    volume: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    exchange: Mapped[Optional[str]] = mapped_column(
        String(5),
        nullable=True,
        server_default=text("'NSE'"),
    )


class Anomaly(Base):
    __tablename__ = "anomalies"
    __table_args__ = (Index("idx_anomalies_ticker_timestamp", "ticker", "timestamp"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    close: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    zscore: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4), nullable=True)
    anomaly_type: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    alert_sent: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        nullable=True,
        server_default=text("FALSE"),
    )
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        server_default=func.now(),
    )


class Forecast(Base):
    __tablename__ = "forecasts"
    __table_args__ = (
        UniqueConstraint("ticker", "forecast_date", name="uq_forecasts_ticker_forecast_date"),
        Index("idx_forecasts_ticker_forecast_date", "ticker", "forecast_date"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False)
    forecast_date: Mapped[date] = mapped_column(Date, nullable=False)
    predicted_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    lower_bound: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    upper_bound: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 4), nullable=True)
    model_version: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        server_default=func.now(),
    )


def get_db() -> Generator[Session, None, None]:
    """Yield DB sessions for dependency injection usage."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_all_tables() -> None:
    """Create all tables mapped by ORM models."""
    Base.metadata.create_all(bind=engine)
