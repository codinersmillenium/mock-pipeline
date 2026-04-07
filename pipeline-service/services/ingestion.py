import hashlib
import logging
import asyncio
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Iterator, Dict, List, Optional, Any, Set

import dlt
import httpx

from models.customer import Customer
from database import DATABASE_URL

# --- CONFIGURATION ---
MOCK_SERVER_URL: str  = "http://mock-server:5000"
FETCH_PAGE_SIZE: int  = 100  # High-throughput batch size
REQUEST_TIMEOUT: int  = 10
CONCURRENT_PAGES: int = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- THREAD-SAFE CACHE ---
_CUSTOMERS_CACHE: List[Dict[str, Any]] = []
_CACHE_ETAG: Optional[str] = None
_CACHE_LOCK: asyncio.Lock = asyncio.Lock()


# --- HELPER FUNCTIONS (Optimized & Preserved) ---

def _safe_parse_decimal(value: Optional[object]) -> Optional[Decimal]:
    """Converts input to Decimal safely; returns None if invalid."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        logger.warning(f"Invalid decimal value: {value}")
        return None

def _safe_parse_date(value: Optional[object]) -> Optional[date]:
    """Parses date strings into date objects."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:
        logger.warning(f"Failed to parse date: {value}")
        return None

def _safe_parse_datetime(value: Optional[object]) -> Optional[datetime]:
    """Parses ISO strings to datetime with UTC normalization."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        # Normalize 'Z' to '+00:00' for fromisoformat compatibility
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        logger.warning(f"Failed to parse datetime: {value}")
        return None

def _safe_transform(row: Dict[str, Any]) -> Optional[Customer]:
    """Maps raw API dictionary to Customer model."""
    try:
        return Customer(
            customer_id=str(row.get("customer_id")),
            first_name=row.get("first_name"),
            last_name=row.get("last_name"),
            email=row.get("email"),
            phone=row.get("phone"),
            address=row.get("address"),
            date_of_birth=_safe_parse_date(row.get("date_of_birth")),
            account_balance=_safe_parse_decimal(row.get("account_balance")),
            created_at=_safe_parse_datetime(row.get("created_at"))
        )
    except Exception as e:
        logger.warning(f"Failed to transform row {row.get('customer_id')}: {e}")
        return None

def _to_dict(c: Customer) -> Dict[str, Any]:
    """Converts Customer object back to dictionary for DLT resources."""
    return {
        "customer_id":     c.customer_id,
        "first_name":      c.first_name,
        "last_name":       c.last_name,
        "email":           c.email,
        "phone":           c.phone,
        "address":         c.address,
        "date_of_birth":   c.date_of_birth,
        "account_balance": c.account_balance,
        "created_at":      c.created_at,
    }


# --- ASYNC FETCH FUNCTIONS ---

async def _fetch_source_fingerprint(client: httpx.AsyncClient) -> Optional[str]:
    """Determines data versioning using ETag or MD5 hash of the first page."""
    try:
        # Attempt lightweight HEAD request first
        head = await client.head(
            f"{MOCK_SERVER_URL}/api/customers",
            params={"page": 1, "limit": FETCH_PAGE_SIZE},
            timeout=REQUEST_TIMEOUT,
        )
        if etag := head.headers.get("etag"):
            return etag
        if last_mod := head.headers.get("last-modified"):
            return last_mod
    except Exception:
        pass
    
    try:
        # Fallback to content hashing
        resp = await client.get(
            f"{MOCK_SERVER_URL}/api/customers",
            params={"page": 1, "limit": FETCH_PAGE_SIZE},
            timeout=REQUEST_TIMEOUT,
        )
        return hashlib.md5(resp.content).hexdigest()
    except Exception as e:
        logger.warning(f"Fingerprint acquisition failed: {e}")
        return None

async def _fetch_page_async(page: int, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Fetches a single paginated result."""
    try:
        response = await client.get(
            f"{MOCK_SERVER_URL}/api/customers",
            params={"page": page, "limit": FETCH_PAGE_SIZE},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        return data.get("data", []) if isinstance(data, dict) else data
    except Exception as e:
        logger.error(f"Error on page {page}: {e}")
        return []

async def _fetch_all_pages_parallel(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Concurrently fetches all data from the API."""
    results: List[Dict[str, Any]] = []
    page = 1
    while True:
        tasks = [
            asyncio.create_task(_fetch_page_async(page + i, client))
            for i in range(CONCURRENT_PAGES)
        ]
        pages_data = await asyncio.gather(*tasks)
        
        stop_pagination = False
        for batch in pages_data:
            if not batch:
                stop_pagination = True
                break
            results.extend(batch)
        
        if stop_pagination:
            break
        page += CONCURRENT_PAGES
    return results

async def _get_cached_customers() -> List[Dict[str, Any]]:
    """Manages cache validation and API synchronization."""
    global _CUSTOMERS_CACHE, _CACHE_ETAG

    async with _CACHE_LOCK:
        async with httpx.AsyncClient() as client:
            fingerprint = await _fetch_source_fingerprint(client)

            if _CUSTOMERS_CACHE and fingerprint and fingerprint == _CACHE_ETAG:
                logger.info("Cache Valid: Using memory state.")
                return _CUSTOMERS_CACHE

            logger.info(f"Cache Refresh: {fingerprint!r}")
            rows = await _fetch_all_pages_parallel(client)

        _CUSTOMERS_CACHE = rows
        _CACHE_ETAG = fingerprint
        return _CUSTOMERS_CACHE


# --- DLT RESOURCES ---

# Primary key hints ensure Postgres optimizes the SQL Joins during diffing
_HINTS = {"customer_id": {"data_type": "text", "primary_key": True}}

@dlt.resource(name="customers_staging", write_disposition="replace", columns=_HINTS)
def _staging_resource(rows: List[Customer]) -> Iterator[Dict[str, Any]]:
    """Loads current state into staging."""
    for c in rows:
        yield _to_dict(c)

@dlt.resource(name="customers", write_disposition="merge", primary_key="customer_id")
def _customers_resource(rows: Iterator[Customer]) -> Iterator[Dict[str, Any]]:
    """Applies merged updates to production."""
    for c in rows:
        yield _to_dict(c)


# --- MAIN INGESTION LOGIC ---

async def ingest_customers_async() -> int:
    """
    Orchestrates the full data pipeline:
    API -> Memory Cache -> Staging Table -> DB Side Diff -> Production Merge.
    """
    logger.info("Pipeline started.")

    raw_rows = await _get_cached_customers()
    if not raw_rows:
        return 0

    # Batch transformation using optimized list comprehension
    transformed: List[Customer] = [
        cust for r in raw_rows if (cust := _safe_transform(r))
    ]

    if not transformed:
        return 0

    pipeline = dlt.pipeline(
        pipeline_name="customer_ingestion",
        destination=dlt.destinations.postgres(DATABASE_URL),
        dataset_name="public",
    )

    # 1. Staging Pass (Full Refresh of staging table)
    pipeline.run(_staging_resource(transformed))

    # 2. Database-side Change Data Capture (CDC)
    # Using 'IS DISTINCT FROM' is the gold standard for comparing nullable Postgres columns
    with pipeline.sql_client() as client:
        diff_query = """
            SELECT s.customer_id FROM public.customers_staging s
            LEFT JOIN public.customers c USING (customer_id)
            WHERE c.customer_id IS NULL OR (
                s.first_name      IS DISTINCT FROM c.first_name OR
                s.last_name       IS DISTINCT FROM c.last_name OR
                s.email           IS DISTINCT FROM c.email OR
                s.phone           IS DISTINCT FROM c.phone OR
                s.address         IS DISTINCT FROM c.address OR
                s.date_of_birth   IS DISTINCT FROM c.date_of_birth OR
                s.account_balance IS DISTINCT FROM c.account_balance OR
                s.created_at      IS DISTINCT FROM c.created_at
            )
        """
        result = client.execute_sql(diff_query)
        changed_ids: Set[str] = {row[0] for row in result}

    if not changed_ids:
        logger.info("No changes found. Database is already in sync.")
        return 0

    # 3. Targeted Production Merge
    # Generator expression maintains a low memory footprint during the final load
    changed_gen = (c for c in transformed if c.customer_id in changed_ids)
    pipeline.run(_customers_resource(changed_gen))

    logger.info(f"Sync complete. Upserted {len(changed_ids)} records.")
    return len(changed_ids)