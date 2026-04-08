from contextlib import asynccontextmanager
from typing import Any, Dict, Generator, AsyncIterator, List

import httpx
from fastapi import Depends, FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from database import Base, SessionLocal, engine
from models.customer import Customer
from services.ingestion import ingest_customers_async

# --- CONFIGURATION ---
MOCK_SERVER_URL: str  = "http://mock-server:5000/api/customers"
HEALTH_TIMEOUT: float = 5.0

# --- DATABASE DEPENDENCY ---
def get_db() -> Generator[Session, None, None]:
    """
    Yields a database session and ensures it is closed after the request.
    Using Generator typing to prevent static analysis errors.
    """
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- APP LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Manages application startup and shutdown events.
    Initializes database tables and a shared HTTP client for connection pooling.
    """
    # Create tables on startup
    Base.metadata.create_all(bind=engine)
    
    # Initialize shared httpx client to be reused across endpoints
    async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT) as client:
        app.state.http_client = client
        yield  # Application stays active here
        
    # Resources are automatically cleaned up after yield

app = FastAPI(
    title="Customer Pipeline API",
    version="1.1.0",
    lifespan=lifespan,
)

# --- ENDPOINTS ---

@app.get("/api/health", tags=["Monitoring"])
async def health_check() -> Dict[str, Any]:
    """
    Probes the upstream mock-server to verify connectivity.
    Returns the health status of the pipeline service.
    """
    report: Dict[str, Any] = {"service": "pipeline-service", "status": "healthy"}
    try:
        # Use the shared client from application state
        client: httpx.AsyncClient = app.state.http_client
        resp = await client.get(MOCK_SERVER_URL, params={"page": 1, "limit": 1})
        
        if resp.status_code != 200:
            report.update({
                "status": "degraded", 
                "detail": f"Upstream mock-server returned HTTP {resp.status_code}"
            })
    except (httpx.HTTPError, Exception) as exc:
        report.update({
            "status": "unhealthy", 
            "detail": f"Connectivity error: {str(exc)}"
        })
    
    return report

@app.post("/api/ingest", tags=["Pipeline"])
async def ingest() -> Dict[str, Any]:
    """
    Triggers the asynchronous DLT-based data ingestion process.
    Updates the local database with records from the mock server.
    """
    try:
        records_processed: int = await ingest_customers_async()
        return {
            "status": "success", 
            "records_processed": records_processed
        }
    except Exception as exc:
        # Returns error dictionary instead of raising HTTPException
        return {
            "status": "error",
            "detail": f"Pipeline ingestion failed: {str(exc)}"
        }

@app.get("/api/customers", tags=["Data"])
def list_customers(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Retrieves a paginated list of customers from the database.
    """
    try:
        # Get total record count for pagination metadata
        count_stmt = select(func.count()).select_from(Customer)
        total = db.scalar(count_stmt) or 0
        
        if total == 0:
            return {
                "status": "error", 
                "detail": "No customers found. Please run ingestion first."
            }

        # Fetch subset of data based on page and limit
        data_stmt = (
            select(Customer)
            .order_by(Customer.customer_id)
            .offset((page - 1) * limit)
            .limit(limit)
        )
        customers = db.execute(data_stmt).scalars().all()

        if not customers:
            return {
                "status": "error", 
                "detail": f"Page {page} not found."
            }

        return {
            "data": jsonable_encoder(customers),
            "total": total,
            "page": page,
            "limit": limit,
        }
    except Exception as exc:
        return {
            "status": "error", 
            "detail": str(exc)
        }

@app.get("/api/customers/{customer_id}", tags=["Data"])
def get_customer(
    customer_id: str, 
    db: Session = Depends(get_db)
) -> Dict:
    """
    Fetches a specific customer record by their primary key ID.
    """
    try:
        stmt = select(Customer).where(Customer.customer_id == customer_id)
        customer = db.execute(stmt).scalar_one_or_none()

        if not customer:
            return {
                "status": "error", 
                "detail": f"Customer ID {customer_id} not found."
            }
            
        return jsonable_encoder(customer)
    except Exception as exc:
        return {
            "status": "error", 
            "detail": str(exc)
        }
