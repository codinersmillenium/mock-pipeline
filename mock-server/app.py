import json
import os
import threading
import requests
from typing import List, Dict, Optional, TypedDict, Any
from flask import Flask, jsonify, request, Response

app = Flask(__name__)

# --- CONFIGURATION ---
BASE_DIR             = os.path.dirname(__file__)
DATA_PATH: str       = os.path.join(BASE_DIR, "data", "customers.json")
MOCK_SERVER_URL: str = "http://mock-server:5000/api/customers"
TIMEOUT_SEC: int     = 5

# --- TYPES ---
class Customer(TypedDict, total=False):
    customer_id: str
    first_name: str
    last_name: str
    email: Optional[str]
    phone: Optional[str]
    address: Optional[str]
    date_of_birth: Optional[str]
    account_balance: Optional[float]
    created_at: Optional[str]

# --- THREAD-SAFE CACHE ---
_CUSTOMERS_CACHE: List[Customer]       = []
_CUSTOMERS_LOOKUP: Dict[str, Customer] = {}
_LAST_MODIFIED: Optional[float]        = None
_CACHE_LOCK                            = threading.Lock()


# --- DATA LOADER ---
def _load_customers() -> None:
    """
    Thread-safe loader for customer data.
    Reloads only if the file is modified to reduce I/O overhead.
    """
    global _CUSTOMERS_CACHE, _CUSTOMERS_LOOKUP, _LAST_MODIFIED

    if not os.path.exists(DATA_PATH):
        with _CACHE_LOCK:
            _CUSTOMERS_CACHE = []
            _CUSTOMERS_LOOKUP = {}
            _LAST_MODIFIED = None
        return

    try:
        current_mtime: float = os.path.getmtime(DATA_PATH)
        
        # Check if reload is needed
        if _LAST_MODIFIED == current_mtime:
            return

        with _CACHE_LOCK:
            # Double-check inside lock to prevent multiple reloads
            if _LAST_MODIFIED == current_mtime:
                return

            with open(DATA_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, list):
                raise ValueError("Data source must be a JSON list")

            _CUSTOMERS_CACHE  = data
            _CUSTOMERS_LOOKUP = {str(c["customer_id"]): c for c in data if "customer_id" in c}
            _LAST_MODIFIED    = current_mtime
            
    except Exception as e:
        # Avoid crashing the server if JSON is malformed
        print(f"[ERROR] Failed to load customers: {e}")


# --- HELPERS ---
def _parse_positive_int(value: Any, default: int = 1) -> int:
    """Safely convert query parameters to positive integers."""
    try:
        if value is None: return default
        n = int(value)
        return max(n, 1)
    except (ValueError, TypeError):
        return default

def _paginated_response(items: List[Customer], page: int, limit: int) -> Dict[str, Any]:
    """Helper to structure paginated results."""
    total = len(items)
    start = (page - 1) * limit
    end   = start + limit
    return {
        "data" : items[start:end],
        "total": total,
        "page" : page,
        "limit": limit
    }


# --- ENDPOINTS ---

@app.route("/api/health", methods=["GET"])
def health_check() -> Response:
    """
    Verifies if the upstream mock server is reachable.
    """
    try:
        # Use shared session or simple get
        resp = requests.get(MOCK_SERVER_URL, params={"page": 1, "limit": 1}, timeout=TIMEOUT_SEC)
        
        if resp.status_code == 200:
            return jsonify({
                "status": "healthy", 
                "service": "mock-server", 
                "upstream": "reachable"
            }), 200
        
        return jsonify({
            "status": "degraded", 
            "detail": f"Upstream returned {resp.status_code}"
        }), 503

    except requests.exceptions.RequestException as e:
        return jsonify({
            "status": "unhealthy", 
            "detail": str(e)
        }), 503


@app.route("/api/customers", methods=["GET"])
def get_customers() -> Response:
    """Returns a paginated list of customers from the cached JSON file."""
    try:
        _load_customers()
        
        page   = _parse_positive_int(request.args.get("page"), 1)
        limit  = min(_parse_positive_int(request.args.get("limit"), 10), 100)

        result = _paginated_response(_CUSTOMERS_CACHE, page, limit)
        
        if page > 1 and not result["data"]:
            return jsonify({"error": f"Page {page} exceeds total data"}), 404
            
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": f"Internal Server Error: {str(e)}"}), 500


@app.route("/api/customers/<customer_id>", methods=["GET"])
def get_customer(customer_id: str) -> Response:
    """Fetches a single customer by their unique ID."""
    try:
        _load_customers()
        customer = _CUSTOMERS_LOOKUP.get(str(customer_id))
        
        if not customer:
            return jsonify({"error": f"Customer ID {customer_id} not found"}), 404
            
        return jsonify(customer), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Ensure data directory exists
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    
    # Initial load
    _load_customers()
    print(f"[INFO] Mock server active. Serving {len(_CUSTOMERS_CACHE)} records.")
    
    # In production, use Gunicorn instead of app.run
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)