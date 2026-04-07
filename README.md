# Mock Pipeline

[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-compose-blue.svg)](https://docs.docker.com/compose/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A robust end-to-end data pipeline that synchronizes customer records from a mock API to PostgreSQL, featuring efficient change tracking, merge operations, and containerized deployment.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Quick Start](#quick-start)
- [API Endpoints](#api-endpoints)
- [CI/CD Workflow](#cicd-workflow)
- [Contributing](#contributing)
- [License](#license)
- [Credits](#credits)

---

## Overview

This project implements a **customer data pipeline** consisting of three Dockerized microservices that work together to extract, transform, and load customer data efficiently.

**Data Flow:**
```
Flask Mock Server → FastAPI Pipeline Service → PostgreSQL Database
     (JSON Data)        (ETL Processing)       (Persistent Storage)
```

---

## Features

- **Microservices Architecture** — Modular design with independent services for scalability
- **Efficient Data Sync** — Staging-based diff via `IS DISTINCT FROM` prevents unnecessary upserts
- **RESTful APIs** — Clean endpoints for data ingestion and retrieval
- **Smart Caching** — Upstream fingerprint detection (ETag / Last-Modified / body hash) skips re-fetch when source is unchanged
- **Containerized Deployment** — Easy setup with Docker Compose
- **Robust Error Handling** — Comprehensive error responses and safe type parsing
- **Pagination Support** — Efficient handling of large datasets

---

## Architecture

### Project Structure

```
mock-pipeline/
├── .github/
│   └── workflows/
│       └── ci-cd.yml             # GitHub Actions CI/CD
├── docker-compose.yml
├── README.md
├── mock-server/
│   ├── app.py                    # Flask API main application
│   ├── data/customers.json       # Mock customer dataset
│   ├── Dockerfile
│   └── requirements.txt
└── pipeline-service/
    ├── main.py                   # FastAPI application entry point
    ├── models/customer.py        # SQLAlchemy data models
    ├── services/ingestion.py     # DLT-powered ingestion logic
    ├── database.py               # Database connection setup
    ├── Dockerfile
    └── requirements.txt
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| **Mock Server** | Serves realistic customer data from a local JSON file via REST API |
| **Pipeline Service** | Orchestrates ETL operations using DLT |
| **PostgreSQL** | Stores processed customer records |

### ETL Process

1. **Extract** — Fetch paginated customer data from the Flask mock server in parallel batches
2. **Transform** — Parse and validate fields; skip rows with missing required values
3. **Load** — Push all rows to a DLT-managed staging table, diff against `customers` in Postgres, upsert only changed rows

---

## Technology Stack

- **Runtime**: Python 3.10+
- **Frameworks**: Flask 3.0.3 (Mock API), FastAPI 0.115.14 (Pipeline Service)
- **Database**: PostgreSQL 15 with SQLAlchemy 2.0.49 ORM
- **ETL**: DLT 1.24.0 for pipeline orchestration
- **Containerization**: Docker & Docker Compose
- **HTTP Clients**: Requests, HTTPX 0.23.1

### Dependencies

**Mock Server**
```
flask==3.0.3
requests
```

**Pipeline Service**
```
fastapi==0.115.14
uvicorn==0.34.3
sqlalchemy==2.0.49
psycopg2-binary==2.9.11
dlt[postgres]==1.24.0
httpx==0.23.1
```

---

## Quick Start

### Prerequisites

- Docker Desktop (running)
- Python 3.10+
- Git

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/codinersmillenium/mock-pipeline.git
   cd mock-pipeline
   ```

2. **Start all services:**
   ```bash
   docker-compose up --build -d
   ```

3. **Verify deployment:**

   | Service | URL |
   |---|---|
   | Mock Server | http://localhost:5000 |
   | Pipeline API | http://localhost:8000 |
   | Database | localhost:5432 |

### Basic Usage

```bash
# Ingest customer data
curl -X POST http://localhost:8000/api/ingest

# Retrieve customers (paginated)
curl "http://localhost:8000/api/customers?page=1&limit=10"

# Get a specific customer
curl http://localhost:8000/api/customers/<customer_id>
```

---

## API Endpoints

### Mock Server — `localhost:5000`

| Endpoint | Method | Description |
|---|---|---|
| `/api/health` | GET | Service health check |
| `/api/customers` | GET | Paginated customer list |
| `/api/customers/{id}` | GET | Single customer by ID |

### Pipeline Service — `localhost:8000`

| Endpoint | Method | Description |
|---|---|---|
| `/api/health` | GET | Service health check (probes mock server) |
| `/api/ingest` | POST | Trigger full data ingestion |
| `/api/customers` | GET | Retrieve customers from database |
| `/api/customers/{id}` | GET | Get specific customer by ID |

---

## CI/CD Workflow

This project uses GitHub Actions for automated testing and deployment.

### Workflow Overview

| Step | What happens |
|---|---|
| **Linting** | `flake8` checks for syntax errors and undefined variables |
| **Container build** | All services built via `docker-compose up --build` |
| **Integration test** | Health check both services after startup |
| **Deploy** | Runs only on push to `main` after all tests pass |

### GitHub Actions — `.github/workflows/ci-cd.yml`

```yaml
name: CI/CD Pipeline

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install -r mock-server/requirements.txt
          pip install -r pipeline-service/requirements.txt

      - name: Run linting
        run: |
          pip install flake8
          flake8 mock-server/ pipeline-service/ \
            --count \
            --select=E9,F63,F7,F82 \
            --show-source \
            --statistics

      - name: Build and test containers
        run: |
          docker-compose up --build -d
          sleep 30
          curl -f http://localhost:5000/api/health
          curl -f http://localhost:8000/api/health
          docker-compose down

  deploy:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    steps:
      - name: Deploy to production
        run: echo "Deployment steps here"
```

### Local CI/CD Simulation

```bash
# 1. Run linting
pip install flake8
flake8 mock-server/ pipeline-service/

# 2. Build and test containers
docker-compose up --build -d
sleep 30
curl -f http://localhost:5000/api/health
curl -f http://localhost:8000/api/health

# 3. Tear down
docker-compose down
```

---

## Contributing

This project uses a **single branch model** with `main` as the only permanent branch.

```
main
 ├── feature/your-feature     ← new features
 ├── fix/your-bugfix          ← bug fixes
 └── chore/your-task          ← maintenance (docs, deps, config)
```

### Contribution Workflow

**1. Fork & Clone**
```bash
git clone https://github.com/codinersmillenium/mock-pipeline.git
cd mock-pipeline
```

**2. Create a branch from `main`**
```bash
git checkout main
git pull origin main
git checkout -b feature/your-feature
```

Branch naming convention:

| Type | Format | Example |
|---|---|---|
| New feature | `feature/your-feature` | `feature/add-retry-logic` |
| Bug fix | `fix/your-bugfix` | `fix/cache-invalidation` |
| Maintenance | `chore/your-task` | `chore/update-dependencies` |

**3. Make changes & check linting**
```bash
pip install flake8
flake8 mock-server/ pipeline-service/ \
  --count --select=E9,F63,F7,F82 --show-source --statistics
```

**4. Commit**

Use a descriptive commit message:

| Prefix | When to use |
|---|---|
| `feat:` | new feature |
| `fix:` | bug fix |
| `chore:` | deps, config, or docs update |
| `refactor:` | code restructure without behavior change |

```bash
git add .
git commit -m "feat: add retry logic for failed page fetch"
```

**5. Push & open a Pull Request to `main`**
```bash
git push origin feature/your-feature
```

PR checklist:
- [ ] All 3 services start with docker-compose up
- [ ] Flask serves data with pagination
- [ ] FastAPI ingests data successfully
- [ ] All API endpoints work

**6. After the PR is merged, clean up your local branch**
```bash
git checkout main
git pull origin main
git branch -d feature/your-feature
```

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## Credits

**Built with:** Flask, FastAPI, DLT, SQLAlchemy, Docker, PostgreSQL, HTTPX, Requests

**Author:** Rizqi Nurhadiyansyah
**Email:** rizqidsas@gmail.com
