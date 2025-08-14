### ETL Connector (Python → REST API → MongoDB)

A minimal, production-grade ETL that calls a REST API with an API key in headers, transforms records by adding timestamps, and loads them into MongoDB. Windows-friendly run steps included.

### Quickstart (Windows PowerShell)

- Create and activate a virtual environment
```powershell
py -m venv .venv
. .\.venv\Scripts\Activate.ps1
```

- Install dependencies
```powershell
pip install -r requirements.txt
```

- Configure environment
  - Edit `.env` with your values.
  - Defaults target a public JSON API, and still send your API key in the headers.

- Run the connector
```powershell
python etl_connector.py
```

### .env

- `.env` is ignored by Git. Example:
```ini
API_KEY=REPLACE_ME
API_BASE_URL=https://jsonplaceholder.typicode.com
API_ENDPOINTS=posts,comments,users
API_AUTH_HEADER=Authorization
API_AUTH_PREFIX=Bearer

MONGO_URI=mongodb://localhost:27017
MONGO_DB=etl_db
# Optional override (default: <host>_raw derived from API_BASE_URL)
# MONGO_COLLECTION=
```

### Behavior
- Sends `API_KEY` as `API_AUTH_HEADER` using `API_AUTH_PREFIX` (e.g., `Authorization: Bearer <KEY>`).
- Fetches each endpoint in `API_ENDPOINTS` from `API_BASE_URL`.
- Transforms each record by adding `ingested_at` (UTC) and `source_endpoint`.
- Loads into MongoDB collection named `<hostname>_raw` (e.g., `jsonplaceholder_typicode_com_raw`).
- Writes pretty JSON dumps to `etl_output/` for debugging.

### MongoDB
- Default URI is `mongodb://localhost:27017`; change via `MONGO_URI`.
- Database defaults to `etl_db`; change via `MONGO_DB`.
- One collection per connector: `<hostname>_raw` or via `MONGO_COLLECTION`.

### Testing & Validation Checklist
- Invalid responses and parsing errors raise with clear logs.
- Retries with exponential backoff on 429/5xx.
- Empty payloads result in 0 inserts; script continues.
- Connectivity issues are retried and logged.

### Notes
- The default `API_BASE_URL` is a public JSON service that does not require auth; your API key will be sent but ignored. Replace `API_BASE_URL` and `API_ENDPOINTS` to target your provider.
- Keep `.env` out of version control.
