import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin, urlparse

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ---------- Logging ----------
ROOT_DIR = Path(__file__).resolve().parent
LOG_FILE = ROOT_DIR / "etl_connector.log"
logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s [%(levelname)s] %(message)s",
	handlers=[
		logging.FileHandler(LOG_FILE, encoding="utf-8"),
		logging.StreamHandler()
	],
)
logger = logging.getLogger("etl")


# ---------- Config ----------
def load_config() -> Dict[str, Any]:
	load_dotenv()  # load from .env if present

	api_base_url = os.getenv("API_BASE_URL", "https://jsonplaceholder.typicode.com").rstrip("/")
	api_endpoints_csv = os.getenv("API_ENDPOINTS", "posts,comments,users")
	api_endpoints = [e.strip().lstrip("/") for e in api_endpoints_csv.split(",") if e.strip()]

	api_key = os.getenv("API_KEY", "")
	api_auth_header = os.getenv("API_AUTH_HEADER", "Authorization")
	api_auth_prefix = os.getenv("API_AUTH_PREFIX", "Bearer")

	mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
	mongo_db = os.getenv("MONGO_DB", "etl_db")
	mongo_collection = os.getenv("MONGO_COLLECTION", "")

	parsed = urlparse(api_base_url)
	connector_name = parsed.hostname.replace(".", "_") if parsed.hostname else "connector"

	if not mongo_collection:
		mongo_collection = f"{connector_name}_raw"

	return {
		"api_base_url": api_base_url,
		"api_endpoints": api_endpoints,
		"api_key": api_key,
		"api_auth_header": api_auth_header,
		"api_auth_prefix": api_auth_prefix,
		"mongo_uri": mongo_uri,
		"mongo_db": mongo_db,
		"mongo_collection": mongo_collection,
		"connector_name": connector_name,
	}


def build_session_with_retries() -> requests.Session:
	session = requests.Session()
	retry = Retry(
		total=5,
		backoff_factor=1.5,
		status_forcelist=[429, 500, 502, 503, 504],
		allowed_methods={"GET", "POST", "PUT", "DELETE", "PATCH"},
		raise_on_status=False,
	)
	adapter = HTTPAdapter(max_retries=retry)
	session.mount("http://", adapter)
	session.mount("https://", adapter)
	session.headers.update({
		"Accept": "application/json",
		"Content-Type": "application/json",
		"User-Agent": "etl-connector/1.0",
	})
	return session


def build_auth_headers(api_key: str, header_name: str, prefix: str) -> Dict[str, str]:
	if not api_key:
		return {}
	value = f"{prefix} {api_key}".strip()
	# If prefix is empty, avoid leading space
	if not prefix:
		value = api_key
	return {header_name: value}


def ensure_list_payload(data: Any) -> List[Dict[str, Any]]:
	if isinstance(data, list):
		return data
	if isinstance(data, dict):
		# Common patterns: {"data": [...]} or {"items": [...]} or single object
		for key in ("data", "items", "results"):
			if key in data and isinstance(data[key], list):
				return data[key]
		return [data]
	return []


def extract(
	session: requests.Session,
	base_url: str,
	endpoint: str,
	auth_headers: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
	url = urljoin(f"{base_url}/", endpoint)
	logger.info("Fetching %s", url)
	response = session.get(url, headers=auth_headers, timeout=30)

	# Handle rate limiting
	if response.status_code == 429:
		retry_after = response.headers.get("Retry-After")
		logger.warning("Rate limited (429). Retry-After=%s", retry_after)

	if not response.ok:
		logger.error("HTTP error %s for %s: %s", response.status_code, url, response.text[:500])
		response.raise_for_status()

	try:
		data = response.json()
	except Exception as exc:  # noqa: BLE001
		logger.exception("Failed to parse JSON from %s", url)
		raise exc

	records = ensure_list_payload(data)
	meta = {
		"status_code": response.status_code,
		"url": url,
		"record_count": len(records),
	}
	return records, meta


def transform(records: List[Dict[str, Any]], endpoint: str) -> List[Dict[str, Any]]:
	ingested_at = datetime.now(timezone.utc)
	transformed: List[Dict[str, Any]] = []
	for record in records:
		# Shallow copy to avoid mutating original
		doc = dict(record)
		doc["ingested_at"] = ingested_at
		doc["source_endpoint"] = endpoint
		transformed.append(doc)
	return transformed


def load_to_mongo(
	mongo_uri: str,
	database_name: str,
	collection_name: str,
	documents: List[Dict[str, Any]],
) -> int:
	if not documents:
		return 0
	client = MongoClient(mongo_uri)
	collection = client[database_name][collection_name]
	result = collection.insert_many(documents, ordered=False)
	return len(result.inserted_ids)


def write_debug_json(endpoint: str, records: List[Dict[str, Any]]) -> Path:
	output_dir = ROOT_DIR / "etl_output"
	output_dir.mkdir(parents=True, exist_ok=True)
	ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
	file_path = output_dir / f"{endpoint}_{ts}.json"
	with file_path.open("w", encoding="utf-8") as f:
		json.dump(records, f, ensure_ascii=False, indent=2, default=str)
	return file_path


def run() -> None:
	cfg = load_config()
	session = build_session_with_retries()
	auth_headers = build_auth_headers(cfg["api_key"], cfg["api_auth_header"], cfg["api_auth_prefix"])

	total_inserted = 0
	for endpoint in cfg["api_endpoints"]:
		try:
			raw_records, meta = extract(session, cfg["api_base_url"], endpoint, auth_headers)
			logger.info("Fetched %d records from %s", meta["record_count"], meta["url"]) 

			docs = transform(raw_records, endpoint)
			inserted = load_to_mongo(
				cfg["mongo_uri"], cfg["mongo_db"], cfg["mongo_collection"], docs
			)
			total_inserted += inserted
			logger.info("Inserted %d docs into %s.%s", inserted, cfg["mongo_db"], cfg["mongo_collection"]) 

			debug_file = write_debug_json(endpoint, docs)
			logger.info("Wrote %s", debug_file)
		except Exception as exc:  # noqa: BLE001
			logger.exception("Failed processing endpoint '%s'", endpoint)

	logger.info("Done. Total inserted: %d", total_inserted)


if __name__ == "__main__":
	run()
