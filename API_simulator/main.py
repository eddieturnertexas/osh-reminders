# /// script
# requires-python = ">=3.10"
# dependencies = ["fastapi==0.141.1", "uvicorn==0.52.4"]
# ///

# ============================================================================
#    ___  ____  _   _
#   / _ \/ ___|| | | |      OAK STREET HEALTH
#  | | | \___ \| |_| |      Appointment Reminder Pipeline
#  | |_| |___) |  _  |      Databricks Asset Bundle / Medallion
#   \___/|____/|_| |_|
# ============================================================================
#  Service : API_simulator/main.py
#  Author  : eddie turner
#  Date    : 2026-09-13
#  Details : Stands in for the OSH source API during the pilot.
#            POST /payload pages the dataset with limit and offset.
# ============================================================================

import argparse
import json
import os
import secrets
from functools import lru_cache
from pathlib import Path

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer


DATA_FILE = Path(__file__).resolve().with_name("patients.json")
CAMPAIGN_ID = "OSH-PILOT-001"
MAX_PAGE_SIZE = 2000
PAGE_CACHE_SIZE = 64


def load_patients(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig") as source:
        patients = json.load(source)
    if not isinstance(patients, list) or not patients:
        raise ValueError("Patient dataset must be a nonempty JSON array of records.")
    signatures = set()
    for patient in patients:
        if not isinstance(patient, dict):
            raise ValueError("Every patient record must be a JSON object.")
        signature = patient.get("patient_signature")
        if not isinstance(signature, str) or not signature:
            raise ValueError("Every patient record must have a nonempty patient_signature.")
        if signature in signatures:
            raise ValueError("Patient signatures must be unique in the source dataset.")
        signatures.add(signature)
    return patients


PATIENTS = load_patients(DATA_FILE)
TOTAL = len(PATIENTS)

RECORD_JSON = [json.dumps(patient, separators=(",", ":")) for patient in PATIENTS]

app = FastAPI(
    title="OSH Appointment Reminder Pilot",
    description="POST /payload with limit and offset to page through the patient dataset.",
    version="4.1.0",
)

app.add_middleware(GZipMiddleware, minimum_size=1024)

bearer_auth = HTTPBearer(
    auto_error=False,
    scheme_name="OSHBearerToken",
    description="Enter the OSH_API_TOKEN configured in Azure App Service.",
)


def require_api_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_auth),
) -> None:
    expected = os.environ.get("OSH_API_TOKEN", "")
    if len(expected.strip()) < 32:
        raise HTTPException(
            status_code=503,
            detail="API authentication is not configured.",
            headers={"Cache-Control": "no-store"},
        )
    if credentials is None or not secrets.compare_digest(
        credentials.credentials.encode("utf-8"), expected.encode("utf-8")
    ):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing bearer token.",
            headers={"WWW-Authenticate": "Bearer", "Cache-Control": "no-store"},
        )


@lru_cache(maxsize=PAGE_CACHE_SIZE)
def build_page(limit: int | None, offset: int) -> bytes:
    start = min(max(offset, 0), TOTAL)
    end = TOTAL if limit is None else min(TOTAL, start + limit)
    header = (
        f'{{"campaign_id":"{CAMPAIGN_ID}",'
        f'"total":{TOTAL},'
        f'"offset":{start},'
        f'"limit":{json.dumps(limit)},'
        f'"returned":{end - start},'
        f'"has_more":{"true" if end < TOTAL else "false"},'
        f'"patients":['
    )
    return f'{header}{",".join(RECORD_JSON[start:end])}]}}'.encode("utf-8")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "version": app.version, "total_patients": TOTAL}


@app.post(
    "/payload",
    summary="Get a page of OSH patient records",
    dependencies=[Depends(require_api_token)],
    responses={401: {"description": "Invalid or missing bearer token"},
               503: {"description": "API authentication is not configured"}},
)
def patient_payload(
    limit: int | None = Query(default=None, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(default=0, ge=0),
) -> Response:
    return Response(
        content=build_page(limit, offset),
        media_type="application/json",
        headers={"Cache-Control": "no-store"},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)
