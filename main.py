import logging
import os
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

load_dotenv()

from hubspot_client import HubSpotClient
from summarizer import summarize_deal_context

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="HubSpot Deal Summarizer")


class DealRequest(BaseModel):
    deal_id: str


@app.post("/summarize")
async def summarize(
    request: DealRequest,
    x_api_key: Optional[str] = Header(default=None),
):
    expected_key = os.getenv("API_KEY")
    if expected_key and x_api_key != expected_key:
        raise HTTPException(status_code=401, detail="Ongeldige API key")

    logger.info("Samenvatting aangevraagd voor deal: %s", request.deal_id)
    try:
        client = HubSpotClient()
        context = await client.gather_deal_context(request.deal_id)
        summary = await summarize_deal_context(context)
        logger.info("Samenvatting klaar voor deal: %s", request.deal_id)
        return {"deal_id": request.deal_id, "summary": summary, "status": "success"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Fout bij deal %s: %s", request.deal_id, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
