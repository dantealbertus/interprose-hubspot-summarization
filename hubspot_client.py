import asyncio
import os
import httpx

HUBSPOT_BASE = "https://api.hubapi.com"

DEAL_PROPS = (
    "dealname,amount,closedate,dealstage,pipeline,description,"
    "hs_lastmodifieddate,createdate,dealtype,closed_lost_reason,"
    "hs_is_closed_won,hs_deal_stage_probability"
)
CONTACT_PROPS = (
    "firstname,lastname,email,phone,jobtitle,company,"
    "hs_lead_status,createdate,notes_last_contacted"
)
COMPANY_PROPS = (
    "name,domain,industry,description,city,country,"
    "annualrevenue,numberofemployees,phone,website"
)
PREV_DEAL_PROPS = "dealname,amount,closedate,dealstage,closed_lost_reason,hs_is_closed_won"


class HubSpotClient:
    def __init__(self):
        token = os.getenv("HUBSPOT_ACCESS_TOKEN")
        if not token:
            raise ValueError("HUBSPOT_ACCESS_TOKEN is niet ingesteld")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def gather_deal_context(self, deal_id: str) -> dict:
        async with httpx.AsyncClient(headers=self.headers, timeout=30.0) as client:
            deal = await self._get_deal(client, deal_id)

            contact_ids = [
                r["id"]
                for r in deal.get("associations", {})
                .get("contacts", {})
                .get("results", [])
            ]
            company_ids = [
                r["id"]
                for r in deal.get("associations", {})
                .get("companies", {})
                .get("results", [])
            ]

            # Haal contacts, companies en deal-engagements parallel op
            parallel_tasks = (
                [self._get_contact(client, cid) for cid in contact_ids]
                + [self._get_company(client, cid) for cid in company_ids]
                + [self._get_engagements(client, "DEAL", deal_id, limit=50)]
            )
            results = await asyncio.gather(*parallel_tasks, return_exceptions=True)

            nc = len(contact_ids)
            nco = len(company_ids)
            contacts = [r for r in results[:nc] if not isinstance(r, Exception)]
            companies = [r for r in results[nc : nc + nco] if not isinstance(r, Exception)]
            deal_engs_raw = results[nc + nco]
            deal_engagements = deal_engs_raw if not isinstance(deal_engs_raw, Exception) else []

            # Haal per contact: engagements + eerdere deals parallel op
            contact_tasks = []
            for cid in contact_ids:
                contact_tasks.append(self._get_engagements(client, "CONTACT", cid, limit=20))
                contact_tasks.append(self._get_related_deals(client, "contacts", cid, exclude=deal_id, max_deals=5))

            company_tasks = [
                self._get_related_deals(client, "companies", cid, exclude=deal_id, max_deals=10)
                for cid in company_ids
            ]

            all_results = await asyncio.gather(*contact_tasks + company_tasks, return_exceptions=True)

            contact_engagements: dict[str, list] = {}
            contact_previous_deals: dict[str, list] = {}
            for i, cid in enumerate(contact_ids):
                eng_r = all_results[i * 2]
                deal_r = all_results[i * 2 + 1]
                contact_engagements[cid] = eng_r if not isinstance(eng_r, Exception) else []
                contact_previous_deals[cid] = deal_r if not isinstance(deal_r, Exception) else []

            company_deals: dict[str, list] = {}
            for i, cid in enumerate(company_ids):
                r = all_results[len(contact_tasks) + i]
                company_deals[cid] = r if not isinstance(r, Exception) else []

            return {
                "deal": deal,
                "contacts": contacts,
                "companies": companies,
                "deal_engagements": deal_engagements,
                "contact_engagements": contact_engagements,
                "contact_previous_deals": contact_previous_deals,
                "company_deals": company_deals,
            }

    # ─── HubSpot API helpers ──────────────────────────────────────────────────

    async def _get_deal(self, client: httpx.AsyncClient, deal_id: str) -> dict:
        r = await client.get(
            f"{HUBSPOT_BASE}/crm/v3/objects/deals/{deal_id}",
            params={"properties": DEAL_PROPS, "associations": "contacts,companies"},
        )
        r.raise_for_status()
        return r.json()

    async def _get_contact(self, client: httpx.AsyncClient, contact_id: str) -> dict:
        r = await client.get(
            f"{HUBSPOT_BASE}/crm/v3/objects/contacts/{contact_id}",
            params={"properties": CONTACT_PROPS},
        )
        r.raise_for_status()
        return r.json()

    async def _get_company(self, client: httpx.AsyncClient, company_id: str) -> dict:
        r = await client.get(
            f"{HUBSPOT_BASE}/crm/v3/objects/companies/{company_id}",
            params={"properties": COMPANY_PROPS},
        )
        r.raise_for_status()
        return r.json()

    async def _get_engagements(
        self, client: httpx.AsyncClient, object_type: str, object_id: str, limit: int = 50
    ) -> list:
        url = f"{HUBSPOT_BASE}/engagements/v1/engagements/associated/{object_type}/{object_id}/paged"
        all_engs: list = []
        params: dict = {"limit": min(limit, 100)}

        while len(all_engs) < limit:
            r = await client.get(url, params=params)
            if r.status_code == 404:
                break
            r.raise_for_status()
            data = r.json()
            all_engs.extend(data.get("results", []))
            if not data.get("hasMore") or len(all_engs) >= limit:
                break
            params["offset"] = data.get("offset")

        return all_engs[:limit]

    async def _get_related_deals(
        self,
        client: httpx.AsyncClient,
        from_object: str,
        object_id: str,
        exclude: str,
        max_deals: int = 5,
    ) -> list:
        r = await client.get(
            f"{HUBSPOT_BASE}/crm/v4/objects/{from_object}/{object_id}/associations/deals"
        )
        if r.status_code in (404, 400):
            return []
        r.raise_for_status()

        deal_ids = [
            str(item["toObjectId"])
            for item in r.json().get("results", [])
            if str(item["toObjectId"]) != str(exclude)
        ][:max_deals]

        deals = []
        for did in deal_ids:
            dr = await client.get(
                f"{HUBSPOT_BASE}/crm/v3/objects/deals/{did}",
                params={"properties": PREV_DEAL_PROPS},
            )
            if dr.status_code == 200:
                deals.append(dr.json())
        return deals
