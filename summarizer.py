import os
from datetime import datetime
from typing import Optional

import anthropic

_async_client = anthropic.AsyncAnthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

_SYSTEM_PROMPT = """\
You are a CRM assistant that helps sales teams prepare for new deals.
You analyze historical HubSpot data and produce concise, actionable summaries for sales.
Write in English. Be concrete, brief and to the point.
Avoid vague statements — mention specific names, amounts, dates and pain points when available.

Follow these formatting rules strictly:
- Do not use markdown such as **, ## or other formatting characters.
- Use capital letters for section titles.
- Place a blank line between each section.
- Use - for bullet points.
- Place a blank line between the section title and its bullet points.
- Place a blank line after the last bullet point of a section, before the next section title.
- Write in a professional and concise manner.
- Do not include any additional text outside this structure."""

_USER_TEMPLATE = """\
Analyze the HubSpot data below and write a summary that is useful when starting a new deal \
with this company or these contacts.

Cover the following points (skip a point if there is no relevant data):

PAIN POINTS & CHALLENGES
What problems or frustrations have come up before?

DESIRES & GOALS
What do they want to achieve? What results are they looking for?

DECISION MAKING & OBJECTIONS
Who are the decision makers? What objections or blockers have come up before?

DEAL HISTORY
Previously won/lost deals, amounts offered, reasons.

COMMUNICATION STYLE
How do they communicate? What worked or didn't work in previous interactions?

KEY TALKING POINTS
Critical context to know before a new conversation.

--- HubSpot data ---

{context}

--- End of data ---

Provide a structured summary of no more than 500 words. Follow the formatting rules exactly."""


def _format_timestamp(ts_ms: Optional[int]) -> str:
    if not ts_ms:
        return ""
    try:
        return datetime.fromtimestamp(ts_ms / 1000).strftime("%d-%m-%Y")
    except Exception:
        return ""


def _format_engagement(eng: dict) -> str:
    e = eng.get("engagement", {})
    m = eng.get("metadata", {})
    eng_type = e.get("type", "")
    date = _format_timestamp(e.get("timestamp"))

    if eng_type == "NOTE":
        body = (m.get("body") or "").strip()[:600]
        return f"[Notitie {date}] {body}"
    elif eng_type == "EMAIL":
        sender = (m.get("from") or {}).get("email", "")
        subject = m.get("subject", "")
        body = (m.get("text") or m.get("body") or "").strip()[:400]
        return f"[E-mail {date}] Van: {sender} | Onderwerp: {subject} | {body}"
    elif eng_type == "CALL":
        dur = (m.get("durationMilliseconds") or 0) // 60000
        body = (m.get("body") or "").strip()[:400]
        return f"[Gesprek {date}] Duur: {dur} min | {body}"
    elif eng_type == "MEETING":
        title = m.get("title", "")
        body = (m.get("body") or "").strip()[:300]
        return f"[Meeting {date}] {title} | {body}"
    elif eng_type == "TASK":
        subject = m.get("subject", "")
        status = m.get("status", "")
        return f"[Taak {date}] {subject} | Status: {status}"
    return f"[{eng_type} {date}]"


def _build_context_text(context: dict) -> str:
    lines: list[str] = []

    # Deal
    deal = context["deal"]
    dp = deal.get("properties", {})
    lines += [
        "=== DEAL ===",
        f"Naam: {dp.get('dealname', '')}",
        f"Fase: {dp.get('dealstage', '')}",
        f"Pipeline: {dp.get('pipeline', '')}",
        f"Bedrag: {dp.get('amount', '')}",
        f"Sluitdatum: {dp.get('closedate', '')}",
        f"Aangemaakt: {dp.get('createdate', '')}",
    ]
    if dp.get("description"):
        lines.append(f"Beschrijving: {dp['description']}")
    if dp.get("closed_lost_reason"):
        lines.append(f"Reden verloren: {dp['closed_lost_reason']}")
    if dp.get("hs_is_closed_won") == "true":
        lines.append("Status: Gewonnen")
    lines.append("")

    # Deal engagements
    deal_engs = context.get("deal_engagements", [])
    if deal_engs:
        lines.append("=== ACTIVITEITEN OP DEZE DEAL ===")
        for eng in deal_engs:
            lines.append(_format_engagement(eng))
        lines.append("")

    # Companies
    for company in context.get("companies", []):
        cp = company.get("properties", {})
        cid = company.get("id", "")
        lines += [
            "=== BEDRIJF ===",
            f"Naam: {cp.get('name', '')}",
            f"Industrie: {cp.get('industry', '')}",
            f"Website: {cp.get('website', '')}",
            f"Omzet: {cp.get('annualrevenue', '')}",
            f"Medewerkers: {cp.get('numberofemployees', '')}",
            f"Locatie: {cp.get('city', '')} {cp.get('country', '')}",
        ]
        if cp.get("description"):
            lines.append(f"Beschrijving: {cp['description']}")

        prev = context.get("company_deals", {}).get(cid, [])
        if prev:
            lines.append("Eerdere deals van dit bedrijf:")
            for d in prev:
                ddp = d.get("properties", {})
                status = "Gewonnen" if ddp.get("hs_is_closed_won") == "true" else ddp.get("dealstage", "")
                line = f"  - {ddp.get('dealname', '')} | {status} | €{ddp.get('amount', '')} | {ddp.get('closedate', '')}"
                if ddp.get("closed_lost_reason"):
                    line += f" | Reden verloren: {ddp['closed_lost_reason']}"
                lines.append(line)
        lines.append("")

    # Contacts
    for contact in context.get("contacts", []):
        cp = contact.get("properties", {})
        cid = contact.get("id", "")
        naam = f"{cp.get('firstname', '')} {cp.get('lastname', '')}".strip()
        lines += [
            "=== CONTACTPERSOON ===",
            f"Naam: {naam}",
            f"Functie: {cp.get('jobtitle', '')}",
            f"E-mail: {cp.get('email', '')}",
            f"Telefoon: {cp.get('phone', '')}",
            f"Bedrijf: {cp.get('company', '')}",
        ]

        prev_deals = context.get("contact_previous_deals", {}).get(cid, [])
        if prev_deals:
            lines.append(f"Eerdere deals van {naam}:")
            for d in prev_deals:
                ddp = d.get("properties", {})
                status = "Gewonnen" if ddp.get("hs_is_closed_won") == "true" else ddp.get("dealstage", "")
                lines.append(f"  - {ddp.get('dealname', '')} | {status} | €{ddp.get('amount', '')}")

        engs = context.get("contact_engagements", {}).get(cid, [])
        if engs:
            lines.append(f"Communicatiegeschiedenis met {naam}:")
            for eng in engs:
                lines.append(f"  {_format_engagement(eng)}")
        lines.append("")

    return "\n".join(lines)


async def summarize_deal_context(context: dict) -> str:
    context_text = _build_context_text(context)

    # Begrens de contextgrootte om tokenlimieten te vermijden
    if len(context_text) > 300_000:
        context_text = context_text[:300_000] + "\n...[tekst ingekort vanwege omvang]"

    user_message = _USER_TEMPLATE.format(context=context_text)

    # Gebruik streaming met prompt-caching op het systeemsysteem
    async with _async_client.messages.stream(
        model="claude-opus-4-7",
        max_tokens=2048,
        thinking={"type": "adaptive"},
        system=[
            {
                "type": "text",
                "text": _SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_message}],
    ) as stream:
        message = await stream.get_final_message()

    for block in message.content:
        if block.type == "text":
            return block.text

    return ""
