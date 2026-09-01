import os
import json
import time
import uuid
import threading
import requests
from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS

app = Flask(__name__, static_folder=None)
CORS(app)

APP_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── MODEL CONFIG ─────────────────────────────────────────────────────────────
# Single source of truth for the screening model. Override via env var if needed.
# NOTE: 'claude-sonnet-4-20250514' was RETIRED on 2026-04-20 — using it caused
# every API call to fail, which is why the screener "completed" with 0 results.
SCREENING_MODEL = os.environ.get('SCREENING_MODEL', 'claude-sonnet-4-6')

# ─── HOOK GENERATION RULES ────────────────────────────────────────────────────
# Controls the {{hook}} text the model writes for Step 1 of BOTH the Personal
# and Standard sequences. Step 1 is identical across both sequences, so a single
# hook (same rules, same parameters) serves both tracks. Reply.io places {{hook}}
# as its OWN paragraph, right after the greeting and right before the body:
#
#   Subject: An Army veteran hoping to learn about your business
#
#   Hi {{FirstName}},
#
#   {{hook}}
#
#   I am looking to buy one company to own and operate for the next 20+ years.
#   I am not a financial buyer or private equity.
#
#   By way of background, I am an Army veteran and currently CFO of a large
#   California public agency ...
#
#   I would appreciate 15 to 20 minutes to hear how you built {{Company}}.
#
# So the hook is the OPENING line of the email. It has to earn attention on its
# own and hand off cleanly into "I am looking to buy one company ...". Step 1 is
# already long, so the hook must stay short and must not repeat anything the body
# already says. Keep one space on each side of {{hook}} in the Reply.io template
# so the paragraphs don't collide. Edit, add, or delete any numbered rule below;
# it flows straight into the screening prompt with no other code changes needed.
HOOK_RULES = """HOOK RULES (write ONLY the text that replaces {{hook}}, nothing else):
Guiding principle: the best hooks feel inevitable, not clever. Before writing, pick the single strongest genuine angle (a real overlap per rules 6 and 7, or otherwise the strongest operational fact about the business), then write the line. Do not force cleverness or personalization.
1. Position: the hook is the FIRST line of the email, right after "Hi {FirstName},". It must grab attention in a single read and lead naturally into the next paragraph, which begins "I am looking to buy one company to own and operate for the next 20+ years." Do NOT greet. Do NOT use the recipient's first name anywhere in the hook, not even in the third person (never "Pat joined in 1993" or "while Arlen oversees every project"), because the greeting directly above already addresses them by name. Write "you" or refer to the company instead. The recipient's name appearing inside the company's own name is fine.
2. Length: keep it tight, because the rest of Step 1 is already long. Default to one or two sentences. If there is a particularly strong, specific connection that needs the room, you may use a third sentence, use your best judgment. Never pad, and never more than three sentences. Choose ONE primary idea. Do not combine multiple unrelated personalization angles unless they naturally reinforce one another.
3. No duplication with the rest of Step 1. The body already covers all of the following about the SENDER, so the hook must NOT state them: the intent to buy, acquire, or search for a company; owning and operating for the long term or for "20+ years"; being a long-term operator rather than a financial buyer or private equity; the sender's own Army veteran service; the sender's own role as a CFO of a California public agency; and the closing request for 15 to 20 minutes or to hear how they built the company. Do NOT propose a call, a meeting, a conversation, or "connecting" anywhere in the hook. (How to handle the RECIPIENT's background, including any overlap with the sender, is governed by rules 6 and 7.)
4. Do NOT name Toluca Lake Group, or any company, fund, or entity, for the sender.
5. Do NOT use dashes or em dashes of any kind ( - , -- , en dash, em dash ). Use commas instead.
6. You MAY reference the recipient's own background, even where it overlaps with the sender's, but do NOT state the sender's matching trait or claim the shared bond directly. The body already reveals that the sender is an Army veteran and a California public agency CFO, so for any military, veteran, finance, or public sector overlap, never write the connection from the sender's side. Forbidden examples: "I too am a veteran", "as a fellow veteran", "I also spent my career in public service", "we share a public sector background". You may instead note the recipient's own service or public sector background as a fact about them, and let the body do the connecting, since the reader will infer the shared ground on their own.
7. For genuine overlaps the body does NOT mention, you MAY draw the connection directly. The body covers only the sender's veteran service and public agency CFO role, nothing else about the sender. The sender's other verified personal attributes, and the ONLY ones that may be treated as personal overlaps, are: first generation immigrant background, endurance athletics, a single parent upbringing, and California university education. When the recipient genuinely shares one of these, you may name it directly in the hook, since the body will not surface it. CRITICAL: do NOT treat attributes of the RECIPIENT'S BUSINESS as shared connections. A bootstrapped or built from nothing origin, long tenure, family ownership, succession or retirement timing, owner operator status, and a government or public sector client base are the recipient's traits and the sender's acquisition criteria. They are reasons the sender wants to buy the company, NOT common ground, and stating them as such produces the exact repetition rule 11 forbids. Statewide California location is shared with every company contacted and is not a bond; treat geography as a business fact under rule 8, never as a connection. Use only real, verified overlaps. Do NOT force or stretch a connection. Manufactured or trivial connections are worse than none. Avoid example: "As another dog lover, I noticed your office mascot." Most contacts will have no genuine personal overlap, and that is expected: the DEFAULT and most common hook is a concrete, respectful observation about the recipient's business alone. A specific business observation is always better than a stretched personal connection.
8. Be specific and verifiable. Open on something real about the recipient or the company (years in business, the region served, a niche, a recognizable detail) so the line could only have been written to this owner. Prefer durable, evergreen facts over temporary announcements (for example "Serving Central Valley growers since the 1980s" rather than a recent trade show appearance). If two facts are equally specific, prefer the one another long term owner would naturally notice. Avoid generic flattery such as "impressive company" or "great work". Do NOT use placeholder nouns such as "something" or "a business" in place of the actual company: always name the company or state concretely what it does. Do NOT open with a generic aphorism, proverb, or observation about a category of people (for example "lifelong cyclists tend to understand..."); the first thing the reader sees must be about them or their company, not a general truism.
9. Tone: warm, plain, peer to peer, human, one future owner introducing himself to another. Curious rather than complimentary in stance (interested in how they built it, not praising them), though never phrase this as a question to the recipient. Match the voice of the email body. No buzzwords, no superlatives, no salesy language.
10. Format: start with a capital letter, end with a period, and return NO leading or trailing spaces. Plain text only.
11. The hook's FINAL clause must be about the recipient's company or its work, never about the sender. The very next line of the email begins "I am looking to buy one company...", so any statement of what the sender noticed, felt, respects, or is seeking creates clumsy repetition. Do NOT end the hook with a clause whose subject is "I". This is a rule about meaning, not a list of phrases: any wording that reports the sender's reaction is banned, including but not limited to "caught my attention", "drew me to reach out", "made me want to reach out", "I noticed", "I notice it", "I noticed it right away", "I paid close attention", "I pay close attention to", "I find myself drawn to", "the reason I wanted to connect", "what I have been looking for", "set out to find", "the kind of business I am interested in". End instead on the company. The closing clause must attach to the SPECIFIC fact you just stated, not deliver a generic verdict. Do NOT use the phrase "is not something that happens by accident", which is banned outright, and do not reach for any single stock closing formula: vary the ending so that two different hooks would never close the same way.
12. Compliment the business, not the outreach. Never frame the value as the company being "worth reaching out to", "worth my attention", or similar, because that centers the sender rather than the recipient. Praise what the recipient built, how long it has lasted, the region it serves, or the niche it holds.
13. Founder vs non founder. Do NOT imply the recipient personally founded, started, or "built from the ground up" a company they did not, and do NOT credit them with the full history of a business older than their own tenure. If the recipient's title is CFO, VP, Principal, Partner, Managing Director, or a President or CEO who is clearly not the founder, or if the company's founding year long predates the recipient, credit the COMPANY's story rather than the person's, and describe what the company has done rather than what the recipient built. Reserve "you built" and "from the ground up" language for confirmed founders.
14. Do NOT name specific individuals (founders, family members, predecessors) unless their name and role are confirmed in the scanner data. Refer to "the founders" or "the family" rather than guessing names. Do NOT assert personal backstory (for example "after losing it all", a specific immigrant journey, a career origin) unless it is confirmed; if it is only inferred, keep the line to verifiable facts about the company.
15. Write like an owner, not a marketer. State observations supported by evidence, prefer operational facts over praise, and let the facts imply quality rather than explaining it. Sound like someone who read the company's website and understood the business, not someone reviewing a resume or copying marketing copy. Good examples: "Remaining focused on precision machining for nearly forty years is uncommon." / "Serving the same customer base for generations says a great deal about the business." Avoid examples: "You have built an impressive company." / "Your award winning innovation and cutting edge solutions really stood out." / "Remaining independent for forty years demonstrates discipline and commitment." (the last one over explains, state the fact and stop).
16. Never speculate about motives, values, or character. State only verifiable facts about the recipient or company; do not infer why they do things or what they care about. Good example: "Many of your employees have been with the company for more than twenty years." Avoid example: "You clearly value your employees."
17. Never assert or embellish facts about the SENDER. The sender's verified attributes are exactly these and nothing more: Army veteran; CFO of a large California public agency; first generation immigrant; endurance athlete; single parent upbringing; California university education. The sender has NEVER founded, built, or run a company, so never write or imply otherwise. Do NOT invent sender biography (avoid examples: "as the son of an immigrant", "someone who built something from nothing", "having grown a business myself"). Do NOT state why the sender is writing, that the sender is reaching out, or what the sender is seeking; the body of the email does all of that.
18. Vary the opening construction across contacts. Do NOT default to a gerund ("Building a...", "Running a...", "Starting a..."), which becomes obvious when many hooks are read together. Rotate among several natural openings: a focus or longevity observation ("Remaining focused on precision machining for nearly forty years..."), a service statement ("Serving Central Valley growers since the 1980s..."), a direct company statement ("San Joaquin Electric has been doing commercial and industrial electrical work since 1980..."), a time anchor ("Nearly four decades after the first shop opened..."), or a single specific operational fact. Choose whichever opening lets the strongest fact land first."""

@app.after_request
def allow_iframe(response):
    response.headers['X-Frame-Options'] = 'ALLOWALL'
    response.headers['Content-Security-Policy'] = "frame-ancestors *"
    return response

HUBSPOT_TOKEN = os.environ.get('HUBSPOT_TOKEN', '')
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_KEY', '')
HUBSPOT_BASE = 'https://api.hubspot.com'
ANTHROPIC_BASE = 'https://api.anthropic.com'

JOBS = {}
JOBS_LOCK = threading.Lock()

def hs_headers():
    return {'Authorization': f'Bearer {HUBSPOT_TOKEN}', 'Content-Type': 'application/json'}

def anthropic_headers():
    return {'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json'}

@app.route('/')
def index():
    return send_from_directory(APP_DIR, 'index.html')

@app.route('/api/messages', methods=['POST'])
def proxy():
    try:
        api_key = request.headers.get('x-api-key', '') or ANTHROPIC_KEY
        data = request.get_json(force=True)
        resp = requests.post(
            f'{ANTHROPIC_BASE}/v1/messages',
            headers={'x-api-key': api_key, 'anthropic-version': '2023-06-01', 'content-type': 'application/json'},
            json=data, timeout=300
        )
        return Response(resp.content, status=resp.status_code, content_type=resp.headers.get('content-type', 'application/json'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def _fetch_contacts(batch_id, full=False):
    base_props = [
        'firstname', 'lastname', 'jobtitle', 'company', 'email', 'hs_linkedinid',
        'industry', 'city', 'state', 'annualrevenue', 'founded_year', 'description',
        'website', 'scanner_track', 'scanner_score', 'scanner_hook', 'neverbounce_status'
    ]
    extra_props = [
        'scanner_recommendation', 'scanner_track_reason', 'scanner_connections',
        'scanner_override', 'scanner_override_note', 'scanner_notes'
    ] if full else []
    props = base_props + extra_props
    contacts = []
    raw_contacts = []
    after = None
    while True:
        payload = {
            'filterGroups': [{'filters': [{'propertyName': 'grata_batch', 'operator': 'EQ', 'value': batch_id}]}],
            'properties': props + ['associatedcompanyid'],
            'limit': 100
        }
        if after:
            payload['after'] = after
        resp = requests.post(f'{HUBSPOT_BASE}/crm/v3/objects/contacts/search', headers=hs_headers(), json=payload, timeout=30)
        if not resp.ok:
            raise Exception(f'HubSpot {resp.status_code}: {resp.text[:200]}')
        data = resp.json()
        raw_contacts.extend(data.get('results', []))
        nxt = data.get('paging', {}).get('next', {}).get('after')
        if nxt:
            after = nxt
        else:
            break

    company_id_to_name = {}
    co_ids = list(set(
        c.get('properties', {}).get('associatedcompanyid', '')
        for c in raw_contacts
        if c.get('properties', {}).get('associatedcompanyid', '')
    ))
    company_id_to_phone = {}
    for i in range(0, len(co_ids), 100):
        batch = co_ids[i:i+100]
        co_resp = requests.post(f'{HUBSPOT_BASE}/crm/v3/objects/companies/batch/read',
            headers=hs_headers(),
            json={'inputs': [{'id': cid} for cid in batch], 'properties': ['name', 'phone']},
            timeout=20)
        if co_resp.ok:
            for co in co_resp.json().get('results', []):
                company_id_to_name[co['id']] = co.get('properties', {}).get('name', '')
                company_id_to_phone[co['id']] = co.get('properties', {}).get('phone', '')

    for c in raw_contacts:
        p = c.get('properties', {})
        loc = ', '.join(x for x in [p.get('city', ''), p.get('state', '')] if x)
        co_id = p.get('associatedcompanyid', '')
        company_name = p.get('company', '') or company_id_to_name.get(co_id, '')
        entry = {
            'hubspot_id': c['id'],
            'firstName': p.get('firstname', ''),
            'lastName': p.get('lastname', ''),
            'jobTitle': p.get('jobtitle', ''),
            'company': company_name,
            'email': p.get('email', ''),
            'phone': company_id_to_phone.get(co_id, ''),
            'linkedin': p.get('hs_linkedinid', ''),
            'industry': p.get('industry', ''),
            'location': loc,
            'revenue': p.get('annualrevenue', ''),
            'founded': p.get('founded_year', ''),
            'description': p.get('description', ''),
            'website': p.get('website', ''),
            'neverbounce': p.get('neverbounce_status', ''),
            'existing_track': p.get('scanner_track', ''),
            'existing_score': p.get('scanner_score', ''),
            'existing_hook': p.get('scanner_hook', ''),
        }
        if full:
            entry.update({
                'existing_recommendation': p.get('scanner_recommendation', ''),
                'existing_track_reason': p.get('scanner_track_reason', ''),
                'existing_connections': p.get('scanner_connections', ''),
                'existing_override': p.get('scanner_override', ''),
                'existing_override_note': p.get('scanner_override_note', ''),
                'existing_notes': p.get('scanner_notes', ''),
            })
        # Contacts without email are still included — screening only needs company/owner
        # background, not an email address. Email is only required if you go on to push
        # to Reply.io, which is a separate, explicit step.
        contacts.append(entry)
    return contacts

@app.route('/api/batch-contacts', methods=['GET'])
def batch_contacts():
    batch_id = request.args.get('batch_id', '').strip()
    if not batch_id:
        return jsonify({'error': 'batch_id required'}), 400
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    try:
        contacts = _fetch_contacts(batch_id, full=True)
        return jsonify({'contacts': contacts, 'count': len(contacts)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def _build_hs_props(result):
    connections = result.get('connections', [])
    conn_json, conn_summary = '', ''
    if connections:
        conn_json = json.dumps(connections)[:65000]
        parts = [f"{c.get('basis','')}/{c.get('strength','')}: {c.get('type','')}" for c in connections if c.get('type')]
        conn_summary = ' | '.join(parts)[:65000]
    props = {
        'scanner_track': result.get('track', ''),
        'scanner_score': str(result.get('score', '')),
        'scanner_hook': (result.get('hook', '') or '')[:65000],
        'scanner_recommendation': (result.get('recommendation', '') or '')[:65000],
        'scanner_track_reason': (result.get('track_reason', '') or '')[:65000],
        'scanner_connections': conn_json,
        'scanner_connection_summary': conn_summary,
        'scanner_override': 'true' if result.get('override') else '',
        'scanner_override_note': (result.get('override_note', '') or '')[:65000],
        'scanner_notes': (result.get('notes', '') or '')[:65000],
    }
    return {k: v for k, v in props.items() if v}

def _write_to_hs(hubspot_id, result):
    props = _build_hs_props(result)
    resp = requests.patch(f'{HUBSPOT_BASE}/crm/v3/objects/contacts/{hubspot_id}',
        headers=hs_headers(), json={'properties': props}, timeout=15)
    return resp.ok, (None if resp.ok else f"HTTP {resp.status_code}: {resp.text[:200]}")

def _build_prompt(contact, criteria):
    name = ' '.join(filter(None, [contact.get('firstName', ''), contact.get('lastName', '')])) or 'Unknown owner'
    has_full = bool(contact.get('firstName') and contact.get('lastName'))
    threshold = criteria.get('threshold', 40)
    profile = criteria.get('profile', {})
    weights = criteria.get('weights', [])
    feedback = criteria.get('feedback', [])
    search2_terms = criteria.get('search2_terms', '')
    # Hook rules are defined by the HOOK_RULES constant in this file, which is the
    # single source of truth. The Criteria-page box no longer overrides it; it is
    # only used if HOOK_RULES is somehow empty. Edit HOOK_RULES in this file to change hooks.
    hook_rules = HOOK_RULES or (criteria.get('hook_rules') or '').strip()
    wlabels = ['', 'Low', 'Medium', 'High']
    weights_text = '\n'.join(
        f"- {w['label']}: {wlabels[min(w.get('weight', 1), 3)]}"
        for w in weights if w.get('label', '').strip()
    )
    profile_parts = [f"{k.title()}: {v}" for k, v in profile.items() if v]
    profile_text = ("Steven Pavlov's profile:\n" + '\n'.join(profile_parts)) if profile_parts else \
        "Steven Pavlov is a Sacramento-area acquisition entrepreneur who buys and operates small businesses."
    feedback_text = ('\nCalibration notes from previous runs:\n' + '\n'.join(f"- {f}" for f in feedback)) if feedback else ''
    search2_line = (
        f'2. "{name} {contact.get("company", "")} {search2_terms}" — targeted at personal connection signals. Only run if you have both first AND last name.'
        if has_full else
        '(Search 2 skipped — no full owner name. Score on search 1 and Grata data.)'
    )
    prompt = f"""You are helping Steven Pavlov screen acquisition targets. Analyze the connection between Steven's background and this owner/company.

{profile_text}

Contact:
- Name: {name} ({contact.get('jobTitle', '')})
- Company: {contact.get('company', '')}
- Industry: {contact.get('industry', '')}
- Location: {contact.get('location', '')}
- Description: {contact.get('description', '')}
- LinkedIn: {contact.get('linkedin', '')}
- Revenue: {contact.get('revenue', '')}
- Founded: {contact.get('founded', '')}

Respond ONLY with raw JSON (no markdown):
{{"score":<0-100>,"track":"Personal Outreach" or "Standard Sequence","track_reason":"<1 sentence>","connections":[{{"strength":"strong|moderate|weak","basis":"confirmed|inferred","type":"","description":"","sourceUrl":""}}],"recommendation":"<1-2 sentences>","hook":"<ALWAYS write a hook for every contact, regardless of track. Follow the HOOK RULES below. Step 1 is identical for Personal Outreach and Standard Sequence, so the same hook applies to both>","industryCluster":"<1-3 word group>"}}

Rules: score>{threshold} = Personal Outreach. basis="confirmed" means direct evidence found; "inferred" means reasoning from signals. Only include real connection points. Never hallucinate URLs.

Connection signal weights (1=low bonus, 2=medium, 3=strong upgrade trigger):
{weights_text}
{feedback_text}

Run web searches before scoring:
1. "{name} {contact.get('company', '')}" — general background and owner bio
{search2_line}

Pay special attention to: California roots (Sacramento, Antelope Valley, San Fernando Valley), military/veteran background, immigrant background, bootstrapped origin story.

{hook_rules}"""
    return prompt, has_full

def _parse_result(text):
    text = text.strip()
    if text.startswith('```'):
        lines = text.split('\n')
        text = '\n'.join(lines[1:-1] if lines[-1].strip() == '```' else lines[1:])
    start, end = text.find('{'), text.rfind('}')
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end+1])
        except json.JSONDecodeError:
            pass
    return None

def _screen_one(contact, criteria):
    prompt, has_name = _build_prompt(contact, criteria)
    body = {
        'model': SCREENING_MODEL,
        'max_tokens': 1200,
        'messages': [{'role': 'user', 'content': prompt}]
    }
    if has_name:
        body['tools'] = [{'type': 'web_search_20250305', 'name': 'web_search'}]
    for attempt in range(3):
        resp = requests.post(f'{ANTHROPIC_BASE}/v1/messages', headers=anthropic_headers(), json=body, timeout=180)
        if resp.status_code == 429:
            time.sleep((attempt + 1) * 30)
            continue
        if not resp.ok:
            raise Exception(f"Anthropic {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if data.get('error'):
            raise Exception(data['error'].get('message', 'API error'))
        usage = data.get('usage', {})
        text = '\n'.join(b['text'] for b in data.get('content', []) if b.get('type') == 'text')
        result = _parse_result(text)
        if result:
            result['_tokens'] = {'input': usage.get('input_tokens', 0), 'output': usage.get('output_tokens', 0)}
            return result
        raise Exception(f"JSON parse failed: {text[:300]}")
    raise Exception("All attempts failed")

def _run_job(job_id, contacts, criteria, sync_to_hs=True):
    with JOBS_LOCK:
        JOBS[job_id]['status'] = 'running'
        JOBS[job_id]['heartbeat'] = time.time()
    processed, errors = 0, []
    for i, contact in enumerate(contacts):
        # Heartbeat: proves this thread is alive. screen_batch reads this to tell
        # a live job from a corpse left behind by a dyno restart (the 409 loop bug).
        with JOBS_LOCK:
            JOBS[job_id]['heartbeat'] = time.time()
        if contact.get('existing_track') and contact.get('existing_score'):
            with JOBS_LOCK:
                JOBS[job_id]['skipped'] = JOBS[job_id].get('skipped', 0) + 1
            continue
        name = f"{contact.get('firstName', '')} {contact.get('lastName', '')} @ {contact.get('company', '')}".strip()
        with JOBS_LOCK:
            JOBS[job_id]['current_contact'] = name
            JOBS[job_id]['current_index'] = i
        try:
            t_start = time.time()
            result = _screen_one(contact, criteria)
            duration_ms = int((time.time() - t_start) * 1000)
            # Scoring-only mode: skip the write-back to HubSpot entirely. The score
            # still lands in JOBS[job_id]['results'] below so it shows in the UI.
            if sync_to_hs:
                ok, err = _write_to_hs(contact['hubspot_id'], result)
                if not ok:
                    errors.append({'contact': name, 'error': f'HubSpot: {err}'})
            tok = result.pop('_tokens', {})
            tok_in = tok.get('input', 0)
            tok_out = tok.get('output', 0)
            contact_cost = round((tok_in * 3 / 1_000_000) + (tok_out * 15 / 1_000_000), 5)
            processed += 1
            with JOBS_LOCK:
                JOBS[job_id]['processed'] = processed
                JOBS[job_id]['heartbeat'] = time.time()
                JOBS[job_id]['tokens_input'] += tok_in
                JOBS[job_id]['tokens_output'] += tok_out
                JOBS[job_id]['results'][contact['hubspot_id']] = result
                JOBS[job_id]['contact_meta'][contact['hubspot_id']] = {
                    'tokens_input': tok_in, 'tokens_output': tok_out,
                    'cost': contact_cost, 'duration_ms': duration_ms,
                    'stop_reason': 'end_turn', 'used_tools': True
                }
        except Exception as e:
            errors.append({'contact': name, 'error': str(e)})
            processed += 1
            with JOBS_LOCK:
                JOBS[job_id]['processed'] = processed
                JOBS[job_id]['heartbeat'] = time.time()
        with JOBS_LOCK:
            JOBS[job_id]['errors'] = errors
        if i < len(contacts) - 1:
            time.sleep(2)
        # Pause support — wait here until unpaused, keeping the heartbeat fresh
        # so a paused job is never mistaken for a dead one.
        while JOBS.get(job_id, {}).get('paused'):
            with JOBS_LOCK:
                JOBS[job_id]['heartbeat'] = time.time()
            time.sleep(1)
    with JOBS_LOCK:
        JOBS[job_id].update({
            'status': 'done', 'done': True, 'errors': errors,
            'processed': processed, 'current_contact': None,
            'heartbeat': time.time(), 'finished_at': time.time()
        })

# ─── HOOK-ONLY REGENERATION ───────────────────────────────────────────────────
# Regenerates just the {{hook}} for a contact, preserving the existing score,
# track, and connections. Used by the "Regenerate Hooks" button so the current
# HOOK_RULES can be re-applied to a whole batch without re-scoring everything.
def _build_hook_prompt(contact, criteria):
    name = ' '.join(filter(None, [contact.get('firstName', ''), contact.get('lastName', '')])) or 'Unknown owner'
    has_full = bool(contact.get('firstName') and contact.get('lastName'))
    # HOOK_RULES in this file is the single source of truth (see _build_prompt).
    hook_rules = HOOK_RULES or (criteria.get('hook_rules') or '').strip()
    ctx_bits = []
    if contact.get('existing_track_reason'):
        ctx_bits.append(f"Acquisition fit rationale (NEVER state or allude to this in the hook): {contact['existing_track_reason']}")
    if contact.get('existing_connections'):
        ctx_bits.append(f"Mixed fit signals and possible personal overlaps (classify each per the guidance above): {contact['existing_connections']}")
    ctx = '\n'.join(ctx_bits) if ctx_bits else 'No prior scanner data on file.'
    prompt = f"""You are writing ONE outreach email hook for Steven Pavlov, a Sacramento-area acquisition entrepreneur who buys and operates small businesses. Write ONLY the hook text, following the HOOK RULES exactly. The same hook is used for both the Personal Outreach and Standard Sequence tracks.

Contact:
- Name: {name} ({contact.get('jobTitle', '')})
- Company: {contact.get('company', '')}
- Industry: {contact.get('industry', '')}
- Location: {contact.get('location', '')}
- Description: {contact.get('description', '')}
- LinkedIn: {contact.get('linkedin', '')}
- Founded: {contact.get('founded', '')}

Prior scanner findings. READ THIS CAREFULLY BEFORE USING THEM:
These fields explain why the company scored well against the sender's ACQUISITION CRITERIA. Most entries (bootstrapped or built from nothing origin, long tenure, family ownership, succession or retirement signals, owner operator status, California location, government or public sector client base) are reasons the SENDER WANTS TO BUY the company. They are NOT shared traits and NOT reasons to state in the hook. Never write that the company is what the sender is looking for, and never state why the sender is writing.
Only these count as genuine personal overlaps with the sender, and only when clearly verified: first generation immigrant background, endurance athletics, single parent upbringing, California university education. These may be named directly, per rules 6 and 7.
Everything else is factual grounding about the business only. Never fabricate. Most hooks should simply be one specific observation about the business.
{ctx}

Respond ONLY with raw JSON (no markdown): {{"hook":"<the hook text>"}}

{hook_rules}"""
    return prompt, has_full

def _regen_hook_one(contact, criteria):
    prompt, has_name = _build_hook_prompt(contact, criteria)
    body = {'model': SCREENING_MODEL, 'max_tokens': 600, 'messages': [{'role': 'user', 'content': prompt}]}
    if has_name:
        body['tools'] = [{'type': 'web_search_20250305', 'name': 'web_search'}]
    for attempt in range(3):
        resp = requests.post(f'{ANTHROPIC_BASE}/v1/messages', headers=anthropic_headers(), json=body, timeout=180)
        if resp.status_code == 429:
            time.sleep((attempt + 1) * 30)
            continue
        if not resp.ok:
            raise Exception(f"Anthropic {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if data.get('error'):
            raise Exception(data['error'].get('message', 'API error'))
        usage = data.get('usage', {})
        text = '\n'.join(b['text'] for b in data.get('content', []) if b.get('type') == 'text')
        result = _parse_result(text)
        if result and result.get('hook'):
            return {'hook': result['hook'], '_tokens': {'input': usage.get('input_tokens', 0), 'output': usage.get('output_tokens', 0)}}
        raise Exception(f"Hook parse failed: {text[:300]}")
    raise Exception("All attempts failed")

def _write_hook_to_hs(hubspot_id, hook):
    props = {'scanner_hook': (hook or '')[:65000]}
    resp = requests.patch(f'{HUBSPOT_BASE}/crm/v3/objects/contacts/{hubspot_id}',
                          headers=hs_headers(), json={'properties': props}, timeout=15)
    return resp.ok, (None if resp.ok else f"HTTP {resp.status_code}: {resp.text[:200]}")

def _run_hook_job(job_id, contacts, criteria, sync_to_hs=True):
    with JOBS_LOCK:
        JOBS[job_id]['status'] = 'running'
        JOBS[job_id]['heartbeat'] = time.time()
    processed, errors = 0, []
    for i, contact in enumerate(contacts):
        with JOBS_LOCK:
            JOBS[job_id]['heartbeat'] = time.time()
        name = f"{contact.get('firstName', '')} {contact.get('lastName', '')} @ {contact.get('company', '')}".strip()
        with JOBS_LOCK:
            JOBS[job_id]['current_contact'] = name
            JOBS[job_id]['current_index'] = i
        try:
            t_start = time.time()
            result = _regen_hook_one(contact, criteria)
            duration_ms = int((time.time() - t_start) * 1000)
            # Scoring-only mode: skip the write-back to HubSpot entirely.
            if sync_to_hs:
                ok, err = _write_hook_to_hs(contact['hubspot_id'], result.get('hook', ''))
                if not ok:
                    errors.append({'contact': name, 'error': f'HubSpot: {err}'})
            tok = result.get('_tokens', {})
            tok_in, tok_out = tok.get('input', 0), tok.get('output', 0)
            contact_cost = round((tok_in * 3 / 1_000_000) + (tok_out * 15 / 1_000_000), 5)
            processed += 1
            with JOBS_LOCK:
                JOBS[job_id]['processed'] = processed
                JOBS[job_id]['heartbeat'] = time.time()
                JOBS[job_id]['tokens_input'] += tok_in
                JOBS[job_id]['tokens_output'] += tok_out
                JOBS[job_id]['results'][contact['hubspot_id']] = {'hook': result.get('hook', '')}
                JOBS[job_id]['contact_meta'][contact['hubspot_id']] = {
                    'tokens_input': tok_in, 'tokens_output': tok_out,
                    'cost': contact_cost, 'duration_ms': duration_ms,
                    'stop_reason': 'end_turn', 'used_tools': True
                }
        except Exception as e:
            errors.append({'contact': name, 'error': str(e)})
            processed += 1
            with JOBS_LOCK:
                JOBS[job_id]['processed'] = processed
                JOBS[job_id]['heartbeat'] = time.time()
        with JOBS_LOCK:
            JOBS[job_id]['errors'] = errors
        if i < len(contacts) - 1:
            time.sleep(2)
        while JOBS.get(job_id, {}).get('paused'):
            with JOBS_LOCK:
                JOBS[job_id]['heartbeat'] = time.time()
            time.sleep(1)
    with JOBS_LOCK:
        JOBS[job_id].update({
            'status': 'done', 'done': True, 'errors': errors,
            'processed': processed, 'current_contact': None,
            'heartbeat': time.time(), 'finished_at': time.time()
        })

@app.route('/api/regen-hooks', methods=['POST'])
def regen_hooks():
    if not ANTHROPIC_KEY:
        return jsonify({'error': 'ANTHROPIC_KEY not configured'}), 500
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    data = request.get_json(force=True)
    batch_id = data.get('batch_id', '').strip()
    criteria = data.get('criteria', {})
    force = bool(data.get('force'))
    sync_to_hs = bool(data.get('sync_to_hubspot', True))
    if not batch_id:
        return jsonify({'error': 'batch_id required'}), 400
    # Same live/stale guard as screen-batch so a regen can't collide with a running job.
    with JOBS_LOCK:
        for jid, job in list(JOBS.items()):
            if job.get('batch_id') == batch_id and not job.get('done'):
                hb_age = time.time() - job.get('heartbeat', job.get('started_at', 0))
                is_dead = hb_age > 90
                is_alive = hb_age <= 20
                if (force and not is_alive) or is_dead:
                    JOBS[jid]['done'] = True
                    JOBS[jid]['status'] = 'stale'
                    continue
                return jsonify({'error': 'Job already running', 'job_id': jid, 'hb_age': round(hb_age, 1)}), 409
    try:
        # full=True so the hook prompt can be grounded in existing connections/track_reason.
        contacts = _fetch_contacts(batch_id, full=True)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch contacts: {e}'}), 500
    # Regenerate the hook for EVERY contact in the batch, regardless of track or score,
    # unless an explicit subset of hubspot_ids is supplied (targeted repair of specific hooks).
    subset = data.get('hubspot_ids') or []
    if subset:
        wanted = {str(x).strip() for x in subset}
        contacts = [c for c in contacts if str(c.get('hubspot_id')) in wanted]
        if not contacts:
            return jsonify({'error': 'None of the supplied hubspot_ids were found in this batch'}), 400
    job_id = str(uuid.uuid4())[:8]
    with JOBS_LOCK:
        JOBS[job_id] = {
            'paused': False, 'mode': 'hooks',
            'job_id': job_id, 'batch_id': batch_id, 'status': 'queued',
            'total': len(contacts), 'total_contacts': len(contacts), 'processed': 0,
            'skipped': 0, 'current_contact': None, 'current_index': 0,
            'errors': [], 'results': {}, 'done': False, 'started_at': time.time(),
            'heartbeat': time.time(), 'finished_at': None, 'sync_to_hs': sync_to_hs,
            'tokens_input': 0, 'tokens_output': 0, 'contact_meta': {}
        }
    threading.Thread(target=_run_hook_job, args=(job_id, contacts, criteria, sync_to_hs), daemon=True).start()
    return jsonify({'ok': True, 'job_id': job_id, 'total': len(contacts),
                    'already_screened': 0, 'total_contacts': len(contacts), 'mode': 'hooks',
                    'sync_to_hubspot': sync_to_hs})

@app.route('/api/pause-job', methods=['POST'])
def pause_job():
    data = request.get_json(force=True)
    job_id = data.get('job_id')
    paused = data.get('paused', True)
    with JOBS_LOCK:
        if job_id not in JOBS:
            return jsonify({'error': 'Job not found'}), 404
        JOBS[job_id]['paused'] = paused
    return jsonify({'ok': True, 'paused': paused})

@app.route('/api/screen-batch', methods=['POST'])
def screen_batch():
    if not ANTHROPIC_KEY:
        return jsonify({'error': 'ANTHROPIC_KEY not configured'}), 500
    data = request.get_json(force=True)
    batch_id = data.get('batch_id', '').strip()
    criteria = data.get('criteria', {})
    force = bool(data.get('force'))  # frontend can force-kill a stuck job and start fresh
    # Local mode: contacts supplied directly in the request (e.g. from a CSV import)
    # instead of a HubSpot batch_id. No HubSpot read OR write happens on this path —
    # sync_to_hubspot is forced off regardless of what's sent, since these contacts
    # were never in HubSpot and have no real hubspot_id to write back to.
    local_contacts = data.get('contacts')
    is_local = bool(local_contacts)
    # Scoring-only mode: when False, contacts are still fetched from HubSpot (that's
    # the data source) but scores are never written back — nothing leaves this app.
    sync_to_hs = False if is_local else bool(data.get('sync_to_hubspot', True))
    if not is_local and not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    if not batch_id:
        if is_local:
            # Local runs don't need a real HubSpot batch — synthesize a label purely
            # for job tracking (the 409/stale-job guard below keys off batch_id).
            batch_id = f'local-{uuid.uuid4().hex[:8]}'
        else:
            return jsonify({'error': 'batch_id required'}), 400
    with JOBS_LOCK:
        for jid, job in list(JOBS.items()):
            if job.get('batch_id') == batch_id and not job.get('done'):
                # A live worker thread updates 'heartbeat' on every loop iteration.
                # If the heartbeat is stale, the thread is dead (e.g. dyno restart) —
                # the job is a corpse and must not block a new run.
                now = time.time()
                heartbeat = job.get('heartbeat', job.get('started_at', 0))
                hb_age = now - heartbeat
                DEAD_AFTER = 90  # seconds without a heartbeat = thread is gone
                is_dead = hb_age > DEAD_AFTER
                # Honor force-kill only if the job isn't clearly alive. A job with a
                # fresh heartbeat is genuinely running and must survive a stray reload.
                LIVE_GRACE = 20  # if heartbeat is younger than this, treat as definitely alive
                is_alive = hb_age <= LIVE_GRACE
                if (force and not is_alive) or is_dead:
                    JOBS[jid]['done'] = True
                    JOBS[jid]['status'] = 'stale'
                    reason = 'forced' if (force and not is_alive) else f'no heartbeat for {hb_age:.0f}s'
                    print(f'[screen-batch] Killing job {jid} ({reason}) — starting fresh')
                    continue
                return jsonify({'error': 'Job already running', 'job_id': jid, 'hb_age': round(hb_age, 1)}), 409
    if is_local:
        # Give every local contact a stable synthetic id (reused by the frontend to
        # match results back to rows) since there's no real hubspot_id for these.
        contacts = []
        for i, c in enumerate(local_contacts):
            c = dict(c)
            c.setdefault('hubspot_id', f'local-{i}')
            contacts.append(c)
    else:
        try:
            contacts = _fetch_contacts(batch_id, full=False)
        except Exception as e:
            return jsonify({'error': f'Failed to fetch contacts: {e}'}), 500
    already = sum(1 for c in contacts if c.get('existing_track') and c.get('existing_score'))
    to_do = len(contacts) - already
    job_id = str(uuid.uuid4())[:8]
    with JOBS_LOCK:
        JOBS[job_id] = {
            'paused': False,
            'job_id': job_id, 'batch_id': batch_id, 'status': 'queued',
            'total': to_do, 'total_contacts': len(contacts), 'processed': 0,
            'skipped': already, 'current_contact': None, 'current_index': 0,
            'errors': [], 'results': {}, 'done': False, 'started_at': time.time(),
            'heartbeat': time.time(), 'sync_to_hs': sync_to_hs,
            'finished_at': None, 'tokens_input': 0, 'tokens_output': 0, 'contact_meta': {}
        }
    threading.Thread(target=_run_job, args=(job_id, contacts, criteria, sync_to_hs), daemon=True).start()
    return jsonify({'ok': True, 'job_id': job_id, 'total': to_do, 'already_screened': already,
                    'total_contacts': len(contacts), 'sync_to_hubspot': sync_to_hs})

@app.route('/api/screen-status/<job_id>', methods=['GET'])
def screen_status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({'error': 'Job not found'}), 404
    elapsed = int(time.time() - job['started_at'])
    hb_age = round(time.time() - job.get('heartbeat', job['started_at']), 1)
    is_dead = (not job['done']) and hb_age > 90
    pct = round((job['processed'] / job['total']) * 100) if job['total'] > 0 else 0
    eta = None
    if job['processed'] > 0 and not job['done'] and job['total'] > 0:
        rate = elapsed / job['processed']
        eta = int(rate * (job['total'] - job['processed']))
    return jsonify({
        'job_id': job['job_id'], 'batch_id': job['batch_id'], 'status': job['status'],
        'mode': job.get('mode', 'screen'), 'sync_to_hubspot': job.get('sync_to_hs', True),
        'total': job['total'], 'total_contacts': job.get('total_contacts', job['total']),
        'processed': job['processed'], 'skipped': job.get('skipped', 0), 'pct': pct,
        'current_contact': job.get('current_contact'), 'errors': job.get('errors', []),
        'error_count': len(job.get('errors', [])), 'done': job['done'],
        'elapsed_s': elapsed, 'eta_s': eta, 'hb_age': hb_age, 'dead': is_dead,
        'tokens_input': job.get('tokens_input', 0), 'tokens_output': job.get('tokens_output', 0),
        'cost': round((job.get('tokens_input', 0) * 3 / 1_000_000) + (job.get('tokens_output', 0) * 15 / 1_000_000), 4),
        'contact_meta': job.get('contact_meta', {}) if job.get('done') else {},
        # Only needed when sync_to_hs is off (nothing was written to HubSpot), so the
        # frontend can populate scores/hooks from the job itself instead of re-fetching
        # from HubSpot. Only sent once the job is done to keep polling responses small.
        'results': (job.get('results', {}) if (job.get('done') and not job.get('sync_to_hs', True)) else {})
    })

@app.route('/api/screen-jobs', methods=['GET'])
def screen_jobs():
    with JOBS_LOCK:
        jobs = [{'job_id': j['job_id'], 'batch_id': j['batch_id'], 'status': j['status'],
                 'total': j['total'], 'processed': j['processed'], 'done': j['done'],
                 'started_at': j['started_at']} for j in JOBS.values()]
    jobs.sort(key=lambda x: x['started_at'], reverse=True)
    return jsonify({'jobs': jobs[:20]})

@app.route('/api/write-results', methods=['POST'])
def write_results():
    data = request.get_json(force=True)
    results = data.get('results', [])
    if not results:
        return jsonify({'error': 'No results'}), 400
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    updated, failed, errors = 0, 0, []
    for r in results:
        hid = r.get('hubspot_id')
        if not hid or str(hid).startswith('local-'):
            failed += 1
            errors.append({'id': hid, 'error': 'Not a real HubSpot contact (locally-imported) — nothing to write'})
            continue
        ok, err = _write_to_hs(hid, r)
        if ok:
            updated += 1
        else:
            failed += 1
            errors.append({'id': hid, 'error': err})
    return jsonify({'ok': True, 'updated': updated, 'failed': failed, 'errors': errors})

@app.route('/api/write-contact', methods=['POST'])
def write_contact():
    data = request.get_json(force=True)
    hid = data.get('hubspot_id')
    if not hid:
        return jsonify({'error': 'hubspot_id required'}), 400
    if str(hid).startswith('local-'):
        return jsonify({'ok': False, 'error': 'Not a real HubSpot contact (locally-imported) — nothing to write'})
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    ok, err = _write_to_hs(hid, data)
    return jsonify({'ok': ok, 'error': err})

@app.route('/api/rollback', methods=['POST'])
def rollback():
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    data = request.get_json(force=True)
    batch_id = data.get('batch_id', '').strip()
    if not batch_id:
        return jsonify({'error': 'batch_id required'}), 400
    contact_ids = []
    after = None
    while True:
        payload = {
            'filterGroups': [{'filters': [{'propertyName': 'grata_batch', 'operator': 'EQ', 'value': batch_id}]}],
            'properties': ['firstname'], 'limit': 100
        }
        if after:
            payload['after'] = after
        resp = requests.post(f'{HUBSPOT_BASE}/crm/v3/objects/contacts/search',
                             headers=hs_headers(), json=payload, timeout=30)
        if not resp.ok:
            return jsonify({'error': f'HubSpot fetch failed: {resp.status_code}'}), 500
        d = resp.json()
        contact_ids.extend([c['id'] for c in d.get('results', [])])
        nxt = d.get('paging', {}).get('next', {}).get('after')
        if nxt:
            after = nxt
        else:
            break
    clear_props = {p: '' for p in [
        'scanner_track', 'scanner_score', 'scanner_hook', 'scanner_recommendation',
        'scanner_track_reason', 'scanner_connections', 'scanner_connection_summary',
        'scanner_override', 'scanner_override_note', 'scanner_notes'
    ]}
    cleared, failed = 0, []
    for cid in contact_ids:
        r = requests.patch(f'{HUBSPOT_BASE}/crm/v3/objects/contacts/{cid}',
                           headers=hs_headers(), json={'properties': clear_props}, timeout=15)
        if r.ok:
            cleared += 1
        else:
            failed.append(cid)
    return jsonify({'ok': True, 'cleared': cleared, 'failed': len(failed), 'total': len(contact_ids)})

@app.route('/ping')
def ping():
    return jsonify({'ok': True, 'ts': time.time()})

@app.route('/health')
def health():
    return jsonify({'ok': True, 'hubspot': bool(HUBSPOT_TOKEN), 'anthropic': bool(ANTHROPIC_KEY),
                    'model': SCREENING_MODEL,
                    'active_jobs': sum(1 for j in JOBS.values() if not j['done'])})

def _keepalive():
    """Ping self every 5 minutes so Render doesn't spin down during active use."""
    import urllib.request
    time.sleep(60)  # wait for server to fully start first
    while True:
        try:
            port = int(os.environ.get('PORT', 5000))
            urllib.request.urlopen(f'http://localhost:{port}/ping', timeout=10)
        except Exception:
            pass
        time.sleep(270)  # every 4.5 minutes

threading.Thread(target=_keepalive, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
