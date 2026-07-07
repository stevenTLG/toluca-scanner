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
1. Position: the hook is the FIRST line of the email, right after "Hi {FirstName},". It must grab attention in a single read and lead naturally into the next paragraph, which begins "I am looking to buy one company to own and operate for the next 20+ years." Do NOT greet and do NOT use the recipient's name.
2. Length: keep it tight, because the rest of Step 1 is already long. Default to one or two sentences. If there is a particularly strong, specific connection that needs the room, you may use a third sentence, use your best judgment. Never pad, and never more than three sentences.
3. No duplication with the rest of Step 1. The body already covers all of the following about the SENDER, so the hook must NOT state them: the intent to buy, acquire, or search for a company; owning and operating for the long term or for "20+ years"; being a long-term operator rather than a financial buyer or private equity; the sender's own Army veteran service; the sender's own role as a CFO of a California public agency; and the closing request for 15 to 20 minutes or to hear how they built the company. Do NOT propose a call, a meeting, a conversation, or "connecting" anywhere in the hook. (How to handle the RECIPIENT's background, including any overlap with the sender, is governed by rules 6 and 7.)
4. Do NOT name Toluca Lake Group, or any company, fund, or entity, for the sender.
5. Do NOT use dashes or em dashes of any kind ( - , -- , en dash, em dash ). Use commas instead.
6. You MAY reference the recipient's own background, even where it overlaps with the sender's, but do NOT state the sender's matching trait or claim the shared bond directly. The body already reveals that the sender is an Army veteran and a California public agency CFO, so for any military, veteran, finance, or public sector overlap, never write the connection from the sender's side. Forbidden examples: "I too am a veteran", "as a fellow veteran", "I also spent my career in public service", "we share a public sector background". You may instead note the recipient's own service or public sector background as a fact about them, and let the body do the connecting, since the reader will infer the shared ground on their own.
7. For genuine overlaps the body does NOT mention, you MAY draw the connection directly. The body covers only the sender's veteran service and public agency CFO role, nothing else about the sender. So other real overlaps the scanner identified, for example shared Sacramento or California roots, an immigrant or first generation story, a bootstrapped or built from nothing origin, endurance athletics, education, or a single parent upbringing, are fair to name directly in the hook, since the body will not surface them. Use only real, scanner identified overlaps. Do NOT force or stretch a connection. If nothing genuinely overlaps, write a concrete, respectful line about the recipient's business alone.
8. Be specific and verifiable. Open on something real about the recipient or the company (years in business, the region served, a niche, a recognizable detail) so the line could only have been written to this owner. Avoid generic flattery such as "impressive company" or "great work". Do NOT use placeholder nouns such as "something" or "a business" in place of the actual company: always name the company or state concretely what it does. Do NOT open with a generic aphorism, proverb, or observation about a category of people (for example "lifelong cyclists tend to understand..."); the first thing the reader sees must be about them or their company, not a general truism.
9. Tone: warm, plain, peer to peer, human, one future owner introducing himself to another. Match the voice of the email body. No buzzwords, no superlatives, no salesy language.
10. Format: start with a capital letter, end with a period, and return NO leading or trailing spaces. Plain text only.
11. Do NOT end the hook with, or place near its end, any phrase that describes the SENDER'S search or reaction, because the very next line begins "I am looking to buy one company..." and the echo reads as clumsy repetition. Banned closings include: "looking for", "what I have been looking for", "set out to find", "the kind of business/company I am interested in", "the type of business I am interested in", "drew me to reach out", "caught my attention", "made me want to reach out", "is exactly what I pay attention to". End instead on the merit of the company itself (for example "is not something that happens by accident", "is genuinely rare", "speaks for itself").
12. Compliment the business, not the outreach. Never frame the value as the company being "worth reaching out to", "worth my attention", or similar, because that centers the sender rather than the recipient. Praise what the recipient built, how long it has lasted, the region it serves, or the niche it holds.
13. Founder vs non founder. Do NOT imply the recipient personally founded, started, or "built from the ground up" a company they did not, and do NOT credit them with the full history of a business older than their own tenure. If the recipient's title is CFO, VP, Principal, Partner, Managing Director, or a President or CEO who is clearly not the founder, or if the company's founding year long predates the recipient, credit the COMPANY's story rather than the person's, and describe what the company has done rather than what the recipient built. Reserve "you built" and "from the ground up" language for confirmed founders.
14. Do NOT name specific individuals (founders, family members, predecessors) unless their name and role are confirmed in the scanner data. Refer to "the founders" or "the family" rather than guessing names. Do NOT assert personal backstory (for example "after losing it all", a specific immigrant journey, a career origin) unless it is confirmed; if it is only inferred, keep the line to verifiable facts about the company."""

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
    for i in range(0, len(co_ids), 100):
        batch = co_ids[i:i+100]
        co_resp = requests.post(f'{HUBSPOT_BASE}/crm/v3/objects/companies/batch/read',
            headers=hs_headers(),
            json={'inputs': [{'id': cid} for cid in batch], 'properties': ['name']},
            timeout=20)
        if co_resp.ok:
            for co in co_resp.json().get('results', []):
                company_id_to_name[co['id']] = co.get('properties', {}).get('name', '')

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
        # Skip contacts with no email — they can't be screened or pushed to Reply.io
        if not entry['email']:
            continue
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
    # Hook rules are editable from the Criteria page and arrive in the payload.
    # The HOOK_RULES constant above is only the default seed / fallback.
    hook_rules = (criteria.get('hook_rules') or '').strip() or HOOK_RULES
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

def _run_job(job_id, contacts, criteria):
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
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    data = request.get_json(force=True)
    batch_id = data.get('batch_id', '').strip()
    criteria = data.get('criteria', {})
    force = bool(data.get('force'))  # frontend can force-kill a stuck job and start fresh
    if not batch_id:
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
            'heartbeat': time.time(),
            'finished_at': None, 'tokens_input': 0, 'tokens_output': 0, 'contact_meta': {}
        }
    threading.Thread(target=_run_job, args=(job_id, contacts, criteria), daemon=True).start()
    return jsonify({'ok': True, 'job_id': job_id, 'total': to_do, 'already_screened': already, 'total_contacts': len(contacts)})

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
        'total': job['total'], 'total_contacts': job.get('total_contacts', job['total']),
        'processed': job['processed'], 'skipped': job.get('skipped', 0), 'pct': pct,
        'current_contact': job.get('current_contact'), 'errors': job.get('errors', []),
        'error_count': len(job.get('errors', [])), 'done': job['done'],
        'elapsed_s': elapsed, 'eta_s': eta, 'hb_age': hb_age, 'dead': is_dead,
        'tokens_input': job.get('tokens_input', 0), 'tokens_output': job.get('tokens_output', 0),
        'cost': round((job.get('tokens_input', 0) * 3 / 1_000_000) + (job.get('tokens_output', 0) * 15 / 1_000_000), 4),
        'contact_meta': job.get('contact_meta', {}) if job.get('done') else {}
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
        if not hid:
            failed += 1
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
