import os
import json
import time
import uuid
import threading
import requests
from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS

app = Flask(__name__, static_folder='.')
CORS(app)

@app.after_request
def allow_iframe(response):
    # Allow embedding from Toluca Tools
    response.headers['X-Frame-Options'] = 'ALLOWALL'
    response.headers['Content-Security-Policy'] = "frame-ancestors *"
    return response

HUBSPOT_TOKEN  = os.environ.get('HUBSPOT_TOKEN', '')
ANTHROPIC_KEY  = os.environ.get('ANTHROPIC_KEY', '')
HUBSPOT_BASE   = 'https://api.hubspot.com'
ANTHROPIC_BASE = 'https://api.anthropic.com'

# ── In-memory job store ───────────────────────────────────────────────────────
JOBS      = {}
JOBS_LOCK = threading.Lock()

def hs_headers():
    return {'Authorization': f'Bearer {HUBSPOT_TOKEN}', 'Content-Type': 'application/json'}

def anthropic_headers():
    return {'x-api-key': ANTHROPIC_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json'}

# ── Static ────────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

# ── Claude API proxy (browser-side fallback) ──────────────────────────────────
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
        return Response(resp.content, status=resp.status_code,
                        content_type=resp.headers.get('content-type', 'application/json'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── Fetch contacts from HubSpot ───────────────────────────────────────────────
def _fetch_contacts(batch_id, full=False):
    """Pull contacts for a batch. full=True fetches all scanner properties."""
    base_props = [
        'firstname', 'lastname', 'jobtitle', 'company', 'email',
        'hs_linkedinid', 'industry', 'city', 'state',
        'annualrevenue', 'founded_year', 'description', 'website',
        'scanner_track', 'scanner_score', 'scanner_hook', 'neverbounce_status'
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
            'properties': props + ['associatedcompanyid'], 'limit': 100
        }
        if after:
            payload['after'] = after
        resp = requests.post(f'{HUBSPOT_BASE}/crm/v3/objects/contacts/search',
                             headers=hs_headers(), json=payload, timeout=30)
        if not resp.ok:
            raise Exception(f'HubSpot {resp.status_code}: {resp.text[:200]}')
        data = resp.json()
        raw_contacts.extend(data.get('results', []))
        nxt = data.get('paging', {}).get('next', {}).get('after')
        if nxt: after = nxt
        else:   break

    # Batch-fetch company names for all associated companies
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
            loc = ', '.join(x for x in [p.get('city',''), p.get('state','')] if x)
            # Use associated company name if contact's own company field is blank
            co_id = p.get('associatedcompanyid', '')
            company_name = p.get('company', '') or company_id_to_name.get(co_id, '')
            entry = {
                'hubspot_id':     c['id'],
                'firstName':      p.get('firstname', ''),
                'lastName':       p.get('lastname', ''),
                'jobTitle':       p.get('jobtitle', ''),
                'company':        company_name,
                'email':          p.get('email', ''),
                'linkedin':       p.get('hs_linkedinid', ''),
                'industry':       p.get('industry', ''),
                'location':       loc,
                'revenue':        p.get('annualrevenue', ''),
                'founded':        p.get('founded_year', ''),
                'description':    p.get('description', ''),
                'website':        p.get('website', ''),
                'neverbounce':    p.get('neverbounce_status', ''),
                'existing_track': p.get('scanner_track', ''),
                'existing_score': p.get('scanner_score', ''),
                'existing_hook':  p.get('scanner_hook', ''),
            }
            if full:
                entry.update({
                    'existing_recommendation': p.get('scanner_recommendation', ''),
                    'existing_track_reason':   p.get('scanner_track_reason', ''),
                    'existing_connections':    p.get('scanner_connections', ''),
                    'existing_override':       p.get('scanner_override', ''),
                    'existing_override_note':  p.get('scanner_override_note', ''),
                    'existing_notes':          p.get('scanner_notes', ''),
                })
            contacts.append(entry)
    return contacts

@app.route('/api/batch-contacts', methods=['GET'])
def batch_contacts():
    batch_id = request.args.get('batch_id', '').strip()
    if not batch_id: return jsonify({'error': 'batch_id required'}), 400
    if not HUBSPOT_TOKEN: return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    try:
        contacts = _fetch_contacts(batch_id, full=True)
        return jsonify({'contacts': contacts, 'count': len(contacts)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── HubSpot write helpers ─────────────────────────────────────────────────────
def _build_hs_props(result):
    connections = result.get('connections', [])
    conn_json, conn_summary = '', ''
    if connections:
        conn_json = json.dumps(connections)[:65000]
        parts = [f"{c.get('basis','')}/{c.get('strength','')}: {c.get('type','')}"
                 for c in connections if c.get('type')]
        conn_summary = ' | '.join(parts)[:65000]
    props = {
        'scanner_track':              result.get('track', ''),
        'scanner_score':              str(result.get('score', '')),
        'scanner_hook':               (result.get('hook', '') or '')[:65000],
        'scanner_recommendation':     (result.get('recommendation', '') or '')[:65000],
        'scanner_track_reason':       (result.get('track_reason', '') or '')[:65000],
        'scanner_connections':        conn_json,
        'scanner_connection_summary': conn_summary,
        'scanner_override':           'true' if result.get('override') else '',
        'scanner_override_note':      (result.get('override_note', '') or '')[:65000],
        'scanner_notes':              (result.get('notes', '') or '')[:65000],
    }
    return {k: v for k, v in props.items() if v}

def _write_to_hs(hubspot_id, result):
    props = _build_hs_props(result)
    resp = requests.patch(f'{HUBSPOT_BASE}/crm/v3/objects/contacts/{hubspot_id}',
                          headers=hs_headers(), json={'properties': props}, timeout=15)
    return resp.ok, (None if resp.ok else f"HTTP {resp.status_code}: {resp.text[:200]}")

# ── Screening prompt builder ──────────────────────────────────────────────────
def _build_prompt(contact, criteria):
    name = ' '.join(filter(None, [contact.get('firstName',''), contact.get('lastName','')])) or 'Unknown owner'
    has_full = bool(contact.get('firstName') and contact.get('lastName'))
    threshold     = criteria.get('threshold', 40)
    profile       = criteria.get('profile', {})
    weights       = criteria.get('weights', [])
    feedback      = criteria.get('feedback', [])
    search2_terms = criteria.get('search2_terms', '')

    wlabels = ['','Low','Medium','High']
    weights_text = '\n'.join(
        f"- {w['label']}: {wlabels[min(w.get('weight',1),3)]}"
        for w in weights if w.get('label','').strip()
    )
    profile_parts = [f"{k.title()}: {v}" for k, v in profile.items() if v]
    profile_text = ("Steven Pavlov's profile:\n" + '\n'.join(profile_parts)) if profile_parts else \
        "Steven Pavlov is a Sacramento-area acquisition entrepreneur who buys and operates small businesses."
    feedback_text = ('\nCalibration notes from previous runs:\n' + '\n'.join(f"- {f}" for f in feedback)) if feedback else ''
    search2_line = (
        f'2. "{name} {contact.get("company","")} {search2_terms}" — targeted at personal connection signals. Only run if you have both first AND last name.'
        if has_full else
        '(Search 2 skipped — no full owner name. Score on search 1 and Grata data.)'
    )

    prompt = f"""You are helping Steven Pavlov screen acquisition targets. Analyze the connection between Steven's background and this owner/company.

{profile_text}

Contact:
- Name: {name} ({contact.get('jobTitle','')})
- Company: {contact.get('company','')}
- Industry: {contact.get('industry','')}
- Location: {contact.get('location','')}
- Description: {contact.get('description','')}
- LinkedIn: {contact.get('linkedin','')}
- Revenue: {contact.get('revenue','')}
- Founded: {contact.get('founded','')}

Respond ONLY with raw JSON (no markdown):
{{"score":<0-100>,"track":"Personal Outreach" or "Standard Sequence","track_reason":"<1 sentence>","connections":[{{"strength":"strong|moderate|weak","basis":"confirmed|inferred","type":"<short category>","description":"<one sentence>","sourceUrl":"<URL or empty string>"}}],"recommendation":"<1-2 sentences>","hook":"<2 sentence opener if Personal Outreach, else empty string>","industryCluster":"<1-3 word group>"}}

Rules: score>{threshold} = Personal Outreach. basis="confirmed" means direct evidence found; "inferred" means reasoning from signals. Only include real connection points. Never hallucinate URLs.

Connection signal weights (1=low bonus, 2=medium, 3=strong upgrade trigger):
{weights_text}
{feedback_text}

Run web searches before scoring:
1. "{name} {contact.get('company','')}" — general background and owner bio
{search2_line}

Pay special attention to: California roots (Sacramento, Antelope Valley, San Fernando Valley), military/veteran background, immigrant background, bootstrapped origin story."""

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
        'model': 'claude-sonnet-4-20250514',
        'max_tokens': 1200,
        'messages': [{'role': 'user', 'content': prompt}]
    }
    if has_name:
        body['tools'] = [{'type': 'web_search_20250305', 'name': 'web_search'}]

    for attempt in range(3):
        resp = requests.post(f'{ANTHROPIC_BASE}/v1/messages',
                             headers=anthropic_headers(), json=body, timeout=180)
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

# ── Background job runner ─────────────────────────────────────────────────────
def _run_job(job_id, contacts, criteria):
    with JOBS_LOCK:
        JOBS[job_id]['status'] = 'running'

    processed, errors = 0, []

    for i, contact in enumerate(contacts):
        # Skip already screened
        if contact.get('existing_track') and contact.get('existing_score'):
            with JOBS_LOCK:
                JOBS[job_id]['skipped'] = JOBS[job_id].get('skipped', 0) + 1
            continue

        name = f"{contact.get('firstName','')} {contact.get('lastName','')} @ {contact.get('company','')}".strip()
        with JOBS_LOCK:
            JOBS[job_id]['current_contact'] = name
            JOBS[job_id]['current_index']   = i

        try:
            t_start = time.time()
            result = _screen_one(contact, criteria)
            duration_ms = int((time.time() - t_start) * 1000)
            ok, err = _write_to_hs(contact['hubspot_id'], result)
            if not ok:
                errors.append({'contact': name, 'error': f'HubSpot: {err}'})
            tok = result.pop('_tokens', {})
            tok_in  = tok.get('input', 0)
            tok_out = tok.get('output', 0)
            contact_cost = round((tok_in * 3 / 1_000_000) + (tok_out * 15 / 1_000_000), 5)
            processed += 1
            with JOBS_LOCK:
                JOBS[job_id]['processed'] = processed
                JOBS[job_id]['tokens_input']  += tok_in
                JOBS[job_id]['tokens_output'] += tok_out
                JOBS[job_id]['results'][contact['hubspot_id']] = result
                JOBS[job_id]['contact_meta'][contact['hubspot_id']] = {
                    'tokens_input':  tok_in,
                    'tokens_output': tok_out,
                    'cost':          contact_cost,
                    'duration_ms':   duration_ms,
                    'stop_reason':   'end_turn',
                    'used_tools':    True
                }
        except Exception as e:
            errors.append({'contact': name, 'error': str(e)})
            processed += 1
            with JOBS_LOCK:
                JOBS[job_id]['processed'] = processed
                JOBS[job_id]['errors']    = errors

        if i < len(contacts) - 1:
            time.sleep(2)

    with JOBS_LOCK:
        JOBS[job_id].update({
            'status': 'done', 'done': True, 'errors': errors,
            'processed': processed, 'current_contact': None,
            'finished_at': time.time()
        })

# ── POST /api/screen-batch ────────────────────────────────────────────────────
@app.route('/api/screen-batch', methods=['POST'])
def screen_batch():
    if not ANTHROPIC_KEY:  return jsonify({'error': 'ANTHROPIC_KEY not configured'}), 500
    if not HUBSPOT_TOKEN:  return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500

    data     = request.get_json(force=True)
    batch_id = data.get('batch_id', '').strip()
    criteria  = data.get('criteria', {})
    if not batch_id: return jsonify({'error': 'batch_id required'}), 400

    # Reject if job already running for this batch
    with JOBS_LOCK:
        for jid, job in JOBS.items():
            if job.get('batch_id') == batch_id and not job.get('done'):
                return jsonify({'error': 'Job already running', 'job_id': jid}), 409

    try:
        contacts = _fetch_contacts(batch_id, full=False)
    except Exception as e:
        return jsonify({'error': f'Failed to fetch contacts: {e}'}), 500

    already = sum(1 for c in contacts if c.get('existing_track') and c.get('existing_score'))
    to_do   = len(contacts) - already

    job_id = str(uuid.uuid4())[:8]
    with JOBS_LOCK:
        JOBS[job_id] = {
            'job_id': job_id, 'batch_id': batch_id, 'status': 'queued',
            'total': to_do, 'total_contacts': len(contacts),
            'processed': 0, 'skipped': already,
            'current_contact': None, 'current_index': 0,
            'errors': [], 'results': {}, 'done': False,
            'started_at': time.time(), 'finished_at': None,
            'tokens_input': 0, 'tokens_output': 0,
            'contact_meta': {}
        }

    threading.Thread(target=_run_job, args=(job_id, contacts, criteria), daemon=True).start()

    return jsonify({'ok': True, 'job_id': job_id, 'total': to_do,
                    'already_screened': already, 'total_contacts': len(contacts)})

# ── GET /api/screen-status/<job_id> ──────────────────────────────────────────
@app.route('/api/screen-status/<job_id>', methods=['GET'])
def screen_status(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job: return jsonify({'error': 'Job not found'}), 404

    elapsed = int(time.time() - job['started_at'])
    pct     = round((job['processed'] / job['total']) * 100) if job['total'] > 0 else 0
    eta     = None
    if job['processed'] > 0 and not job['done'] and job['total'] > 0:
        rate = elapsed / job['processed']
        eta  = int(rate * (job['total'] - job['processed']))

    return jsonify({
        'job_id': job['job_id'], 'batch_id': job['batch_id'],
        'status': job['status'], 'total': job['total'],
        'total_contacts': job.get('total_contacts', job['total']),
        'processed': job['processed'], 'skipped': job.get('skipped', 0),
        'pct': pct, 'current_contact': job.get('current_contact'),
        'errors': job.get('errors', []), 'error_count': len(job.get('errors', [])),
        'done': job['done'], 'elapsed_s': elapsed, 'eta_s': eta,
        'tokens_input': job.get('tokens_input', 0),
        'tokens_output': job.get('tokens_output', 0),
        'cost': round((job.get('tokens_input',0) * 3 / 1_000_000) + (job.get('tokens_output',0) * 15 / 1_000_000), 4),
        'contact_meta': job.get('contact_meta', {}) if job.get('done') else {}
    })

# ── GET /api/screen-jobs ──────────────────────────────────────────────────────
@app.route('/api/screen-jobs', methods=['GET'])
def screen_jobs():
    with JOBS_LOCK:
        jobs = [{'job_id': j['job_id'], 'batch_id': j['batch_id'], 'status': j['status'],
                 'total': j['total'], 'processed': j['processed'], 'done': j['done'],
                 'started_at': j['started_at']} for j in JOBS.values()]
    jobs.sort(key=lambda x: x['started_at'], reverse=True)
    return jsonify({'jobs': jobs[:20]})

# ── POST /api/write-results (bulk) ───────────────────────────────────────────
@app.route('/api/write-results', methods=['POST'])
def write_results():
    data    = request.get_json(force=True)
    results = data.get('results', [])
    if not results:     return jsonify({'error': 'No results'}), 400
    if not HUBSPOT_TOKEN: return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    updated, failed, errors = 0, 0, []
    for r in results:
        hid = r.get('hubspot_id')
        if not hid: failed += 1; continue
        ok, err = _write_to_hs(hid, r)
        if ok: updated += 1
        else:  failed  += 1; errors.append({'id': hid, 'error': err})
    return jsonify({'ok': True, 'updated': updated, 'failed': failed, 'errors': errors})

# ── POST /api/write-contact (single auto-save) ───────────────────────────────
@app.route('/api/write-contact', methods=['POST'])
def write_contact():
    """Auto-save a single contact change immediately."""
    data = request.get_json(force=True)
    hid  = data.get('hubspot_id')
    if not hid:           return jsonify({'error': 'hubspot_id required'}), 400
    if not HUBSPOT_TOKEN: return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    ok, err = _write_to_hs(hid, data)
    return jsonify({'ok': ok, 'error': err})

# ── POST /api/push-replyio ────────────────────────────────────────────────────
@app.route('/api/push-replyio', methods=['POST'])
def push_replyio():
    REPLYIO_KEY = os.environ.get('REPLYIO_KEY', '')
    if not REPLYIO_KEY: return jsonify({'error': 'REPLYIO_KEY not configured'}), 500
    data         = request.get_json(force=True)
    contacts     = data.get('contacts', [])
    personal_seq = data.get('personal_sequence_id')
    standard_seq = data.get('standard_sequence_id')
    if not contacts: return jsonify({'error': 'No contacts'}), 400

    enrolled, failed, errors = 0, 0, []
    headers = {'x-api-key': REPLYIO_KEY, 'Content-Type': 'application/json'}
    debug_log = []
    for c in contacts:
        track  = c.get('track', 'Standard Sequence')
        seq_id = personal_seq if track == 'Personal Outreach' else standard_seq
        if not seq_id:
            failed += 1; errors.append({'email': c.get('email'), 'error': 'No sequence ID'}); continue
        email = c.get('email', '')
        hook  = c.get('hook', '')

        # Step 1: Create/update contact with hook variable
        person_payload = {
            'email': email,
            'firstName': c.get('firstName', ''),
            'lastName': c.get('lastName', ''),
            'variables': [{'name': 'hook', 'value': hook}]
        }
        p_resp = requests.post('https://api.reply.io/v1/people',
                      headers=headers, json=person_payload, timeout=15)
        debug_log.append({'step': 'create_person', 'email': email, 'status': p_resp.status_code, 'body': p_resp.text[:200]})

        # Step 2: Enroll via campaigns endpoint
        enroll_resp = requests.post(
            f'https://api.reply.io/v1/campaigns/{seq_id}/people',
            headers=headers,
            json={'email': email},
            timeout=15
        )
        debug_log.append({'step': 'enroll', 'email': email, 'seq_id': seq_id, 'status': enroll_resp.status_code, 'body': enroll_resp.text[:200]})

        if enroll_resp.ok:
            enrolled += 1
        else:
            failed += 1
            errors.append({'email': email, 'status': enroll_resp.status_code, 'body': enroll_resp.text[:200]})

    return jsonify({'ok': True, 'enrolled': enrolled, 'failed': failed, 'errors': errors, 'debug': debug_log})


# ── POST /api/rollback ───────────────────────────────────────────────────────
@app.route('/api/rollback', methods=['POST'])
def rollback():
    """Clear all scanner_* properties from every contact in a batch."""
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500
    data = request.get_json(force=True)
    batch_id = data.get('batch_id', '').strip()
    if not batch_id:
        return jsonify({'error': 'batch_id required'}), 400
    # Fetch all contact IDs for this batch
    contact_ids = []
    after = None
    while True:
        payload = {
            'filterGroups': [{'filters': [{'propertyName': 'grata_batch', 'operator': 'EQ', 'value': batch_id}]}],
            'properties': ['firstname'],
            'limit': 100
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
    # Clear scanner properties on each contact
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

# ── Health ────────────────────────────────────────────────────────────────────
@app.route('/health')
def health():
    return jsonify({'ok': True, 'hubspot': bool(HUBSPOT_TOKEN), 'anthropic': bool(ANTHROPIC_KEY),
                    'active_jobs': sum(1 for j in JOBS.values() if not j['done'])})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
