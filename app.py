import os
import requests
from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS

app = Flask(__name__, static_folder='.')
CORS(app)

HUBSPOT_TOKEN = os.environ.get('HUBSPOT_TOKEN', '')
HUBSPOT_BASE  = 'https://api.hubspot.com'

def hs_headers():
    return {
        'Authorization': f'Bearer {HUBSPOT_TOKEN}',
        'Content-Type': 'application/json'
    }

# ── Static ──────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

# ── Claude API proxy ─────────────────────────────────────────────────────────
@app.route('/api/messages', methods=['POST'])
def proxy():
    try:
        api_key = request.headers.get('x-api-key', '')
        data = request.get_json(force=True)
        resp = requests.post(
            'https://api.anthropic.com/v1/messages',
            headers={
                'x-api-key': api_key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json'
            },
            json=data,
            timeout=300
        )
        return Response(
            resp.content,
            status=resp.status_code,
            content_type=resp.headers.get('content-type', 'application/json')
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ── Fetch contacts from HubSpot by grata_batch ───────────────────────────────
@app.route('/api/batch-contacts', methods=['GET'])
def batch_contacts():
    """
    Pull all contacts from HubSpot tagged with a given grata_batch value.
    Returns them in the same shape the scanner expects.
    """
    batch_id = request.args.get('batch_id', '').strip()
    if not batch_id:
        return jsonify({'error': 'batch_id required'}), 400
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500

    contacts = []
    after = None

    while True:
        payload = {
            'filterGroups': [{
                'filters': [{
                    'propertyName': 'grata_batch',
                    'operator': 'EQ',
                    'value': batch_id
                }]
            }],
            'properties': [
                'firstname', 'lastname', 'jobtitle', 'company',
                'email', 'phone', 'linkedinbio', 'hs_linkedinid',
                'industry', 'city', 'state',
                'annualrevenue', 'founded_year',
                'description', 'website',
                'scanner_track', 'scanner_score', 'scanner_hook',
                'neverbounce_status'
            ],
            'limit': 100
        }
        if after:
            payload['after'] = after

        resp = requests.post(
            f'{HUBSPOT_BASE}/crm/v3/objects/contacts/search',
            headers=hs_headers(),
            json=payload,
            timeout=30
        )
        if not resp.ok:
            return jsonify({'error': f'HubSpot error {resp.status_code}', 'detail': resp.text}), 500

        data = resp.json()
        for c in data.get('results', []):
            p = c.get('properties', {})
            location_parts = [x for x in [p.get('city',''), p.get('state','')] if x]
            contacts.append({
                'hubspot_id':   c['id'],
                'firstName':    p.get('firstname', ''),
                'lastName':     p.get('lastname', ''),
                'jobTitle':     p.get('jobtitle', ''),
                'company':      p.get('company', ''),
                'email':        p.get('email', ''),
                'linkedin':     p.get('hs_linkedinid', ''),
                'industry':     p.get('industry', ''),
                'location':     ', '.join(location_parts),
                'revenue':      p.get('annualrevenue', ''),
                'founded':      p.get('founded_year', ''),
                'description':  p.get('description', ''),
                'website':      p.get('website', ''),
                'neverbounce':  p.get('neverbounce_status', ''),
                # Pre-populate existing scanner results if re-running
                'existing_track': p.get('scanner_track', ''),
                'existing_score': p.get('scanner_score', ''),
                'existing_hook':  p.get('scanner_hook', ''),
            })

        paging = data.get('paging', {})
        next_page = paging.get('next', {}).get('after')
        if next_page:
            after = next_page
        else:
            break

    return jsonify({'contacts': contacts, 'count': len(contacts)})

# ── Write scanner results back to HubSpot ────────────────────────────────────
@app.route('/api/write-results', methods=['POST'])
def write_results():
    """
    Write scanner results back to HubSpot contacts.
    Expects: { results: [{ hubspot_id, track, score, hook, recommendation, override }] }
    """
    data = request.get_json(force=True)
    results = data.get('results', [])
    if not results:
        return jsonify({'error': 'No results provided'}), 400
    if not HUBSPOT_TOKEN:
        return jsonify({'error': 'HUBSPOT_TOKEN not configured'}), 500

    updated = 0
    failed = 0
    errors = []

    for r in results:
        hubspot_id = r.get('hubspot_id')
        if not hubspot_id:
            failed += 1
            continue

        # Build connection summary and JSON blob
        connections = r.get('connections', [])
        conn_json = ''
        conn_summary = ''
        if connections:
            import json as _json
            conn_json = _json.dumps(connections)[:65000]
            parts = []
            for c in connections:
                basis    = c.get('basis', '')
                strength = c.get('strength', '')
                ctype    = c.get('type', '')
                if ctype:
                    parts.append(f"{basis}/{strength}: {ctype}")
            conn_summary = ' | '.join(parts)[:65000]

        props = {
            'scanner_track':              r.get('track', ''),
            'scanner_score':              str(r.get('score', '')),
            'scanner_hook':               r.get('hook', '')[:65000],
            'scanner_recommendation':     r.get('recommendation', '')[:65000],
            'scanner_track_reason':       r.get('track_reason', '')[:65000],
            'scanner_connections':        conn_json,
            'scanner_connection_summary': conn_summary,
            'scanner_override':           'true' if r.get('override') else '',
            'scanner_override_note':      r.get('override_note', '')[:65000],
            'scanner_notes':              r.get('notes', '')[:65000],
        }
        # Remove empty values — don't overwrite HubSpot fields with blanks
        props = {k: v for k, v in props.items() if v}

        resp = requests.patch(
            f'{HUBSPOT_BASE}/crm/v3/objects/contacts/{hubspot_id}',
            headers=hs_headers(),
            json={'properties': props},
            timeout=15
        )
        if resp.ok:
            updated += 1
        else:
            failed += 1
            errors.append({'id': hubspot_id, 'status': resp.status_code, 'detail': resp.text[:200]})

    return jsonify({'ok': True, 'updated': updated, 'failed': failed, 'errors': errors})

# ── Enroll contacts in Reply.io sequence ─────────────────────────────────────
@app.route('/api/push-replyio', methods=['POST'])
def push_replyio():
    """
    Enroll approved contacts in a Reply.io sequence.
    Expects: {
        contacts: [{ email, firstName, lastName, hook, track }],
        personal_sequence_id: <int>,
        standard_sequence_id: <int>
    }
    """
    REPLYIO_KEY = os.environ.get('REPLYIO_KEY', '')
    if not REPLYIO_KEY:
        return jsonify({'error': 'REPLYIO_KEY not configured'}), 500

    data = request.get_json(force=True)
    contacts = data.get('contacts', [])
    personal_seq = data.get('personal_sequence_id')
    standard_seq = data.get('standard_sequence_id')

    if not contacts:
        return jsonify({'error': 'No contacts provided'}), 400

    enrolled = 0
    failed = 0
    errors = []

    for c in contacts:
        track = c.get('track', 'Standard Sequence')
        seq_id = personal_seq if track == 'Personal Outreach' else standard_seq
        if not seq_id:
            failed += 1
            errors.append({'email': c.get('email'), 'error': 'No sequence ID for track'})
            continue

        # Reply.io add and push to sequence
        payload = {
            'email':     c.get('email', ''),
            'firstName': c.get('firstName', ''),
            'lastName':  c.get('lastName', ''),
            'sequenceId': seq_id,
            'variables': {
                'hook':  c.get('hook', ''),
                'track': track
            }
        }

        resp = requests.post(
            'https://api.reply.io/v1/people',
            headers={
                'x-api-key': REPLYIO_KEY,
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=15
        )
        if resp.ok:
            enrolled += 1
        else:
            failed += 1
            errors.append({'email': c.get('email'), 'status': resp.status_code, 'detail': resp.text[:200]})

    return jsonify({'ok': True, 'enrolled': enrolled, 'failed': failed, 'errors': errors})

# ── Health check ──────────────────────────────────────────────────────────────
@app.route('/health')
def health():
    return jsonify({'ok': True, 'hubspot': bool(HUBSPOT_TOKEN)})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
