#!/usr/bin/env python3
"""Check the running Compose telemetry path, including one server-to-worker trace."""

import argparse
import base64
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from smoke import png


def request(url, method='GET', payload=None, headers=None, expected=200):
    req = urllib.request.Request(url, data=payload, headers=headers or {}, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read()
        assert response.status == expected, (method, url, response.status, body[:500])
        return body


def read_json(url, **kwargs):
    return json.loads(request(url, **kwargs))


def eventually(description, check, timeout=90):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                print('PASS:', description)
                return result
        except (OSError, AssertionError, ValueError, KeyError) as error:
            last_error = error
        time.sleep(1)
    raise AssertionError(f'{description} did not pass within {timeout}s: {last_error}')


def main():
    env = {}
    if Path('.env').is_file():
        for line in Path('.env').read_text().splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                key, value = line.split('=', 1)
                env[key.strip()] = value.strip().strip('\"\'')

    def setting(key, default=''):
        return os.getenv(key, env.get(key, default))

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:' + setting('HTTP_PORT', '8080'))
    parser.add_argument('--prometheus', default='http://127.0.0.1:' + setting('PROMETHEUS_PORT', '9090'))
    parser.add_argument('--loki', default='http://127.0.0.1:' + setting('LOKI_PORT', '3100'))
    parser.add_argument('--jaeger', default='http://127.0.0.1:' + setting('JAEGER_PORT', '16686'))
    parser.add_argument('--grafana', default='http://127.0.0.1:' + setting('GRAFANA_PORT', '3000'))
    args = parser.parse_args()
    base = args.url.rstrip('/')
    grafana_user = setting('GRAFANA_ADMIN_USER', 'admin')
    grafana_password = setting('GRAFANA_ADMIN_PASSWORD')
    auth = base64.b64encode(f'{grafana_user}:{grafana_password}'.encode()).decode()
    grafana_headers = {'Authorization': 'Basic ' + auth}
    worker = 'http://127.0.0.1:' + setting('WORKER_METRICS_PORT', '9091')
    alertmanager = 'http://127.0.0.1:' + setting('ALERTMANAGER_PORT', '9093')
    for name, url in [
        ('server health', base + '/health'),
        ('worker health', worker + '/health'),
        ('Prometheus ready', args.prometheus + '/-/ready'),
        ('Loki ready', args.loki + '/ready'),
        ('Jaeger query API', args.jaeger + '/api/v3/services'),
        ('Alertmanager ready', alertmanager + '/-/ready'),
        ('Grafana health', args.grafana + '/api/health'),
    ]:
        eventually(name, lambda url=url: request(url))

    def prometheus_query(expression):
        data = read_json(args.prometheus + '/api/v1/query?' + urllib.parse.urlencode({'query': expression}))
        assert data['status'] == 'success', data
        return data['data']['result']

    def targets_ready():
        targets = prometheus_query('up{job=~"gophprofile-server|gophprofile-worker"}')
        return len(targets) == 2 and all(float(t['value'][1]) == 1 for t in targets)

    eventually('both application scrape targets are up', targets_ready)
    datasource_uids = {source['uid'] for source in read_json(args.grafana + '/api/datasources', headers=grafana_headers)}
    assert {'prometheus', 'loki', 'jaeger', 'alertmanager'} <= datasource_uids, datasource_uids
    dashboard = read_json(args.grafana + '/api/dashboards/uid/gophprofile-overview', headers=grafana_headers)
    assert dashboard['dashboard']['panels'], 'dashboard was not provisioned'
    for uid in ['prometheus', 'loki', 'jaeger']:
        health = read_json(args.grafana + '/api/datasources/uid/' + uid + '/health', headers=grafana_headers)
        assert health['status'] == 'OK', (uid, health)
    print('PASS: Grafana datasources and service dashboard provisioned')

    trace_id = uuid.uuid4().hex
    traceparent = f'00-{trace_id}-{uuid.uuid4().hex[:16]}-01'
    user = 'telemetry-' + uuid.uuid4().hex[:12]
    boundary = 'gophprofile-' + uuid.uuid4().hex
    multipart = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="telemetry.png"\r\n'
                 'Content-Type: image/png\r\n\r\n').encode() + png() + f'\r\n--{boundary}--\r\n'.encode()
    headers = {'X-User-ID': user, 'Content-Type': 'multipart/form-data; boundary=' + boundary, 'traceparent': traceparent}
    avatar_id = None
    try:
        uploaded = read_json(base + '/api/v1/avatars', method='POST', payload=multipart, headers=headers, expected=201)
        avatar_id = uploaded['id']
        route = base + '/api/v1/avatars/' + avatar_id
        eventually('worker completed the traced upload', lambda: read_json(route + '/metadata')['status'] == 'completed')
        request(route + '?size=100x100', headers={'traceparent': traceparent})

        for metric in ['gophprofile_http_requests_total', 'gophprofile_http_request_duration_seconds_count',
                       'gophprofile_uploads_total', 'gophprofile_upload_duration_seconds_count',
                       'gophprofile_worker_events_total', 'gophprofile_worker_event_duration_seconds_count',
                       'gophprofile_storage_bytes']:
            eventually(metric + ' has observations', lambda metric=metric: any(
                float(result['value'][1]) > 0 for result in prometheus_query(metric)))
        eventually('outbox queue gauge exported', lambda: prometheus_query('gophprofile_outbox_queue_depth'))
        for state in ['acquired', 'idle', 'total']:
            eventually('DB pool state ' + state, lambda state=state: prometheus_query(
                'gophprofile_db_connections{state="' + state + '"}'))
        eventually('Go and process collectors', lambda: prometheus_query('go_goroutines') and prometheus_query('process_resident_memory_bytes'))

        def distributed_trace():
            traces = read_json(args.jaeger + '/api/traces/' + trace_id).get('data') or []
            services = {process['serviceName'] for trace in traces for process in trace.get('processes', {}).values()}
            spans = [span for trace in traces for span in trace.get('spans', [])]
            tagged = [(span, {tag['key']: tag.get('value') for tag in span.get('tags', [])}) for span in spans]
            producers = {span['spanID'] for span, tags in tagged
                         if tags.get('messaging.system') == 'kafka' and tags.get('span.kind') == 'producer'}
            consumers = [span for span, tags in tagged
                         if tags.get('messaging.system') == 'kafka' and tags.get('span.kind') == 'consumer']
            checks = {
                'both services': {'gophprofile-server', 'gophprofile-worker'} <= services,
                'HTTP route': any(tags.get('http.route') == '/api/v1/avatars' for _, tags in tagged),
                'PostgreSQL': any(tags.get('db.system.name') == 'postgresql' for _, tags in tagged),
                'S3': any(tags.get('rpc.service') == 'S3' for _, tags in tagged),
                'Kafka producer/consumer parentage': any(
                    ref.get('refType') == 'CHILD_OF' and ref.get('spanID') in producers
                    for span in consumers for ref in span.get('references', [])),
                'worker task': any(span['operationName'] == 'avatar.process' for span in spans),
            }
            assert all(checks.values()), [name for name, passed in checks.items() if not passed]
            return True

        eventually('one trace contains server and worker spans', distributed_trace)

        for query in [
            {'refId': 'A', 'datasource': {'uid': 'jaeger', 'type': 'jaeger'}, 'query': trace_id},
            {'refId': 'A', 'datasource': {'uid': 'jaeger', 'type': 'jaeger'},
             'queryType': 'search', 'service': 'gophprofile-server', 'limit': 20},
        ]:
            result = read_json(args.grafana + '/api/ds/query', method='POST',
                               payload=json.dumps({'queries': [query], 'from': 'now-1h', 'to': 'now'}).encode(),
                               headers={**grafana_headers, 'Content-Type': 'application/json'})['results']['A']
            assert not result.get('error') and result.get('frames'), result
        print('PASS: Grafana Jaeger datasource opens traces and searches by service')

        def correlated_logs():
            query = '{app="gophprofile"} | json | trace_id="' + trace_id + '"'
            data = read_json(args.loki + '/loki/api/v1/query_range?' + urllib.parse.urlencode({
                'query': query, 'since': '5m', 'limit': 1000,
            }))
            streams = data['data']['result']
            services = {stream['stream'].get('service_name') for stream in streams if stream.get('values')}
            return {'gophprofile-server', 'gophprofile-worker'} <= services

        eventually('server and worker JSON logs share the trace_id in Loki', correlated_logs)
        rules = read_json(args.prometheus + '/api/v1/rules')['data']['groups']
        rule_names = {rule['name'] for group in rules for rule in group['rules']}
        assert {'GophProfileHighHTTPErrorRatio', 'GophProfileHighHTTPLatency', 'GophProfileTargetDown'} <= rule_names, rule_names
        loki_rules = read_json(args.loki + '/prometheus/api/v1/rules')['data']['groups']
        assert any(rule['name'] == 'GophProfileRepeatedErrorLogs'
                   for group in loki_rules for rule in group['rules']), 'Loki log alert rule is not loaded'
        print('PASS: alert rules loaded; trace:', trace_id)
    finally:
        if avatar_id:
            try:
                request(base + '/api/v1/avatars/' + avatar_id, method='DELETE', headers={'X-User-ID': user}, expected=204)
            except (OSError, AssertionError) as error:
                print('Cleanup failed for temporary avatar:', avatar_id, error)


if __name__ == '__main__':
    main()
