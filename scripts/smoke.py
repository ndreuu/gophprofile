#!/usr/bin/env python3
import argparse
import json
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zlib


def png(width=180, height=120):
    def chunk(kind, payload):
        return struct.pack('!I', len(payload)) + kind + payload + struct.pack('!I', zlib.crc32(kind + payload) & 0xffffffff)
    pixels = b''.join(b'\0' + bytes([40, 170, 130]) * width for _ in range(height))
    return b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('!2I5B', width, height, 8, 2, 0, 0, 0)) + chunk(b'IDAT', zlib.compress(pixels)) + chunk(b'IEND', b'')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:8080')
    parser.add_argument('--write-fixture', help='write a PNG fixture and exit')
    args = parser.parse_args()
    if args.write_fixture:
        with open(args.write_fixture, 'wb') as f:
            f.write(png())
        return
    base = args.url.rstrip('/')
    user = 'smoke-' + uuid.uuid4().hex[:12]
    avatar_id = None

    def request(method, route, payload=None, headers=None, expected=200):
        req = urllib.request.Request(base + route, data=payload, method=method, headers=headers or {})
        try:
            response = urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as e:
            response = e
        with response:
            body = response.read()
            assert response.status == expected, (method, route, response.status, body[:500])
            return body, dict(response.headers)

    deadline = time.monotonic() + 60
    while True:
        try:
            health, headers = request('GET', '/health')
            assert json.loads(health)['status'] == 'ok'
            break
        except (OSError, AssertionError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)
    print('Health OK; mode:', headers.get('X-App-Mode', 'production'))
    request('POST', '/api/v1/avatars', b'', expected=400)
    data = png()
    boundary = 'gophprofile-' + uuid.uuid4().hex
    multipart = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="smoke.png"\r\nContent-Type: image/png\r\n\r\n'.encode() + data + f'\r\n--{boundary}--\r\n'.encode())
    try:
        raw, _ = request('POST', '/api/v1/avatars', multipart,
                         {'X-User-ID': user, 'Content-Type': 'multipart/form-data; boundary=' + boundary}, 201)
        uploaded = json.loads(raw)
        avatar_id = uploaded['id']
        assert uploaded['status'] == 'processing'
        route = '/api/v1/avatars/' + avatar_id
        original, _ = request('GET', route)
        assert original == data
        request('DELETE', route, headers={'X-User-ID': 'not-the-owner'}, expected=403)
        deadline = time.monotonic() + 45
        while True:
            raw, _ = request('GET', route + '/metadata')
            metadata = json.loads(raw)
            if metadata['status'] == 'completed':
                break
            assert time.monotonic() < deadline, 'worker did not complete within 45 seconds'
            time.sleep(.25)
        assert {t['size'] for t in metadata['thumbnails']} == {'100x100', '300x300'}
        for size in ('100x100', '300x300'):
            body, image_headers = request('GET', route + '?size=' + size)
            assert body.startswith(b'\xff\xd8'), 'thumbnail is not JPEG'
            assert image_headers['Content-Type'] == 'image/jpeg'
        for size in ('original', '100x100', '300x300'):
            for image_format in ('jpeg', 'png', 'webp'):
                converted, converted_headers = request('GET', route + '?size=' + size + '&format=' + image_format)
                assert converted_headers['Content-Type'] == 'image/' + image_format
                assert int(converted_headers['Content-Length']) == len(converted)
                if image_format == 'jpeg':
                    assert converted.startswith(b'\xff\xd8')
                elif image_format == 'png':
                    assert converted.startswith(b'\x89PNG\r\n\x1a\n')
                else:
                    assert converted[:4] == b'RIFF' and converted[8:12] == b'WEBP'
        request('GET', route + '?format=gif', expected=400)
        listing, _ = request('GET', '/api/v1/users/' + urllib.parse.quote(user) + '/avatars')
        parsed_list = json.loads(listing)
        if isinstance(parsed_list, dict):
            parsed_list = parsed_list['avatars']
        assert any(a['id'] == avatar_id for a in parsed_list)
        request('GET', '/api/v1/users/' + user + '/avatar')
        request('DELETE', route, headers={'X-User-ID': user}, expected=204)
        request('DELETE', route, headers={'X-User-ID': user}, expected=204)
        request('GET', route, expected=404)
        print('PASS: upload, original, asynchronous 100/300 thumbnails, JPEG/PNG/WebP conversion, list/current, ownership, idempotent delete, 404 after soft delete')
    finally:
        if avatar_id:
            try:
                request('DELETE', '/api/v1/avatars/' + avatar_id, headers={'X-User-ID': user}, expected=204)
            except (OSError, AssertionError):
                pass


if __name__ == '__main__':
    main()
