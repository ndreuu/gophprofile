import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from observability_smoke import eventually, http_ready


class HTTPReadinessTest(unittest.TestCase):
    def setUp(self):
        self.statuses = [200]
        self.requests = 0
        case = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                case.requests += 1
                self.send_response(case.statuses.pop(0) if len(case.statuses) > 1 else case.statuses[0])
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/health'

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_accepts_healthy_empty_response(self):
        self.assertIs(eventually('empty health response', lambda: http_ready(self.url), timeout=1), True)
        self.assertEqual(self.requests, 1)

    def test_rejects_unhealthy_http_status(self):
        self.statuses = [503]
        with self.assertRaises(AssertionError):
            http_ready(self.url)
        self.assertEqual(self.requests, 1)

    def test_retries_until_healthy_empty_response(self):
        self.statuses = [503, 200]
        self.assertIs(eventually('health after retry', lambda: http_ready(self.url), timeout=3), True)
        self.assertEqual(self.requests, 2)


if __name__ == '__main__':
    unittest.main()
