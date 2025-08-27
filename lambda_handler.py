
import base64, io, sys, urllib.parse
from app import app  # must expose "app" (Flask instance)

TEXT_LIKE = ("text/", "json", "xml", "javascript", "svg")

def _is_http_v2(event):
    # Lambda Function URL and API Gateway HTTP API (v2)
    return isinstance(event.get("requestContext", {}).get("http"), dict) or "rawPath" in event

def _get_method(event):
    if _is_http_v2(event):
        return event.get("requestContext", {}).get("http", {}).get("method", "GET")
    return event.get("httpMethod", "GET")  # REST v1

def _get_path(event):
    if _is_http_v2(event):
        return event.get("rawPath") or "/"
    return event.get("path") or "/"

def _get_querystring(event):
    if "rawQueryString" in event and event["rawQueryString"]:
        return event["rawQueryString"]
    # Build from (multi)value query params (REST v1 fallback)
    mv = event.get("multiValueQueryStringParameters")
    if mv:
        parts = []
        for k, vals in mv.items():
            for v in vals:
                parts.append(f"{urllib.parse.quote_plus(k)}={urllib.parse.quote_plus(v)}")
        return "&".join(parts)
    q = event.get("queryStringParameters")
    if q:
        return urllib.parse.urlencode(q)
    return ""

def _get_headers(event):
    return event.get("headers") or {}

def _first_header(headers, name, default=None):
    # case-insensitive single-value fetch
    low = name.lower()
    for k, v in headers.items():
        if k.lower() == low:
            return v
    return default

def handler(event, context):
    headers = _get_headers(event)

    # method/path/query/body
    method = _get_method(event)
    raw_path = _get_path(event)
    query = _get_querystring(event)

    body = event.get("body") or ""
    is_b64 = event.get("isBase64Encoded", False)
    body_bytes = base64.b64decode(body) if is_b64 else body.encode("utf-8")

    # server + client info
    scheme = _first_header(headers, "x-forwarded-proto", "https")
    server_name = _first_header(headers, "host", "lambda")
    server_port = _first_header(headers, "x-forwarded-port", "443" if scheme == "https" else "80")
    remote_addr = (_first_header(headers, "x-forwarded-for", "") or "").split(",")[0].strip() or "0.0.0.0"

    script_name = ""
    path_info = raw_path

    # Build WSGI environ for Flask
    environ = {
        "REQUEST_METHOD": method,
        "SCRIPT_NAME": script_name,
        "PATH_INFO": path_info,
        "QUERY_STRING": query,
        "SERVER_NAME": server_name,
        "SERVER_PORT": str(server_port),
        "REMOTE_ADDR": remote_addr,
        "SERVER_PROTOCOL": "HTTP/1.1",
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": scheme,
        "wsgi.input": io.BytesIO(body_bytes),
        "wsgi.errors": sys.stderr,
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": True,
        "CONTENT_LENGTH": str(len(body_bytes)),
    }

    # Pass through Content-Type/Length
    ctype = _first_header(headers, "content-type")
    if ctype:
        environ["CONTENT_TYPE"] = ctype

    # Map other headers to HTTP_*
    for k, v in headers.items():
        kl = k.lower()
        if kl in ("content-type", "content_length"):
            continue
        http_key = "HTTP_" + k.upper().replace("-", "_")
        environ[http_key] = v

    # Run the WSGI app
    status_holder = {}
    headers_holder = []

    def start_response(status, response_headers, exc_info=None):
        status_holder["status"] = status
        # Keep as a list to preserve duplicates (e.g., multiple Set-Cookie)
        headers_holder[:] = list(response_headers)
        return None

    result = app(environ, start_response)

    try:
        chunks = []
        for chunk in result:
            if isinstance(chunk, (bytes, bytearray)):
                chunks.append(bytes(chunk))
            else:
                chunks.append(str(chunk).encode("utf-8"))
        body_out = b"".join(chunks)
    finally:
        if hasattr(result, "close"):
            result.close()

    # Parse status
    status_code = int((status_holder["status"] or "200 OK").split(" ", 1)[0])

    # Separate cookies (multiple Set-Cookie must not be merged)
    response_headers = headers_holder
    cookies_out = [v for (k, v) in response_headers if k.lower() == "set-cookie"]
    # Build headers dict, merging duplicates except Set-Cookie
    headers_out = {}
    for k, v in response_headers:
        kl = k.lower()
        if kl == "set-cookie":
            continue
        if k in headers_out:
            # merge duplicates with comma, which is standard for most headers
            headers_out[k] = f"{headers_out[k]},{v}"
        else:
            headers_out[k] = v

    # Decide if body is text-like
    resp_ctype = (headers_out.get("Content-Type") or headers_out.get("content-type") or "").lower()
    is_text_like = any(tok in resp_ctype for tok in TEXT_LIKE)

    # Respect explicit charset if present
    charset = "utf-8"
    if "charset=" in resp_ctype:
        charset = resp_ctype.split("charset=", 1)[1].split(";", 1)[0].strip()

    if is_text_like:
        try:
            body_str = body_out.decode(charset, errors="replace")
        except LookupError:
            body_str = body_out.decode("utf-8", errors="replace")
        is_base64 = False
    else:
        body_str = base64.b64encode(body_out).decode("ascii")
        is_base64 = True

    # Build Lambda proxy response
    response = {
        "statusCode": status_code,
        "headers": headers_out,
        "body": body_str,
        "isBase64Encoded": is_base64,
    }
    if cookies_out:
        # Function URLs and HTTP API v2 accept top-level "cookies"
        response["cookies"] = cookies_out

    return response




















