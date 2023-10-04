import dataclasses
import typing as t
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class HTTPRequest:
    method: str
    path: str
    version: str
    parameters: t.Dict[str, str]
    headers: t.Dict[str, str]

    @staticmethod
    def parse_headers(data: bytes) -> tuple:
        logger.info(data)

        header_lines = list(filter(bool, data.split(b'\r\n')))

        method, path, version = header_lines[0].decode().split()

        headers = {}
        for line in header_lines[1:]:
            key, value = line.decode().split(':', 1)
            headers[key.strip()] = value.strip()
        return method, path, version, headers

    @staticmethod
    def extract_parameters(path: str) -> dict:
        parameters = {}
        if '?' in path:
            path, param_string = path.split('?', 1)
            param_pairs = param_string.split('&')
            for pair in param_pairs:
                key, value = pair.split('=')
                parameters[key] = value
        return parameters

    @staticmethod
    def from_bytes(data: bytes) -> "HTTPRequest":
        method, path, version, headers = HTTPRequest.parse_headers(data)
        parameters = HTTPRequest.extract_parameters(path)
        return HTTPRequest(method, path, version, parameters, headers)

    def to_bytes(self) -> bytes:
        pass


@dataclasses.dataclass
class HTTPResponse:
    version: str
    status: str
    headers: t.Dict[str, str]
    body: bytes = b""

    @staticmethod
    def from_bytes(data: bytes) -> "HTTPResponse":
        pass

    def to_bytes(self) -> bytes:
        status_line = f"{self.version} {self.status} {HTTP_REASON_BY_STATUS[self.status]}\r\n"

        headers_str = ""
        for key, value in self.headers.items():
            headers_str += f"{key}: {value}\r\n"

        response_bytes = status_line.encode() + headers_str.encode() + CRLF + self.body

        return response_bytes

# Common HTTP strings and constants


CR = b'\r'
LF = b'\n'
CRLF = CR + LF

HTTP_VERSION = "1.1"

OPTIONS = 'OPTIONS'
GET = 'GET'
HEAD = 'HEAD'
POST = 'POST'
PUT = 'PUT'
DELETE = 'DELETE'

METHODS = [
    OPTIONS,
    GET,
    HEAD,
    POST,
    PUT,
    DELETE,
]

HEADER_HOST = "Host"
HEADER_CONTENT_LENGTH = "Content-Length"
HEADER_CONTENT_TYPE = "Content-Type"
HEADER_CONTENT_ENCODING = "Content-Encoding"
HEADER_ACCEPT_ENCODING = "Accept-Encoding"
HEADER_CREATE_DIRECTORY = "Create-Directory"
HEADER_SERVER = "Server"
HEADER_REMOVE_DIRECTORY = "Remove-Directory"

GZIP = "gzip"

TEXT_PLAIN = "text/plain"
APPLICATION_OCTET_STREAM = "application/octet-stream"
APPLICATION_GZIP = "application/gzip"

OK = "200"
BAD_REQUEST = "400"
NOT_FOUND = "404"
METHOD_NOT_ALLOWED = "405"
NOT_ACCEPTABLE = "406"
CONFLICT = "409"

HTTP_REASON_BY_STATUS = {
    "100": "Continue",
    "101": "Switching Protocols",
    "200": "OK",
    "201": "Created",
    "202": "Accepted",
    "203": "Non-Authoritative Information",
    "204": "No Content",
    "205": "Reset Content",
    "206": "Partial Content",
    "300": "Multiple Choices",
    "301": "Moved Permanently",
    "302": "Found",
    "303": "See Other",
    "304": "Not Modified",
    "305": "Use Proxy",
    "307": "Temporary Redirect",
    "400": "Bad Request",
    "401": "Unauthorized",
    "402": "Payment Required",
    "403": "Forbidden",
    "404": "Not Found",
    "405": "Method Not Allowed",
    "406": "Not Acceptable",
    "407": "Proxy Authentication Required",
    "408": "Request Time-out",
    "409": "Conflict",
    "410": "Gone",
    "411": "Length Required",
    "412": "Precondition Failed",
    "413": "Request Entity Too Large",
    "414": "Request-URI Too Large",
    "415": "Unsupported Media Type",
    "416": "Requested range not satisfiable",
    "417": "Expectation Failed",
    "500": "Internal Server Error",
    "501": "Not Implemented",
    "502": "Bad Gateway",
    "503": "Service Unavailable",
    "504": "Gateway Time-out",
    "505": "HTTP Version not supported",
}
