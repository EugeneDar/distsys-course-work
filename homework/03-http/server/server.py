import gzip
import logging
import pathlib
import shutil
from dataclasses import dataclass
from socketserver import StreamRequestHandler
import typing as t
import click
import subprocess
import socket
import os
from http_messages import *

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

BATCH_SIZE = 32 * 1024 * 1024

@dataclass
class HTTPServer:
    server_address: t.Tuple[str, int]
    socket: socket.socket
    server_domain: str
    working_directory: pathlib.Path


class HTTPHandler(StreamRequestHandler):
    server: HTTPServer

    def _build_request(self):
        headers_bytes = b""
        while True:
            line = self.rfile.readline()
            if not line.strip():
                break
            headers_bytes += line
        return HTTPRequest.from_bytes(headers_bytes)

    def read_rfile_by_parts(self, content_length):
        while True:
            if content_length == 0:
                break
            will_read = min(content_length, BATCH_SIZE)
            content_length -= will_read
            batch = self.rfile.read(will_read)
            yield batch

    def handle_get(self, http_request, abs_path):
        if self.server.server_domain and self.server.server_domain != http_request.headers[HEADER_HOST]:
            return HTTPResponse(http_request.version, BAD_REQUEST, {})

        if not os.path.exists(abs_path):
            output_bytes = f"{abs_path} not found".encode()
            headers = {
                HEADER_CONTENT_TYPE: TEXT_PLAIN,
                HEADER_CONTENT_LENGTH: str(len(output_bytes))
            }
            return HTTPResponse(http_request.version, NOT_FOUND, headers, output_bytes)

        if os.path.isdir(abs_path):
            output = subprocess.check_output(["ls", "-lA", "--time-style=long-iso", abs_path], universal_newlines=True)
            output_bytes = output.encode()

            # TODO may be too much memory here

            if GZIP in http_request.headers.get(HEADER_ACCEPT_ENCODING, ''):
                output_bytes = gzip.compress(output_bytes)
                headers = {
                    HEADER_CONTENT_TYPE: APPLICATION_GZIP,
                    HEADER_CONTENT_LENGTH: str(len(output_bytes)),
                    HEADER_CONTENT_ENCODING: GZIP
                }
                return HTTPResponse(http_request.version, OK, headers, output_bytes)
            else:
                headers = {
                    HEADER_CONTENT_TYPE: TEXT_PLAIN,
                    HEADER_CONTENT_LENGTH: str(len(output_bytes))
                }
                return HTTPResponse(http_request.version, OK, headers, output_bytes)

        if os.path.isfile(abs_path):
            if os.stat(abs_path).st_size > BATCH_SIZE:
                if GZIP in http_request.headers.get(HEADER_ACCEPT_ENCODING, ''):
                    headers = {
                        HEADER_CONTENT_TYPE: APPLICATION_OCTET_STREAM,
                        HEADER_CONTENT_ENCODING: GZIP,
                    }
                else:
                    headers = {
                        HEADER_CONTENT_TYPE: TEXT_PLAIN,
                        HEADER_CONTENT_LENGTH: str(os.stat(abs_path).st_size)
                    }
                return HTTPResponse(http_request.version, OK, headers)

            with open(abs_path, 'rb') as file:
                file_content = file.read()

            if GZIP in http_request.headers.get(HEADER_ACCEPT_ENCODING, ''):
                file_content = gzip.compress(file_content)
                headers = {
                    HEADER_CONTENT_TYPE: APPLICATION_GZIP,
                    HEADER_CONTENT_LENGTH: str(len(file_content)),
                    HEADER_CONTENT_ENCODING: GZIP
                }
                return HTTPResponse(http_request.version, OK, headers, file_content)
            else:
                headers = {
                    HEADER_CONTENT_TYPE: TEXT_PLAIN,
                    HEADER_CONTENT_LENGTH: str(len(file_content))
                }
                return HTTPResponse(http_request.version, OK, headers, file_content)

    def handle_post(self, http_request, abs_path):
        content_length = int(http_request.headers.get(HEADER_CONTENT_LENGTH, 0))

        if self.server.server_domain and self.server.server_domain != http_request.headers[HEADER_HOST]:
            for _ in self.read_rfile_by_parts(content_length):
                pass

            return HTTPResponse(http_request.version, BAD_REQUEST, {})

        if os.path.exists(abs_path):
            for _ in self.read_rfile_by_parts(content_length):
                pass

            output_bytes = "File or directory already exists".encode()
            headers = {
                HEADER_CONTENT_TYPE: TEXT_PLAIN,
                HEADER_CONTENT_LENGTH: str(len(output_bytes))
            }
            return HTTPResponse(http_request.version, CONFLICT, headers, output_bytes)

        if http_request.headers.get(HEADER_CREATE_DIRECTORY, '').lower() == 'true':
            for _ in self.read_rfile_by_parts(content_length):
                pass

            os.makedirs(abs_path, exist_ok=True)
            return HTTPResponse(http_request.version, OK, {})

        if not os.path.exists(os.path.dirname(abs_path)):
            os.makedirs(os.path.dirname(abs_path))

        # to empty file
        open(abs_path, 'w').close()

        # open it for appending
        with open(abs_path, 'ab') as file:
            for batch in self.read_rfile_by_parts(content_length):
                file.write(batch)

        return HTTPResponse(http_request.version, OK, {})

    def handle_put(self, http_request, abs_path):
        content_length = int(http_request.headers.get(HEADER_CONTENT_LENGTH, 0))

        if self.server.server_domain and self.server.server_domain != http_request.headers[HEADER_HOST]:
            for _ in self.read_rfile_by_parts(content_length):
                pass

            return HTTPResponse(http_request.version, BAD_REQUEST, {})

        if os.path.exists(abs_path) and os.path.isdir(abs_path):
            for _ in self.read_rfile_by_parts(content_length):
                pass

            output_bytes = "This is directory".encode()
            headers = {
                HEADER_CONTENT_TYPE: TEXT_PLAIN,
                HEADER_CONTENT_LENGTH: str(len(output_bytes))
            }
            return HTTPResponse(http_request.version, CONFLICT, headers, output_bytes)

        if not os.path.exists(os.path.dirname(abs_path)):
            os.makedirs(os.path.dirname(abs_path))

        # to empty file
        open(abs_path, 'w').close()

        # open it for appending
        with open(abs_path, 'ab') as file:
            for batch in self.read_rfile_by_parts(content_length):
                file.write(batch)

        return HTTPResponse(http_request.version, OK, {})

    def handle_delete(self, http_request, abs_path):
        if self.server.server_domain and self.server.server_domain != http_request.headers[HEADER_HOST]:
            return HTTPResponse(http_request.version, BAD_REQUEST, {})

        if (
            os.path.exists(abs_path)
            and os.path.isdir(abs_path)
            and not http_request.headers.get(HEADER_REMOVE_DIRECTORY, None)
        ):
            output_bytes = f"We need {HEADER_REMOVE_DIRECTORY} header to remove directory".encode()
            headers = {
                HEADER_CONTENT_TYPE: TEXT_PLAIN,
                HEADER_CONTENT_LENGTH: str(len(output_bytes))
            }
            return HTTPResponse(http_request.version, NOT_ACCEPTABLE, headers, output_bytes)

        if os.path.isfile(abs_path):
            os.remove(abs_path)
        elif os.path.isdir(abs_path):
            shutil.rmtree(abs_path)

        return HTTPResponse(http_request.version, OK, {})

    def handle(self) -> None:
        logger.info(f"Handle connection from {self.client_address}")

        http_request = self._build_request()
        abs_path = str(pathlib.Path(str(self.server.working_directory) + http_request.path))

        http_response = None
        if http_request.method == GET:
            http_response = self.handle_get(http_request, abs_path)
        elif http_request.method == POST:
            http_response = self.handle_post(http_request, abs_path)
        elif http_request.method == PUT:
            http_response = self.handle_put(http_request, abs_path)
        elif http_request.method == DELETE:
            http_response = self.handle_delete(http_request, abs_path)

        http_response.headers[HEADER_SERVER] = self.server.server_domain or 'server'

        self.wfile.write(http_response.to_bytes())

        if HEADER_CONTENT_LENGTH in http_response.headers and int(http_response.headers[HEADER_CONTENT_LENGTH]) > BATCH_SIZE:
            with open(abs_path, 'rb') as file:
                while True:
                    file_content = file.read(BATCH_SIZE)
                    if not file_content:
                        break
                    self.wfile.write(file_content)

        logger.info(http_response.status == OK)
        logger.info(HEADER_CONTENT_LENGTH not in http_response.headers)

        if http_response.status == OK and HEADER_CONTENT_LENGTH not in http_response.headers:
            logger.info('1')
            with open(abs_path, 'rb') as file:
                gzipper = gzip.GzipFile(fileobj=self.wfile, mode='wb')
                try:
                    while True:
                        data = file.read(BATCH_SIZE)
                        if not data:
                            break
                        gzipper.write(data)
                finally:
                    gzipper.close()



@click.command()
@click.option("--host", type=str)
@click.option("--port", type=int)
@click.option("--server-domain", type=str)
@click.option("--working-directory", type=str)
def main(host, port, server_domain, working_directory):
    host = host or os.environ.get('SERVER_HOST', '0.0.0.0')
    port = port or os.environ.get('SERVER_PORT', 8080)
    working_directory = working_directory or os.environ.get('SERVER_WORKING_DIRECTORY', None)
    server_domain = server_domain or os.environ.get('SERVER_DOMAIN', None)
    if working_directory is None:
        logger.error('working_directory not set')
        exit(1)

    working_directory_path = pathlib.Path(working_directory)

    logger.info(
        f"Starting server on {host}:{port}, domain {server_domain}, working directory {working_directory}"
    )

    # Create a server socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Set SO_REUSEADDR option
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Bind the socket object to the address and port
    s.bind((host, port))
    # Start listening for incoming connections
    s.listen()

    logger.info(f"Listening at {s.getsockname()}")
    server = HTTPServer((host, port), s, server_domain, working_directory_path)

    while True:
        # Accept any new connection (request, client_address)
        try:
            conn, addr = s.accept()
        except OSError:
            break

        try:
            # Handle the request
            HTTPHandler(conn, addr, server)

            # Close the connection
            conn.shutdown(socket.SHUT_WR)
            conn.close()
        except Exception as e:
            logger.error(e)
            conn.close()


if __name__ == "__main__":
    main(auto_envvar_prefix="SERVER")
