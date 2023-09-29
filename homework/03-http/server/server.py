import logging
import pathlib
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

    def _handle_get(self, http_request):
        abs_path = str(pathlib.Path(str(self.server.working_directory) + http_request.path))

        if not os.path.exists(abs_path):
            logger.info(f'File {abs_path} does not exist')
            return  # TODO

        if os.path.isdir(abs_path):
            output = subprocess.check_output(["ls", "-lA", "--time-style=long-iso", abs_path], universal_newlines=True)
            logger.info(output)
            output_bytes = output.encode()
            headers = {
                'Content-Type': TEXT_PLAIN,
                'Content-Length': str(len(output_bytes))
            }
            return HTTPResponse(http_request.version, OK, headers, output_bytes).to_bytes()

        if os.path.isfile(abs_path):
            with open(abs_path, 'rb') as file:
                file_content = file.read()
            headers = {
                'Content-Type': TEXT_PLAIN,
                'Content-Length': str(len(file_content))
            }
            return HTTPResponse(http_request.version, OK, headers, file_content).to_bytes()

        logger.error('Can not handle get')

    # Use self.rfile and self.wfile to interact with the client
    # Access domain and working directory with self.server.{attr}
    def handle(self) -> None:
        logger.info(f"Handle connection from {self.client_address}")

        http_request = self._build_request()

        response_bytes = None
        if http_request.method == 'GET':
            response_bytes = self._handle_get(http_request)

        self.wfile.write(response_bytes)


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
