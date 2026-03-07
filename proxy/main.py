
import asyncio
import logging
import signal
import sys
from typing import Optional

from config import ProxyConfig, load_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("weir.proxy")


CHUNK_SIZE = 65_536  # 64 KiB — matches PostgreSQL's default send buffer





async def pipe(
    src_reader: asyncio.StreamReader,
    dst_writer: asyncio.StreamWriter,
    label: str,
) -> None:
    """
    Read bytes from *src_reader* and write them verbatim to *dst_writer*
    until EOF or an error, then close *dst_writer*.

    This is intentionally a pure byte relay — no parsing, no buffering
    beyond the OS socket layer.  SQL interception will be layered on top
    in Task 0.2.
    """
    try:
        while True:
            data: bytes = await src_reader.read(CHUNK_SIZE)
            if not data:
                log.debug("%s  EOF — closing pipe", label)
                break
            dst_writer.write(data)
            await dst_writer.drain()
    except (asyncio.IncompleteReadError, ConnectionResetError) as exc:
        log.debug("%s  connection reset: %s", label, exc)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s  unexpected pipe error: %s", label, exc)
    finally:
        try:
            dst_writer.close()
            await dst_writer.wait_closed()
        except Exception:
            pass


async def handle_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    cfg: ProxyConfig,
) -> None:
    """
    Accept one client connection, open a matching upstream connection,
    then bridge them with two concurrent pipe() tasks.
    """
    client_addr = client_writer.get_extra_info("peername", "<unknown>")
    log.info("New connection  client=%s", client_addr)

    server_reader: Optional[asyncio.StreamReader] = None
    server_writer: Optional[asyncio.StreamWriter] = None

    try:
        server_reader, server_writer = await asyncio.open_connection(
            cfg.target_host, cfg.target_port
        )
        log.info(
            "Upstream connected  client=%s  →  %s:%d",
            client_addr,
            cfg.target_host,
            cfg.target_port,
        )
    except OSError as exc:
        log.error(
            "Cannot reach upstream %s:%d — %s",
            cfg.target_host,
            cfg.target_port,
            exc,
        )
        client_writer.close()
        return

    # Run both directions concurrently; whichever finishes first will
    # close its writer, which causes the other pipe to detect EOF and exit.
    upstream_label = f"[{client_addr} → upstream]"
    downstream_label = f"[upstream → {client_addr}]"

    await asyncio.gather(
        pipe(client_reader, server_writer, upstream_label),
        pipe(server_reader, client_writer, downstream_label),
        return_exceptions=True,
    )

    log.info("Connection closed  client=%s", client_addr)




async def run_proxy(cfg: ProxyConfig) -> None:
    """Start the TCP server and run until cancelled."""

    async def _handler(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await handle_connection(reader, writer, cfg)

    server = await asyncio.start_server(
        _handler,
        host=cfg.listen_host,
        port=cfg.listen_port,
        # Let the OS reuse the address immediately after restart.
        reuse_address=True,
    )

    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log.info("Weir proxy listening on %s", addrs)
    log.info("%s", cfg)
    log.info("Press Ctrl-C to stop.")

    async with server:
        await server.serve_forever()


def _install_signal_handlers(loop: asyncio.AbstractEventLoop) -> None:
    """Gracefully stop on SIGINT / SIGTERM (Unix only)."""
    if sys.platform == "win32":
        return

    def _shutdown(sig: signal.Signals) -> None:
        log.info("Received %s — shutting down…", sig.name)
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)




def main() -> None:
    cfg = load_config()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _install_signal_handlers(loop)

    try:
        loop.run_until_complete(run_proxy(cfg))
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        # Cancel all outstanding tasks before closing the loop.
        pending = asyncio.all_tasks(loop)
        if pending:
            log.debug("Cancelling %d pending task(s)…", len(pending))
            for task in pending:
                task.cancel()
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()
        log.info("Weir proxy stopped.")


if __name__ == "__main__":
    main()