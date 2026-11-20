"""TCP MLLP server.

* Accepts MLLP frames on a configurable local port; TCP read boundaries
  are irrelevant thanks to the streaming frame parser.
* Per-connection parser state: an unterminated frame is discarded when
  the peer disconnects and never leaks into the next connection.
* Commit/ACK ordering: AA is sent only after ``Store.commit_message``
  has durably committed the message identity, raw bytes and all result
  rows in one transaction.  Business rejections and identity conflicts
  get AE with a bounded diagnostic; framing errors are recorded with
  their location and the connection keeps serving later frames.
* Graceful stop: ``shutdown()`` stops accepting, lets in-flight
  connections finish (bounded), and closes the listener.
"""

from __future__ import annotations

import socket
import threading

from .ack import AA, AE, build_ack
from .builder import build_message
from .errors import Hl7Error
from .faults import NULL_INJECTOR, FaultInjected, FaultInjector
from .hl7 import best_effort_msh
from .mllp import DEFAULT_MAX_FRAME, MllpStreamParser
from .store import (
    OUT_COMMITTED,
    OUT_CONFLICT,
    OUT_DUPLICATE,
    ST_REJECTED,
    Store,
)

RECV_BYTES = 65536
JOIN_TIMEOUT = 5.0


class MllpServer:
    def __init__(
        self,
        host: str,
        port: int,
        store: Store,
        *,
        max_frame: int = DEFAULT_MAX_FRAME,
        faults: FaultInjector = NULL_INJECTOR,
    ) -> None:
        self.store = store
        self.max_frame = max_frame
        self.faults = faults
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._conns: set[socket.socket] = set()
        self._conns_lock = threading.Lock()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(64)
        self._sock.settimeout(0.2)
        self.host, self.port = self._sock.getsockname()[:2]

    # -- accept loop -------------------------------------------------------
    def serve_forever(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    conn, _addr = self._sock.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break  # listener closed by shutdown()
                t = threading.Thread(target=self._handle_conn, args=(conn,), daemon=True)
                t.start()
                self._threads.append(t)
        finally:
            for t in self._threads:
                t.join(timeout=JOIN_TIMEOUT)

    def shutdown(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        # unblock connection threads parked in recv(); their per-connection
        # parser state is discarded (an unterminated frame is dropped, never
        # carried into another connection)
        with self._conns_lock:
            conns = list(self._conns)
        for conn in conns:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass

    # -- connection handling -------------------------------------------------
    def _handle_conn(self, conn: socket.socket) -> None:
        conn_id = self.store.next_conn_id()
        with self._conns_lock:
            self._conns.add(conn)
        parser = MllpStreamParser(self.max_frame)
        frame_no = 0
        try:
            while not self._stop.is_set():
                try:
                    data = conn.recv(RECV_BYTES)
                except OSError:
                    break
                if not data:
                    break
                for ev in parser.feed(data):
                    if ev.kind == "error":
                        self.store.record_frame_error(
                            conn_id, ev.offset, ev.reason, ev.detail
                        )
                    else:
                        frame_no += 1
                        self._handle_frame(conn, conn_id, frame_no, ev.payload or b"")
        finally:
            with self._conns_lock:
                self._conns.discard(conn)
            for ev in parser.close():
                self.store.record_frame_error(conn_id, ev.offset, ev.reason, ev.detail)
            try:
                conn.close()
            except OSError:
                pass

    # -- one frame: parse -> commit -> ACK -----------------------------------
    def _handle_frame(
        self, conn: socket.socket, conn_id: int, frame_no: int, payload: bytes
    ) -> None:
        attempt_id: int | None = None
        ack_sent = False
        try:
            self.faults.hit("after_frame")
            attempt_id = self.store.begin_attempt(conn_id, frame_no, payload)
            try:
                msg = build_message(
                    payload, hook=lambda: self.faults.hit("during_parse")
                )
            except Hl7Error as exc:
                info = best_effort_msh(payload)
                self.store.reject_attempt(
                    attempt_id,
                    ST_REJECTED,
                    str(exc),
                    payload,
                    sender=_sender_of(info),
                    control_id=info.get("message_control_id"),
                )
                ack_sent = self._send_ack(
                    conn, AE, info.get("message_control_id") or "UNKNOWN",
                    version=info.get("version_id") or "2.5.1",
                    recv_app=info.get("sending_application") or "",
                    recv_fac=info.get("sending_facility") or "",
                    err_text=str(exc),
                )
                return

            self.faults.hit("before_commit")
            outcome = self.store.commit_message(attempt_id, msg, payload)
            if outcome == OUT_CONFLICT:
                ack_sent = self._send_ack(
                    conn, AE, msg.control_id, version=msg.version,
                    recv_app=msg.sender_app, recv_fac=msg.sender_fac,
                    err_text="identity conflict: (sender, control id) already "
                             "exists with different payload",
                )
                return
            if outcome == OUT_DUPLICATE:
                # identical retransmission: no new rows, acknowledge again
                ack_sent = self._send_ack(
                    conn, AA, msg.control_id, version=msg.version,
                    recv_app=msg.sender_app, recv_fac=msg.sender_fac,
                )
                return
            assert outcome == OUT_COMMITTED
            self.faults.hit("after_commit_before_ack")
            ack_sent = self._send_ack(
                conn, AA, msg.control_id, version=msg.version,
                recv_app=msg.sender_app, recv_fac=msg.sender_fac,
            )
            self.store.mark_acked(attempt_id)
            self.faults.hit("after_ack")
        except FaultInjected as exc:
            # Simulated internal failure: never a success ACK.
            self._record_internal_failure(attempt_id, payload, exc)
        except Exception as exc:  # e.g. database failure at commit boundary
            self._record_internal_failure(attempt_id, payload, exc)
            if ack_sent:
                # an ACK already went out for this frame; a second one would
                # confuse the sender -- the resend logic resolves the rest
                return
            info = best_effort_msh(payload)
            self._send_ack(
                conn, AE, info.get("message_control_id") or "UNKNOWN",
                version=info.get("version_id") or "2.5.1",
                recv_app=info.get("sending_application") or "",
                recv_fac=info.get("sending_facility") or "",
                err_text=f"internal error: {exc}",
            )

    def _record_internal_failure(
        self, attempt_id: int | None, payload: bytes, exc: BaseException
    ) -> None:
        if attempt_id is None:
            return
        try:
            # Only an attempt that never reached the commit may be marked
            # rejected; a committed attempt keeps its truth (a resend will
            # resolve against the committed identity).
            self.store.reject_attempt(
                attempt_id, ST_REJECTED, f"internal failure: {exc}", payload,
                only_if_state="received",
            )
        except Exception:
            pass  # the store itself may be the broken component

    def _send_ack(
        self,
        conn: socket.socket,
        code: str,
        control_id: str,
        *,
        version: str,
        recv_app: str,
        recv_fac: str,
        err_text: str | None = None,
    ) -> bool:
        """Send one ACK frame; returns True when the bytes were delivered."""
        frame = build_ack(
            code,
            control_id,
            version=version,
            recv_app=recv_app,
            recv_fac=recv_fac,
            err_text=err_text,
        )
        try:
            conn.sendall(frame)
            return True
        except OSError:
            # Client vanished before the ACK could be delivered.  If the
            # message was committed the attempt stays 'committed' and will
            # be reported as 'ack_lost' after a restart; a resend resolves
            # against the committed identity.
            return False


def _sender_of(info: dict[str, str | None]) -> str | None:
    app, fac = info.get("sending_application"), info.get("sending_facility")
    if app and fac:
        return f"{app}^{fac}"
    return app or fac
