#!/usr/bin/env python3
"""Standalone binary launcher for kubectl-exec-safe background start.

Mirrors the pattern in ``cli/start.py`` (``subprocess.Popen`` with
``start_new_session=True``): launch the binary in a new session so it does
not hold the caller's stdout/stderr pipe open, poll for readiness, then
print the PID and exit. The caller (``kubectl exec``) returns immediately
because the binary is detached and the launcher parent has exited.

This avoids two problems with the previous ``nohup ./binary ... & echo $!``
shell pattern:

1. ``kubectl exec`` hangs for the full subprocess timeout because the
   background binary inherits the shell's SPDY pipe; the launcher's parent
   does not have that pipe after ``start_new_session=True`` + redirects,
   so ``kubectl exec`` returns as soon as the launcher parent exits.
2. The ``setsid`` binary is missing on some minimal images; the launcher
   uses Python's ``subprocess.Popen(start_new_session=True)`` which calls
   ``setsid(2)`` directly, no external binary required.

Readiness polling matches ``dscli start`` (``cli/start.py``):

* If ``--ready-file`` is given, poll for that file's existence until it
  appears (or ``--ready-timeout`` elapses). Used by worker standalones;
  the worker binary writes ``FLAGS_ready_check_path`` only after
  ``WaitForServiceReady()`` + ``WaitForTopologyReady()`` complete, so this
  is the authoritative readiness signal.
* If ``--port`` is given (and no ``--ready-file``), poll TCP connect to
  ``--host:--port`` until it succeeds. Used by coordinator standalones.
* If neither is given, poll ``proc.poll()`` for ``--no-signal-grace``
  seconds (default 2) to catch early exits (bad gflags, missing .so,
  config errors), then print the PID. Used by client-style binaries that
  do not expose a readiness signal; the grace window catches the common
  failure mode where the binary parses gflags and exits before the caller
  can verify via ``pgrep``.

Failure detection:

* If the binary exits before becoming ready (or before the no-signal grace
  period elapses), the launcher prints an error to stderr and returns 1
  — no PID is printed to stdout, so the caller can distinguish success
  from failure by checking stdout.
* If the readiness deadline elapses without the binary becoming ready and
  without the binary exiting, the launcher does a final ``proc.poll()``
  check. If the binary has exited (race with the last sleep), it returns
  1. Otherwise it prints the PID + elapsed and returns 0 with a WARNING
  on stderr, so the caller can verify via ``pgrep``.

Output format:

* On success: prints ``{pid} {elapsed}`` to stdout, where ``elapsed`` is
  the actual binary startup time measured inside the launcher (Popen →
  ready signal), excluding ``kubectl exec`` / ``python3`` startup overhead.
* On failure: prints nothing to stdout; error details on stderr.

The caller should parse stdout's last line, split on whitespace: first
field is the PID (digit string), second field (if present) is the elapsed
time in seconds (float).
"""

import argparse
import os
import socket
import subprocess
import sys
import time


def is_port_ready(host, port, timeout=0.5):
    """Return True if a TCP connect to host:port succeeds."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def is_file_ready(path, cwd=None):
    """Return True if a readiness file exists.

    ``path`` may be absolute or relative. Relative paths are resolved
    against ``cwd`` (the binary's working directory), matching the worker
    binary's own file-write semantics — the binary writes
    ``FLAGS_ready_check_path`` relative to its CWD, so the launcher must
    check the same resolved path.
    """
    if not path:
        return False
    if not os.path.isabs(path) and cwd:
        path = os.path.join(cwd, path)
    return os.path.exists(path)


def clear_stale_ready_file(path, cwd=None):
    """Remove a stale ready file before launching a new binary.

    Mirrors dscli ``start_worker`` (``cli/start.py:671-672``): the worker
    binary deletes ``FLAGS_ready_check_path`` at the start of
    ``ReadinessProbe()`` and re-creates it only after
    ``WaitForTopologyReady()`` completes (``worker_oc_server.cpp:2920-2922``).
    The worker's ``Shutdown()`` does not delete the file, so without this
    cleanup a second launch would hit the previous run's stale file on the
    first readiness poll and report ``ready`` in milliseconds, before the
    new worker has reached ``ReadinessProbe()``.

    Relative paths are resolved against ``cwd`` to match ``is_file_ready``.
    Returns True when a file was removed, False when it was already absent
    or could not be removed (treated as best-effort: launch proceeds and
    the readiness poll still bounds the wait).
    """
    if not path:
        return False
    if not os.path.isabs(path) and cwd:
        path = os.path.join(cwd, path)
    try:
        os.unlink(path)
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return False


def build_env(extra_lib_path=None):
    """Return a copy of os.environ with LD_LIBRARY_PATH prepended if given."""
    env = os.environ.copy()
    if extra_lib_path:
        existing = env.get('LD_LIBRARY_PATH', '')
        env['LD_LIBRARY_PATH'] = (
            f'{extra_lib_path}:{existing}' if existing else extra_lib_path)
    return env


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description='Launch a binary detached from the caller session.')
    parser.add_argument('--binary', required=True,
                        help='Path to the binary to launch')
    parser.add_argument('--cwd',
                        help='Working directory for the binary')
    parser.add_argument('--log', required=True,
                        help='File to redirect binary stdout+stderr (append)')
    parser.add_argument('--lib-path',
                        help='Path to prepend to LD_LIBRARY_PATH for the binary')
    parser.add_argument('--ready-file',
                        help='If set, poll for this file\'s existence until '
                             'ready (authoritative readiness signal, e.g. '
                             'worker ready_check_path). Relative paths are '
                             'resolved against --cwd.')
    parser.add_argument('--port', type=int,
                        help='If set, poll TCP connect on host:port until ready')
    parser.add_argument('--host', default='127.0.0.1',
                        help='Host for port readiness poll (default: 127.0.0.1)')
    parser.add_argument('--ready-timeout', type=float, default=30.0,
                        help='Max seconds to wait for readiness (default: 30)')
    parser.add_argument('--ready-interval', type=float, default=0.5,
                        help='Polling interval in seconds (default: 0.5)')
    parser.add_argument('--no-signal-grace', type=float, default=2.0,
                        help='Max seconds to poll proc.poll() for early '
                             'exits when no readiness signal (--ready-file '
                             'or --port) is given (default: 2). Catches '
                             'gflag/config failures before reporting '
                             'success. Ignored when --ready-file or --port '
                             'is set.')
    parser.add_argument('argv', nargs='*',
                        help='Arguments for the binary (separate with --)')
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    binary_argv = list(args.argv)
    # Defensive: strip a leading `--` if argparse left it in positional argv.
    if binary_argv and binary_argv[0] == '--':
        binary_argv = binary_argv[1:]

    env = build_env(args.lib_path)

    # Open log in parent; pass fd to child as stdout/stderr. Parent closes
    # its copy after Popen so the file is only held by the child.
    log_fd = os.open(args.log,
                     os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)

    # Clear any stale ready_file before Popen, mirroring dscli start_worker
    # (cli/start.py:671-672). Without this, the first readiness poll could
    # hit a file left by a previous run and report ready before the new
    # binary reaches ReadinessProbe(). See clear_stale_ready_file for the
    # full rationale.
    if args.ready_file:
        clear_stale_ready_file(args.ready_file, args.cwd)

    # t_launch anchors the actual binary startup timing (Popen → ready),
    # excluding kubectl exec / python3 startup overhead that the caller's
    # outer time.monotonic() diff would include.
    t_launch = time.monotonic()
    try:
        proc = subprocess.Popen(
            [args.binary] + binary_argv,
            cwd=args.cwd,
            env=env,
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
    except OSError as e:
        os.close(log_fd)
        print(f'standalone_launcher: failed to start {args.binary}: {e}',
              file=sys.stderr, flush=True)
        return 1
    finally:
        try:
            os.close(log_fd)
        except OSError:
            pass

    # Readiness polling priority: --ready-file (authoritative, e.g. worker
    # ready_check_path) > --port (TCP connect, e.g. coordinator) > none
    # (grace-poll proc.poll() to catch early exits). This mirrors dscli's
    # split: start_worker waits on ready_check_path, start_coordinator waits
    # on is_tcp_ready.
    #
    # For the no-signal path, use a shorter grace deadline instead of the
    # full ready_timeout: the goal is to catch gflag/config failures (which
    # happen within the first few hundred ms), not to wait for indefinite
    # readiness. The binary is expected to stay running; if it survives the
    # grace period, print PID + elapsed and return 0.
    has_readiness_signal = bool(args.ready_file) or args.port is not None
    if has_readiness_signal:
        deadline = t_launch + args.ready_timeout
    else:
        deadline = t_launch + min(args.ready_timeout, args.no_signal_grace)

    while time.monotonic() < deadline:
        rc = proc.poll()
        if rc is not None:
            elapsed = time.monotonic() - t_launch
            print(f'standalone_launcher: binary exited early with code {rc} '
                  f'after {elapsed:.3f}s',
                  file=sys.stderr, flush=True)
            return 1
        if args.ready_file:
            if is_file_ready(args.ready_file, args.cwd):
                elapsed = time.monotonic() - t_launch
                print(f'{proc.pid} {elapsed:.3f}', flush=True)
                return 0
        elif args.port is not None:
            if is_port_ready(args.host, args.port,
                             timeout=args.ready_interval):
                elapsed = time.monotonic() - t_launch
                print(f'{proc.pid} {elapsed:.3f}', flush=True)
                return 0
        # No readiness signal: keep polling proc.poll() until the grace
        # deadline to catch early exits (bad gflags, missing .so, etc).
        time.sleep(args.ready_interval)

    # Deadline expired. Final poll: if the binary exited during the last
    # sleep interval (race window), report failure instead of printing a
    # dead PID.
    rc = proc.poll()
    if rc is not None:
        elapsed = time.monotonic() - t_launch
        print(f'standalone_launcher: binary exited with code {rc} '
              f'after {elapsed:.3f}s',
              file=sys.stderr, flush=True)
        return 1

    # Binary is still running but not ready within the deadline. Print
    # PID + elapsed so the caller can pgrep/kill, with a warning on stderr.
    elapsed = time.monotonic() - t_launch
    print(f'{proc.pid} {elapsed:.3f}', flush=True)
    timeout_label = (args.ready_timeout if has_readiness_signal
                     else min(args.ready_timeout, args.no_signal_grace))
    waited_on = (f'file={args.ready_file}' if args.ready_file
                 else f'port={args.host}:{args.port}' if args.port is not None
                 else 'no-signal')
    print(f'standalone_launcher: WARNING: not ready within '
          f'{timeout_label}s ({waited_on}, pid={proc.pid}); '
          f'printed PID for caller verify',
          file=sys.stderr, flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
