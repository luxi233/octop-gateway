"""_harness_patches unit tests -- send_file patches shipped inside the
channel package.

Covers: both blocks applied, marker idempotency, partial-patch resume,
anchor-mismatch never writes, missing target no-op.
"""

from __future__ import annotations

from octop_gateway.channels.wechatauto import _harness_patches

SAMPLE = (
    "def send_file_to_user(file_path):\n"
    '        """Send a file to the user through the chat channel.\n'
    '        """\n'
    "        local = workspace.materialize_local(file_path)\n"
    "        return local\n"
)

BOUNDARY_ONLY = (
    "def send_file_to_user(file_path):\n"
    '        """Send a file to the user through the chat channel.\n'
    '        """\n' + _harness_patches._BOUNDARY_BLOCK + "        return local\n"
)


def test_apply_both_patches(tmp_path):
    target = tmp_path / "send_file.py"
    target.write_text(SAMPLE, encoding="utf-8")
    desc = _harness_patches.apply(target=target, log=lambda s: None)
    assert desc == "boundary-degrade+group-semantics docstring"
    text = target.read_text(encoding="utf-8")
    assert _harness_patches._BOUNDARY_MARKER in text
    assert _harness_patches._DOC_MARKER in text


def test_apply_idempotent(tmp_path):
    target = tmp_path / "send_file.py"
    target.write_text(SAMPLE, encoding="utf-8")
    assert _harness_patches.apply(target=target) is not None
    assert _harness_patches.apply(target=target) is None


def test_partial_patch_boundary_only(tmp_path):
    target = tmp_path / "send_file.py"
    target.write_text(BOUNDARY_ONLY, encoding="utf-8")
    desc = _harness_patches.apply(target=target)
    assert desc == "group-semantics docstring"
    # boundary block must appear exactly once -- no double application
    assert target.read_text(encoding="utf-8").count("PATCH(local)") == 1


def test_anchor_mismatch_no_write(tmp_path):
    logs: list[str] = []
    target = tmp_path / "send_file.py"
    target.write_text("def f():\n    pass\n", encoding="utf-8")
    assert _harness_patches.apply(target=target, log=logs.append) is None
    assert target.read_text(encoding="utf-8") == "def f():\n    pass\n"
    assert any("anchor mismatch" in line for line in logs)


def test_missing_target(tmp_path):
    assert _harness_patches.apply(target=tmp_path / "nope.py") is None
