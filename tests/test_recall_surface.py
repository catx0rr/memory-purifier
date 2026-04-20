"""L: weighted recall surface picks by actionability, not pure age (v1.4.0).

Scoring formula:
    score = (age_days * 0.1)
          + status_weight[status]    # contested=3, unresolved=2, retire_candidate=1
          + (min(provenance_count, 5) * 0.3)
          + (min(recurrence or 0, 10) * 0.2)

Scenarios:
- contested-recent vs retire_candidate-old: contested wins
- unresolved-recent vs retire_candidate-old: unresolved wins
- all three present: contested wins
"""

import json
import unittest
from datetime import datetime, timedelta, timezone

from _helpers import build_fixtures, cleanup_sandbox, make_sandbox, run_installer, run_pipeline, write_source


def _iso_days_ago(days: int) -> tuple:
    """Return (local_iso, utc_iso) for a timestamp N days in the past."""
    now = datetime.now().astimezone()
    ts = now - timedelta(days=days)
    return ts.isoformat(), ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _make_claim(claim_id: str, status: str, age_days: int, provenance_count: int = 1, text: str = "") -> dict:
    local, utc = _iso_days_ago(age_days)
    prov = [
        {"source": f"MEMORY.md", "lineSpan": [i + 1, i + 1], "type": "direct",
         "capturedAt": local}
        for i in range(provenance_count)
    ]
    return {
        "id": claim_id,
        "type": "fact" if status != "open_question" else "open_question",
        "status": status,
        "text": text or f"Test claim {claim_id}",
        "subject": f"subject-{claim_id}", "predicate": "is", "object": None,
        "primaryHome": "LTMEMORY.md", "secondaryTags": [],
        "profileScope": "business",
        "provenance": prov,
        "supersedes": [], "supersededBy": [],
        "updatedAt": local, "updatedAt_utc": utc, "timezone": "Asia/Manila",
    }


def _seed_and_skip(sandbox, seeded_claims: list):
    """Helper: run installer + wet run + inject claims + trigger skip run. Return final JSON."""
    ws = sandbox["workspace"]
    write_source(ws, "MEMORY.md", "# MEMORY\nOperator prefers terse responses.\n")
    write_source(ws, "RTMEMORY.md", "# RTMEMORY\n\n")
    write_source(ws, "PROCEDURES.md", "# PROCEDURES\n\n")
    run_installer(sandbox, profile="business")

    build_fixtures(sandbox, profile="business", run_id="recall-r1")
    r1 = run_pipeline(sandbox, run_id="recall-r1")
    assert r1["status"] == "ok", f"seed run failed: {r1}"

    # Inject the test claims directly into purified-claims.jsonl.
    claims_path = sandbox["runtime_dir"] / "purified-claims.jsonl"
    with claims_path.open("a", encoding="utf-8") as f:
        for c in seeded_claims:
            f.write(json.dumps(c) + "\n")

    # Trigger skip with unchanged sources.
    build_fixtures(sandbox, profile="business", run_id="recall-r2")
    r2 = run_pipeline(sandbox, run_id="recall-r2")
    assert r2["status"] == "skipped", f"expected skip: {r2}"
    return r2


class TestRecallSurface(unittest.TestCase):
    def test_contested_beats_old_retire_candidate(self):
        """A recent contested claim (age 10d) outranks an old retire_candidate (age 200d)."""
        sandbox = make_sandbox()
        try:
            seeded = [
                _make_claim("cl-contested-recent", "contested", age_days=10, provenance_count=2,
                            text="Operator prefers Python for backend work."),
                _make_claim("cl-retire-old", "retire_candidate", age_days=200, provenance_count=1,
                            text="Operator uses abandoned CI tool XYZ."),
            ]
            result = _seed_and_skip(sandbox, seeded)
            recall = result.get("recallSurface")
            self.assertIsNotNone(recall, "recall should populate")
            self.assertEqual(
                recall["claimId"], "cl-contested-recent",
                f"contested-recent should win weighted scoring; got {recall}",
            )
            self.assertEqual(recall["status"], "contested")
            self.assertIn("recallScore", recall)
            self.assertGreater(recall["recallScore"], 0.0)
        finally:
            cleanup_sandbox(sandbox)

    def test_unresolved_beats_old_retire_candidate(self):
        """Unresolved (age 60d) outranks older retire_candidate (age 200d)."""
        sandbox = make_sandbox()
        try:
            seeded = [
                _make_claim("cl-unresolved-mid", "unresolved", age_days=60, provenance_count=1,
                            text="Question about escalation path remains open."),
                _make_claim("cl-retire-old", "retire_candidate", age_days=200, provenance_count=1,
                            text="Old claim whose sources are gone."),
            ]
            result = _seed_and_skip(sandbox, seeded)
            recall = result.get("recallSurface")
            self.assertIsNotNone(recall)
            self.assertEqual(
                recall["claimId"], "cl-unresolved-mid",
                f"unresolved should win over older retire_candidate; got {recall}",
            )
            self.assertEqual(recall["status"], "unresolved")
        finally:
            cleanup_sandbox(sandbox)

    def test_contested_beats_unresolved_and_retire_candidate(self):
        """All three present: contested wins despite being youngest."""
        sandbox = make_sandbox()
        try:
            seeded = [
                _make_claim("cl-contested-young", "contested", age_days=5, provenance_count=3,
                            text="Recent but contested claim."),
                _make_claim("cl-unresolved-mid", "unresolved", age_days=80, provenance_count=1,
                            text="Moderately old unresolved question."),
                _make_claim("cl-retire-ancient", "retire_candidate", age_days=300, provenance_count=1,
                            text="Ancient retired claim."),
            ]
            result = _seed_and_skip(sandbox, seeded)
            recall = result.get("recallSurface")
            self.assertIsNotNone(recall)
            self.assertEqual(
                recall["claimId"], "cl-contested-young",
                f"contested-young should win with 3x status-weight + 3x provenance; got {recall}",
            )
        finally:
            cleanup_sandbox(sandbox)


if __name__ == "__main__":
    unittest.main()
