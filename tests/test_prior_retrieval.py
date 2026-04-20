"""E: prior-claim lookup — the ranked lookup must surface topically-matching claims
even when they are not among the most recent ones."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from score_purifier import retrieve_prior_claims  # noqa: E402


class TestPriorLookup(unittest.TestCase):
    def _write_claims(self, path: Path, claims: list):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(json.dumps(c) for c in claims) + "\n")

    def test_relevance_beats_recency(self):
        import tempfile
        tmp = Path(tempfile.mkdtemp(prefix="ranker-test-"))
        try:
            claims_path = tmp / "purified-claims.jsonl"
            # Construct 60 prior claims. The first (oldest) is the most on-topic to
            # a cluster about "operator timezone". Naive recency would miss it.
            claims = []
            claims.append({
                "id": "cl-oldest-on-topic",
                "subject": "operator", "predicate": "has timezone", "object": "Asia/Manila",
                "text": "The operator's timezone is Asia/Manila.",
                "type": "fact", "status": "resolved", "primaryHome": "LTMEMORY.md",
                "provenance": [{"source": "MEMORY.md", "lineSpan": [1, 1], "capturedAt": "2020-01-01T00:00:00Z"}],
                "updatedAt": "2020-01-01T00:00:00+00:00",
            })
            # 59 off-topic but recent claims
            for i in range(59):
                claims.append({
                    "id": f"cl-recent-{i:03d}",
                    "subject": f"topic-{i}", "predicate": "notes", "object": None,
                    "text": f"Unrelated note about topic-{i}.",
                    "type": "fact", "status": "resolved", "primaryHome": "LTMEMORY.md",
                    "provenance": [{"source": "RTMEMORY.md", "lineSpan": [i, i], "capturedAt": f"2026-04-{(i % 30) + 1:02d}T00:00:00Z"}],
                    "updatedAt": f"2026-04-{(i % 30) + 1:02d}T00:00:00+00:00",
                })
            self._write_claims(claims_path, claims)

            # Cluster about operator timezone
            clusters = [{
                "cluster_id": "clust-tz-check",
                "candidates": [{
                    "candidate_id": "cand-1", "text": "Operator's timezone is Asia/Manila.",
                    "type_hint": "fact",
                    "source_refs": [{"source": "MEMORY.md", "line_span": [3, 3], "captured_at": "2026-04-20T00:00:00Z"}],
                    "pass_1_verdict": "promote", "pass_1_rationale": "", "compress_target": None,
                }],
                "cluster_hints": {
                    "shared_entities": ["Asia/Manila"],
                    "shared_subject": "operator",
                    "proposed_type": "fact",
                    "proposed_primary_home": "LTMEMORY.md",
                    "contradiction_candidates": [],
                },
            }]

            # With cap=10, the ranker must surface the old on-topic claim, not just recent ones.
            retrieved = retrieve_prior_claims(claims_path, clusters, cap=10)
            ids = [c["claim_id"] for c in retrieved]
            self.assertIn("cl-oldest-on-topic", ids, "ranked lookup must surface the topically-matching older claim")
            # The on-topic claim should rank near the top, not be drowned by recents.
            self.assertLess(ids.index("cl-oldest-on-topic"), 3,
                            f"on-topic claim should rank top-3; got index={ids.index('cl-oldest-on-topic')}")
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
