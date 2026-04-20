"""N: prior-claim lookup v1.4.0 improvements exercised directly.

Tests retrieve_prior_claims as a library function:
- per-cluster top-K guarantees no cluster starves in multi-cluster runs
- min-score threshold filters sub-threshold noise
- cluster_hints.contradiction_candidates gets populated
- stopword filter prevents common-word inflation
- predicate signal surfaces prior claims with matching verbs
"""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

from score_purifier import retrieve_prior_claims, _tokens, _STOPWORDS  # noqa: E402


def _write_claims_jsonl(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _claim(cid: str, subject: str, predicate: str, text: str,
           home: str = "LTMEMORY.md", ctype: str = "fact",
           status: str = "resolved", updated_at: str = "2026-04-01T00:00:00+08:00") -> dict:
    return {
        "id": cid,
        "type": ctype,
        "status": status,
        "text": text,
        "subject": subject,
        "predicate": predicate,
        "object": None,
        "primaryHome": home,
        "provenance": [{"source": "MEMORY.md", "lineSpan": [1, 1], "type": "direct", "capturedAt": updated_at}],
        "updatedAt": updated_at,
    }


def _cluster(cluster_id: str, shared_subject: str, candidates_text: list,
             proposed_type: str = "fact", proposed_home: str = "LTMEMORY.md") -> dict:
    cands = []
    for i, text in enumerate(candidates_text):
        cands.append({
            "candidate_id": f"cand-{cluster_id}-{i}",
            "text": text,
            "type_hint": proposed_type,
            "source_refs": [{"source": "MEMORY.md", "line_span": [i + 1, i + 1], "captured_at": "2026-04-20T00:00:00+08:00"}],
            "pass_1_verdict": "promote",
            "pass_1_rationale": "test",
            "compress_target": None,
        })
    return {
        "cluster_id": cluster_id,
        "candidates": cands,
        "cluster_hints": {
            "shared_entities": [],
            "shared_subject": shared_subject,
            "proposed_type": proposed_type,
            "proposed_primary_home": proposed_home,
            "contradiction_candidates": [],
        },
    }


class TestLongHorizonLookup(unittest.TestCase):

    def test_stopwords_are_filtered(self):
        """_STOPWORDS is a non-empty set and _tokens strips them."""
        self.assertIn("the", _STOPWORDS)
        self.assertIn("is", _STOPWORDS)
        tokens = _tokens("The operator is based in Manila")
        self.assertNotIn("the", tokens)
        self.assertNotIn("is", tokens)
        self.assertNotIn("in", tokens)
        self.assertIn("operator", tokens)
        self.assertIn("manila", tokens)

    def test_per_cluster_budget_prevents_starvation(self):
        """Three distinct-subject clusters each receive their own top-K priors."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-longhorizon-"))
        try:
            claims_path = tmp / "purified-claims.jsonl"
            records = []
            for i in range(20):
                records.append(_claim(f"cl-python-{i}", "python",
                                      "prefers" if i % 2 == 0 else "uses",
                                      f"Operator uses Python framework {i} for backend work."))
            for i in range(20):
                records.append(_claim(f"cl-rust-{i}", "rust",
                                      "prefers" if i % 2 == 0 else "writes",
                                      f"Operator writes Rust crate {i} for systems code."))
            for i in range(20):
                records.append(_claim(f"cl-ts-{i}", "typescript",
                                      "prefers" if i % 2 == 0 else "uses",
                                      f"Operator uses TypeScript library {i} for frontend."))
            _write_claims_jsonl(claims_path, records)

            clusters = [
                _cluster("clu-py", "python",
                         ["Operator prefers Python for new backend services.",
                          "Python is the primary language for internal tooling."]),
                _cluster("clu-rs", "rust",
                         ["Operator writes Rust for performance-critical crates."]),
                _cluster("clu-ts", "typescript",
                         ["Operator uses TypeScript for frontend components."]),
            ]

            result = retrieve_prior_claims(claims_path, clusters, per_cluster=5, min_score=0.5, cap=50)
            self.assertGreater(len(result), 0, "result should not be empty")
            self.assertLessEqual(len(result), 50, "global cap should be respected")

            for c in clusters:
                cand_ids = c["cluster_hints"]["contradiction_candidates"]
                self.assertGreater(
                    len(cand_ids), 0,
                    f"cluster {c['cluster_id']} should receive at least one contradiction candidate (no starvation)",
                )
                if c["cluster_id"] == "clu-py":
                    self.assertTrue(all(cid.startswith("cl-python-") for cid in cand_ids),
                                    f"python cluster candidates should be python priors; got {cand_ids}")
                elif c["cluster_id"] == "clu-rs":
                    self.assertTrue(all(cid.startswith("cl-rust-") for cid in cand_ids),
                                    f"rust cluster candidates should be rust priors; got {cand_ids}")
                elif c["cluster_id"] == "clu-ts":
                    self.assertTrue(all(cid.startswith("cl-ts-") for cid in cand_ids),
                                    f"typescript cluster candidates should be typescript priors; got {cand_ids}")

            result_ids = {c["claim_id"] for c in result}
            python_hit = any(i.startswith("cl-python-") for i in result_ids)
            rust_hit = any(i.startswith("cl-rust-") for i in result_ids)
            ts_hit = any(i.startswith("cl-ts-") for i in result_ids)
            self.assertTrue(python_hit and rust_hit and ts_hit,
                            f"union should contain priors from all 3 pools; got {sorted(result_ids)[:10]}...")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_min_score_threshold_filters_noise(self):
        """Priors that only share stopwords with the cluster should not slip through."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-minscore-"))
        try:
            claims_path = tmp / "purified-claims.jsonl"
            relevant = _claim("cl-relevant", "operator", "prefers",
                              "Operator prefers Python for backend.")
            # Noise claims: stopwords-only text, unrelated subject/predicate,
            # AND a different home+type so they don't get home/type affinity
            # boosts. They should score below the min_score threshold.
            noise = [
                _claim(f"cl-noise-{i}", f"unrelated-subject-{i}", "is",
                       "The is a an in on at.",
                       home="PLAYBOOKS.md", ctype="method")
                for i in range(5)
            ]
            _write_claims_jsonl(claims_path, [relevant] + noise)

            clusters = [_cluster("clu-py", "operator",
                                 ["Operator prefers Python for new backend services."])]
            result = retrieve_prior_claims(claims_path, clusters, per_cluster=10, min_score=0.5, cap=50)
            result_ids = {c["claim_id"] for c in result}
            self.assertIn("cl-relevant", result_ids, "relevant prior must be returned")
            for noise_id in (f"cl-noise-{i}" for i in range(5)):
                self.assertNotIn(noise_id, result_ids,
                                 f"noise prior {noise_id} should be filtered by min_score")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_predicate_signal_surfaces_matching_verb(self):
        """A prior whose predicate tokens appear in the cluster text gets a predicate-arm boost."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-predicate-"))
        try:
            claims_path = tmp / "purified-claims.jsonl"
            pred_match = _claim("cl-pred-match", "different-subject", "prefers",
                                "Claim body mentions preferences.")
            irrelevant = _claim("cl-unrelated", "xyz", "computes",
                                "Irrelevant claim text about computations.")
            _write_claims_jsonl(claims_path, [pred_match, irrelevant])

            clusters = [_cluster("clu-pref", "operator",
                                 ["Operator prefers Python for backend."])]
            result = retrieve_prior_claims(claims_path, clusters, per_cluster=5, min_score=0.5, cap=50)
            result_ids = {c["claim_id"] for c in result}
            self.assertIn("cl-pred-match", result_ids,
                          f"predicate signal should surface the prefers prior; got {result_ids}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
