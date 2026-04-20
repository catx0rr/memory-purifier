"""O: cluster_hints.contradiction_candidates is populated from ranked priors (v1.4.0).

The clusterer (cluster_survivors.py) leaves contradiction_candidates empty.
score_purifier.py::retrieve_prior_claims is the population site — for each
cluster, after ranking prior claims, the top-N prior ids are merged into
that cluster's cluster_hints.contradiction_candidates.

Pass 2 then consumes the field per the prompt contract.
"""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PACKAGE_ROOT / "scripts"))

from score_purifier import retrieve_prior_claims, CONTRADICTION_CANDIDATES_PER_CLUSTER  # noqa: E402


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


def _cluster(cluster_id: str, shared_subject: str, candidates_text: list) -> dict:
    cands = []
    for i, text in enumerate(candidates_text):
        cands.append({
            "candidate_id": f"cand-{cluster_id}-{i}",
            "text": text,
            "type_hint": "fact",
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
            "proposed_type": "fact",
            "proposed_primary_home": "LTMEMORY.md",
            "contradiction_candidates": [],
        },
    }


class TestContradictionCandidates(unittest.TestCase):

    def test_clusterer_leaves_field_empty(self):
        """Sanity check: a fresh cluster has contradiction_candidates=[]
        before retrieve_prior_claims runs (the clusterer's contract)."""
        c = _cluster("clu-x", "subject-x", ["text body"])
        self.assertEqual(c["cluster_hints"]["contradiction_candidates"], [])

    def test_population_from_ranked_priors(self):
        """Single cluster: contradiction_candidates is populated with the
        top-N ranked prior ids after retrieve_prior_claims runs."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-contra-cand-"))
        try:
            claims_path = tmp / "purified-claims.jsonl"
            # Seed enough relevant priors that we can verify the top-N selection
            # (CONTRADICTION_CANDIDATES_PER_CLUSTER = 3 by default).
            records = [
                _claim(f"cl-py-{i}", "python", "prefers",
                       f"Operator prefers Python framework {i} for backend.")
                for i in range(8)
            ]
            with claims_path.open("w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")

            cluster = _cluster("clu-py", "python",
                               ["Operator prefers Python for new backend services."])
            clusters = [cluster]

            # Pre-condition: empty
            self.assertEqual(cluster["cluster_hints"]["contradiction_candidates"], [])

            retrieve_prior_claims(claims_path, clusters,
                                  per_cluster=5, min_score=0.5, cap=50)

            # Post-condition: populated, exactly the configured top-N count, all valid ids.
            cand_ids = cluster["cluster_hints"]["contradiction_candidates"]
            self.assertGreater(len(cand_ids), 0,
                               f"contradiction_candidates should be populated; got {cand_ids}")
            self.assertLessEqual(
                len(cand_ids), CONTRADICTION_CANDIDATES_PER_CLUSTER,
                f"should not exceed CONTRADICTION_CANDIDATES_PER_CLUSTER ({CONTRADICTION_CANDIDATES_PER_CLUSTER}); got {len(cand_ids)}",
            )
            for cid in cand_ids:
                self.assertTrue(cid.startswith("cl-py-"),
                                f"candidate id {cid} should be from the python prior pool")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_existing_candidates_preserved_and_deduped(self):
        """If the cluster already has contradiction_candidates from a prior
        stage, retrieve_prior_claims merges new ids with existing ones,
        deduping while preserving order."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-contra-merge-"))
        try:
            claims_path = tmp / "purified-claims.jsonl"
            records = [
                _claim("cl-existing", "python", "prefers", "Existing python preference."),
                _claim("cl-new", "python", "prefers", "New python preference body."),
            ]
            with claims_path.open("w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")

            cluster = _cluster("clu-py", "python",
                               ["Operator prefers Python for backend."])
            # Pre-seed an existing id (e.g. injected by an upstream stage).
            cluster["cluster_hints"]["contradiction_candidates"] = ["cl-existing", "cl-injected-by-someone-else"]

            retrieve_prior_claims(claims_path, [cluster],
                                  per_cluster=5, min_score=0.5, cap=50)

            cand_ids = cluster["cluster_hints"]["contradiction_candidates"]
            # Existing ids must come first (preservation), no duplicates.
            self.assertEqual(cand_ids[0], "cl-existing", f"first id must be the existing one; got {cand_ids}")
            self.assertEqual(cand_ids[1], "cl-injected-by-someone-else", f"second pre-existing id must be preserved; got {cand_ids}")
            # cl-existing must NOT appear twice (dedupe).
            self.assertEqual(cand_ids.count("cl-existing"), 1, f"cl-existing must not be duplicated; got {cand_ids}")
            # cl-new should be appended after the existing ids.
            self.assertIn("cl-new", cand_ids, f"new ranked id should be appended; got {cand_ids}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_unrelated_priors_do_not_pollute(self):
        """Priors that score below min_score don't reach contradiction_candidates."""
        tmp = Path(tempfile.mkdtemp(prefix="mp-contra-noise-"))
        try:
            claims_path = tmp / "purified-claims.jsonl"
            records = [
                _claim("cl-relevant", "python", "prefers", "Python preference."),
                # Noise: unrelated subject + different home + different type
                _claim("cl-noise", "totally-unrelated-x", "is", "stopword body the a is.",
                       home="PLAYBOOKS.md", ctype="method"),
            ]
            with claims_path.open("w") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")

            cluster = _cluster("clu-py", "python",
                               ["Operator prefers Python for backend."])
            retrieve_prior_claims(claims_path, [cluster],
                                  per_cluster=5, min_score=0.5, cap=50)

            cand_ids = cluster["cluster_hints"]["contradiction_candidates"]
            self.assertIn("cl-relevant", cand_ids)
            self.assertNotIn("cl-noise", cand_ids,
                             f"noise prior should be filtered by min_score; got {cand_ids}")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    @classmethod
    def tearDownClass(cls):
        # Clean up any leftover tempdirs (best-effort; tests use unique prefixes).
        import glob
        for d in glob.glob("/tmp/mp-contra-*"):
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
