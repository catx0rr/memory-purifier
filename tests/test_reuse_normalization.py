"""G: reworded-claim reuse via v1.4.0 normalization.

Guarantees trivial morphology + article/plural normalization reuses the
prior claim id (no duplicate, no false supersession).

Scenarios:
- Plural fold: subject "cat" -> "cats"
- Predicate morphology: predicate "prefers" -> "prefer"
- Leading article: subject "cat" -> "the cat"
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _helpers import build_fixtures, cleanup_sandbox, load_claims, make_sandbox, run_installer, run_pipeline, write_source  # noqa: E402


_DEFAULT_SCORES = {
    "semantic_cluster_confidence": 0.9, "canonical_clarity": 0.9,
    "provenance_strength": 0.9, "contradiction_pressure": 0.0,
    "freshness": 0.9, "confidence": 0.9,
    "route_fitness": 0.9, "supersession_confidence": 0.9,
}


def _make_claim_hook(subject: str, predicate: str, text_marker: str):
    """Return a claim_hook that overrides canonical subject/predicate for
    clusters whose candidate text contains the given marker. Other clusters
    fall back to the default hook behavior (None)."""
    def _hook(cluster):
        cand = cluster["candidates"][0]
        if text_marker.lower() not in cand["text"].lower():
            return None
        prov = [
            {"source": r["source"], "line_span": r["line_span"], "type": "direct", "captured_at": r["captured_at"]}
            for r in cand["source_refs"]
        ]
        return {
            "claim_id": "<new>",
            "source_cluster_id": cluster["cluster_id"],
            "scores": dict(_DEFAULT_SCORES),
            "canonical": {
                "type": "fact", "status": "resolved",
                "text": cand["text"][:120],
                "subject": subject,
                "predicate": predicate,
                "object": None,
                "primary_home": "LTMEMORY.md", "secondary_tags": [],
            },
            "provenance": prov,
            "contradictions": [],
            "supersedes": [],
            "superseded_by": [],
            "freshness_posture": "fresh",
            "confidence_posture": "high",
            "rationale": "reuse-normalization test",
            "route_rationale": "fact -> LTMEMORY",
        }
    return _hook


def _find_target(claims, subject_accept: set, predicate_accept: set):
    """Return the single claim whose subject+predicate fall in the accept sets."""
    matches = [
        c for c in claims
        if c.get("subject") in subject_accept and c.get("predicate") in predicate_accept
    ]
    return matches


class TestReuseNormalization(unittest.TestCase):

    def _run_pair(self, marker_r1: str, subj_r1: str, pred_r1: str,
                  seed_source_r2: str, marker_r2: str, subj_r2: str, pred_r2: str,
                  accept_subjects: set, accept_predicates: set):
        """Common harness: run 1 seeds the prior; run 2 re-words. Assert the id is reused."""
        sandbox = make_sandbox()
        try:
            ws = sandbox["workspace"]
            write_source(ws, "MEMORY.md", f"# MEMORY\n{marker_r1}\n")
            write_source(ws, "RTMEMORY.md", "# RTMEMORY\n\n")
            write_source(ws, "PROCEDURES.md", "# PROCEDURES\n\n")
            run_installer(sandbox, profile="business")

            build_fixtures(
                sandbox, profile="business", run_id="reuse-r1",
                claim_hook=_make_claim_hook(subj_r1, pred_r1, marker_r1),
            )
            r1 = run_pipeline(sandbox, run_id="reuse-r1")
            self.assertEqual(r1["status"], "ok", f"run1: {r1}")
            claims_1 = load_claims(sandbox)
            target_1 = _find_target(claims_1, {subj_r1}, {pred_r1})
            self.assertEqual(len(target_1), 1,
                             f"run1 should emit exactly one claim with subject={subj_r1}/predicate={pred_r1}; got {len(target_1)}: {target_1}")
            prior_id = target_1[0]["id"]

            # Mutate source so scope is non-empty in reconciliation run
            write_source(ws, "MEMORY.md", f"# MEMORY\n{marker_r1}\n\n{seed_source_r2}\n")
            build_fixtures(
                sandbox, profile="business", run_id="reuse-r2",
                claim_hook=_make_claim_hook(subj_r2, pred_r2, marker_r2),
            )
            r2 = run_pipeline(sandbox, mode="reconciliation", run_id="reuse-r2")
            self.assertEqual(r2["status"], "ok", f"run2: {r2}")

            claims_2 = load_claims(sandbox)
            # After reuse, there should be exactly one claim in the accept set,
            # carrying the prior id (reworded text updates in place).
            target_2 = _find_target(claims_2, accept_subjects, accept_predicates)
            self.assertEqual(
                len(target_2), 1,
                f"expected exactly one claim after reuse; got {len(target_2)}: "
                f"{[(c.get('id'), c.get('subject'), c.get('predicate')) for c in target_2]}",
            )
            self.assertEqual(
                target_2[0]["id"], prior_id,
                f"id reuse failed: got {target_2[0]['id']}, expected {prior_id}",
            )
            # No false supersession: prior_id must not appear as superseded anywhere,
            # and no claim should have prior_id in its supersedes list.
            for claim in claims_2:
                self.assertNotIn(
                    prior_id, claim.get("supersedes") or [],
                    f"false supersession: claim {claim.get('id')} supersedes {prior_id}",
                )
            self.assertNotEqual(
                target_2[0].get("status"), "superseded",
                "reused claim must not be marked superseded by its own in-place update",
            )
        finally:
            cleanup_sandbox(sandbox)

    def test_plural_fold_reuses_prior_id(self):
        self._run_pair(
            marker_r1="Operator has a cat named Felix.",
            subj_r1="cat", pred_r1="prefers",
            seed_source_r2="Operator prefers cats at home.",
            marker_r2="cats",
            subj_r2="cats", pred_r2="prefers",
            accept_subjects={"cat", "cats"},
            accept_predicates={"prefer", "prefers"},
        )

    def test_predicate_morphology_reuses_prior_id(self):
        self._run_pair(
            marker_r1="Operator prefers quiet workspaces.",
            subj_r1="operator", pred_r1="prefers",
            seed_source_r2="Operator prefer minimal noise.",
            marker_r2="minimal",
            subj_r2="operator", pred_r2="prefer",
            accept_subjects={"operator"},
            accept_predicates={"prefer", "prefers"},
        )

    def test_leading_article_reuses_prior_id(self):
        self._run_pair(
            marker_r1="Cat named Felix lives at home.",
            subj_r1="cat", pred_r1="lives at",
            seed_source_r2="The cat sleeps in the kitchen.",
            marker_r2="kitchen",
            subj_r2="the cat", pred_r2="lives at",
            accept_subjects={"cat", "the cat"},
            accept_predicates={"live at", "lives at"},
        )


if __name__ == "__main__":
    unittest.main()
