"""
run_evals.py
------------
Runs every test prompt through the router and records:
- Did it route to the expected skill?
- Confidence score
- Latency

Outputs evals/results.json which the dashboard reads.

This is the "trust" layer: hiring managers see we measure routing
accuracy systematically, not just hope it works.
"""
import os
import json
import time
from pathlib import Path
from anthropic import Anthropic

from engine import load_skills, route_question


OUTPUT_PATH = Path(__file__).parent / "evals" / "results.json"


def run_all_evals(client: Anthropic) -> dict:
    """Run every test prompt for every skill, score routing accuracy."""
    skills = load_skills()
    results = []

    print(f"Running evals: {len(skills)} skills × ~10 prompts each")
    print("=" * 70)

    for skill_name, skill in skills.items():
        for prompt in skill.test_prompts:
            t0 = time.time()
            routing = route_question(client, skills, prompt)
            latency_ms = int((time.time() - t0) * 1000)

            predicted = routing.get("skill")
            passed = predicted == skill_name

            results.append({
                "expected_skill": skill_name,
                "prompt": prompt,
                "predicted_skill": predicted,
                "confidence": routing.get("confidence", 0.0),
                "reasoning": routing.get("reasoning", ""),
                "passed": passed,
                "latency_ms": latency_ms,
            })

            status = "PASS" if passed else "FAIL"
            print(f"  [{status}] {skill_name:32s} | {prompt[:50]:52s} | {latency_ms}ms")

    # ============ AGGREGATE METRICS ============
    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    accuracy = passed / total if total else 0
    avg_latency = sum(r["latency_ms"] for r in results) / total if total else 0
    avg_confidence = sum(r["confidence"] for r in results) / total if total else 0

    # Per-skill breakdown
    per_skill = {}
    for skill_name in skills:
        skill_results = [r for r in results if r["expected_skill"] == skill_name]
        if not skill_results:
            continue
        per_skill[skill_name] = {
            "display_name": skills[skill_name].display_name,
            "total": len(skill_results),
            "passed": sum(1 for r in skill_results if r["passed"]),
            "accuracy": sum(1 for r in skill_results if r["passed"]) / len(skill_results),
            "avg_latency_ms": int(sum(r["latency_ms"] for r in skill_results) / len(skill_results)),
        }

    summary = {
        "run_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
        "total_tests": total,
        "passed": passed,
        "failed": total - passed,
        "accuracy": round(accuracy, 4),
        "avg_latency_ms": int(avg_latency),
        "avg_confidence": round(avg_confidence, 3),
        "per_skill": per_skill,
        "results": results,
    }

    print("=" * 70)
    print(f"OVERALL: {passed}/{total} passed ({accuracy*100:.1f}%) | avg latency {int(avg_latency)}ms")

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(summary, indent=2))
    print(f"\nResults written to {OUTPUT_PATH}")

    return summary


if __name__ == "__main__":
    if not os.getenv("ANTHROPIC_API_KEY"):
        print("ERROR: Set ANTHROPIC_API_KEY environment variable.")
        exit(1)
    client = Anthropic()
    run_all_evals(client)
