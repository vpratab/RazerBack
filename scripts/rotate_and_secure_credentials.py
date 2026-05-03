from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parent.parent
PRODUCTION_ROOT = REPO_ROOT / "RazerBack_Production"
DEFAULT_SCAN_ROOTS = [REPO_ROOT]
if PRODUCTION_ROOT.exists():
    DEFAULT_SCAN_ROOTS.append(PRODUCTION_ROOT)

TEXT_SUFFIXES = {
    ".py",
    ".ps1",
    ".cmd",
    ".bat",
    ".json",
    ".jsonl",
    ".toml",
    ".yaml",
    ".yml",
    ".md",
    ".log",
    ".txt",
    ".env",
    ".ini",
    ".cfg",
}
IGNORE_DIRS = {".git", "__pycache__", ".venv", "venv", "node_modules"}
ENV_TEMPLATE = "\n".join(
    [
        "OANDA_API_TOKEN=",
        "OANDA_ACCOUNT_ID=",
        "OANDA_ENVIRONMENT=practice",
        "",
    ]
)
GITIGNORE_LINES = [
    ".env",
    ".env.*",
    "!.env.template",
    "*.secret",
    "*.secrets",
    "live_engine.lock",
]
TEXT_PATTERNS = {
    "bearer_token_literal": re.compile(r"(?i)bearer\\s+[A-Za-z0-9_-]{20,}"),
    "oanda_token_assignment": re.compile(r"(?i)\\b(?:OANDA_API_TOKEN|OANDA_API_KEY)\\b\\s*=\\s*[^\\s#]{12,}"),
    "oanda_account_assignment": re.compile(r"(?i)\\bOANDA_ACCOUNT_ID\\b\\s*=\\s*[^\\s#]{6,}"),
}
SECRET_NAME_HINTS = ("token", "secret", "key", "account_id", "accountid")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and harden OANDA credential handling.")
    parser.add_argument("--report-path", default=str(REPO_ROOT / "output" / "credential_security_audit.json"))
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any hardcoded credential literal is found.")
    return parser.parse_args()


def is_text_candidate(path: Path) -> bool:
    if any(part in IGNORE_DIRS for part in path.parts):
        return False
    return path.suffix.lower() in TEXT_SUFFIXES or path.name.lower() in {".env", ".gitignore"}


def scan_python_literals(path: Path) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return findings

    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent

    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        value = node.value.strip()
        if len(value) < 12:
            continue
        parent = parents.get(node)
        target_name = ""
        if isinstance(parent, ast.Assign):
            target_names = [target.id for target in parent.targets if isinstance(target, ast.Name)]
            if target_names:
                target_name = target_names[0]
        if isinstance(parent, ast.AnnAssign) and isinstance(parent.target, ast.Name):
            target_name = parent.target.id
        normalized = target_name.lower()
        if normalized and any(hint in normalized for hint in SECRET_NAME_HINTS):
            findings.append(
                {
                    "path": str(path),
                    "line": getattr(node, "lineno", 0),
                    "kind": "python_secret_literal",
                    "target": target_name,
                }
            )
    return findings


def scan_text_patterns(path: Path) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return findings
    for lineno, line in enumerate(text.splitlines(), 1):
        for kind, pattern in TEXT_PATTERNS.items():
            if pattern.search(line):
                findings.append({"path": str(path), "line": lineno, "kind": kind})
    return findings


def scan_roots(roots: list[Path]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or not is_text_candidate(path):
                continue
            if path.suffix.lower() == ".py":
                findings.extend(scan_python_literals(path))
            findings.extend(scan_text_patterns(path))
    deduped = {(item["path"], item["line"], item["kind"]): item for item in findings}
    return list(deduped.values())


def ensure_gitignore(path: Path) -> None:
    existing_lines = []
    if path.exists():
        existing_lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    merged = list(existing_lines)
    existing_set = set(existing_lines)
    for entry in GITIGNORE_LINES:
        if entry not in existing_set:
            merged.append(entry)
    path.write_text("\n".join(merged).rstrip() + "\n", encoding="utf-8")


def ensure_env_template(path: Path) -> None:
    path.write_text(ENV_TEMPLATE, encoding="utf-8")


def main() -> None:
    args = parse_args()
    scan_roots_list = DEFAULT_SCAN_ROOTS
    ensure_gitignore(REPO_ROOT / ".gitignore")
    ensure_env_template(REPO_ROOT / ".env.template")
    if PRODUCTION_ROOT.exists():
        ensure_env_template(PRODUCTION_ROOT / ".env.template")

    findings = scan_roots(scan_roots_list)
    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "status": "ok" if not findings else "findings_present",
        "scan_roots": [str(root) for root in scan_roots_list],
        "finding_count": len(findings),
        "findings": findings,
        "env_template": str(REPO_ROOT / ".env.template"),
        "gitignore_updated": str(REPO_ROOT / ".gitignore"),
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Credential audit report: {report_path}")
    print(f"Findings: {len(findings)}")
    for finding in findings:
        print(f"{finding['path']}:{finding['line']} [{finding['kind']}]")

    if findings and args.strict:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
