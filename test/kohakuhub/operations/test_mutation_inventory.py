"""Architecture checks for external mutation call boundaries."""

import ast
from pathlib import Path


ROOT = Path(__file__).parents[3]
SRC = ROOT / "src" / "kohakuhub"

LAKEFS_MUTATORS = {
    "commit",
    "upload_object",
    "delete_object",
    "create_repository",
    "delete_repository",
    "create_branch",
    "delete_branch",
    "create_tag",
    "delete_tag",
    "revert_branch",
    "merge_into_branch",
    "hard_reset_branch",
}
S3_DESTRUCTIVE = {"delete_object", "delete_objects"}
ALLOWED = {
    SRC / "lakefs_mutation_gateway.py",
    SRC / "storage_deletion_gateway.py",
    SRC / "lakefs_rest_client.py",
    SRC / "utils" / "s3.py",
}


def _calls(path: Path, names: set[str]) -> list[tuple[str, int, str]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    found: list[tuple[str, int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr in names:
            receiver = node.func.value
            if isinstance(receiver, ast.Name) and receiver.id in {
                "mutation_gateway",
                "deletion_gateway",
            }:
                continue
            found.append((str(path.relative_to(ROOT)), node.lineno, node.func.attr))
    return found


def test_external_mutators_have_one_auditable_gateway():
    offenders: list[tuple[str, int, str]] = []
    for path in SRC.rglob("*.py"):
        if path in ALLOWED:
            continue
        offenders.extend(_calls(path, LAKEFS_MUTATORS | S3_DESTRUCTIVE))

    assert offenders == [], (
        "external mutation calls must go through operations gateways: "
        f"{offenders}"
    )
