import ast
from pathlib import Path


WRITER_DIRECTORY = Path(__file__).parents[2] / "shared" / "opensearch_index"
FORBIDDEN_IMPORT_ROOTS = {
    "app",
    "database",
    "flask",
    "flask_sqlalchemy",
    "harvester",
    "sqlalchemy",
}


def test_writer_package_has_no_application_or_orm_imports():
    violations = []

    for path in WRITER_DIRECTORY.glob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            imported_names = []
            if isinstance(node, ast.Import):
                imported_names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_names = [node.module]

            for name in imported_names:
                if name.split(".", 1)[0] in FORBIDDEN_IMPORT_ROOTS:
                    violations.append(f"{path.name}: {name}")

    assert violations == []
