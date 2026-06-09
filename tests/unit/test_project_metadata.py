from __future__ import annotations

from pathlib import Path

import yaml
from packaging.specifiers import SpecifierSet
from packaging.version import Version

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_package_dbt_version_range_includes_fusion():
    project = yaml.safe_load((REPO_ROOT / "dbt_project.yml").read_text(encoding="utf-8"))
    version_range = project["require-dbt-version"]

    specifier = SpecifierSet(version_range)
    assert specifier.contains(Version("1.10.0"))
    assert specifier.contains(Version("2.0.0"))
    assert not specifier.contains(Version("1.9.9"))
    assert not specifier.contains(Version("3.0.0"))


def test_workflows_parse_with_dbt_fusion():
    expected_parse = "dbtf parse --profiles-dir tests/ci_profiles --no-version-check"

    for workflow_path in (
        REPO_ROOT / ".github/workflows/ci.yml",
        REPO_ROOT / ".github/workflows/release.yml",
    ):
        workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
        assert workflow["env"]["DBT_FUSION_VERSION"].startswith("2.0.0")
        assert workflow["env"]["DBT_FUSION_TARGET"] == "x86_64-unknown-linux-gnu"

        runs = []
        for job in workflow["jobs"].values():
            for step in job.get("steps", []):
                if "run" in step:
                    runs.append(step["run"])

        assert expected_parse in runs
