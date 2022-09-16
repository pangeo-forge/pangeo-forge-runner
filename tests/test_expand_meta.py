import json
import subprocess

invocations = [
    {
        "repo": "https://github.com/pangeo-forge/riops-feedstock",
        "ref": "60a286406250cb53a6684c221553ce46432fcbf4",
        "meta": {
            "title": "RIOPS",
            "description": "Regional Ice Ocean Prediction System (RIOPS) data available on the Meteorological Service of Canada (MSC) Datamart",
            "pangeo_forge_version": "0.8.2",
            "pangeo_notebook_version": "2021.12.02",
            "recipes": [{"id": "riops", "object": "recipe:recipe"}],
            "provenance": {
                "providers": [
                    {
                        "name": "ECCC",
                        "description": "Environment and Climate Change Canada",
                        "roles": ["producer", "licensor"],
                        "url": "https://dd.weather.gc.ca/model_riops/",
                    }
                ],
                "license": "CC-BY-4.0",
            },
            "maintainers": [
                {
                    "name": "James Munroe",
                    "orcid": "0000-0002-4078-0852",
                    "github": "jmunroe",
                }
            ],
            "bakery": {"id": "pangeo-ldeo-nsf-earthcube"},
        },
    }
]


def test_expand_meta_json():
    for invocation in invocations:
        cmd = [
            "pangeo-forge-runner",
            "expand-meta",
            "--repo",
            invocation["repo"],
            "--ref",
            invocation["ref"],
            "--json",
        ]
        out = subprocess.check_output(cmd, encoding="utf-8")
        found_meta = False
        for l in out.splitlines():
            p = json.loads(l)
            if p["status"] == "completed":
                assert p["meta"] == invocation["meta"]
                found_meta = True
        assert found_meta


def test_expand_meta_no_json():
    for invocation in invocations:
        cmd = [
            "pangeo-forge-runner",
            "expand-meta",
            "--repo",
            invocation["repo"],
            "--ref",
            invocation["ref"],
        ]
        out = subprocess.check_output(cmd, encoding="utf-8")
        last_line = out.splitlines()[-1]
        assert json.loads(last_line) == invocation["meta"]
