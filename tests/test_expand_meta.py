import json
import subprocess

invocations = [
    {
        "repo": "https://github.com/pforgetest/gpcp-from-gcs-feedstock.git",
        "ref": "4f41e02512b2078c8bdb286368a1a9d878b5cec2",
        "meta": {
            "title": "Global Precipitation Climatology Project",
            "description": "Global Precipitation Climatology Project (GPCP) Daily Version 1.3 gridded, merged ty satellite/gauge precipitation Climate data Record (CDR) from 1996 to present.\n",
            "pangeo_forge_version": "0.9.0",
            "pangeo_notebook_version": "2022.06.02",
            "recipes": [{"id": "gpcp-from-gcs", "object": "recipe:recipe"}],
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
                    "name": "Charles Stern",
                    "orcid": "0000-0002-4078-0852",
                    "github": "cisaacstern",
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
