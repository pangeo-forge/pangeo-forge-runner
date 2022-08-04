import json
import subprocess

invocations = [
    {
        "repo": "https://github.com/pangeo-forge/gpcp-feedstock",
        "ref": "2cde04745189665a1f5a05c9eae2a98578de8b7f",
        "meta": {
            "title": "Global Precipitation Climatology Project",
            "description": "Global Precipitation Climatology Project (GPCP) Daily Version 1.3 gridded, merged ty satellite/gauge precipitation Climate data Record (CDR) from 1996 to present.\n",
            "pangeo_forge_version": "0.9.0",
            "pangeo_notebook_version": "2022.06.02",
            "recipes": [{"id": "gpcp", "object": "recipe:recipe"}],
            "provenance": {
                "providers": [
                    {
                        "name": "NOAA NCEI",
                        "description": "National Oceanographic & Atmospheric Administration National Centers for Environmental Information",
                        "roles": ["host", "licensor"],
                        "url": "https://www.ncei.noaa.gov/products/global-precipitation-climatology-project",
                    },
                    {
                        "name": "University of Maryland",
                        "description": "University of Maryland College Park Earth System Science Interdisciplinary Center (ESSIC) and Cooperative Institute for Climate and Satellites (CICS).\n",
                        "roles": ["producer"],
                        "url": "http://gpcp.umd.edu/",
                    },
                ],
                "license": "No constraints on data access or use.",
            },
            "maintainers": [
                {
                    "name": "Ryan Abernathey",
                    "orcid": "0000-0001-5999-4917",
                    "github": "rabernat",
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
