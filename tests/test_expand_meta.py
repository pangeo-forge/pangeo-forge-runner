import tempfile
import json
import os
import subprocess

invocations = [
    {
        "repo": "https://github.com/pangeo-forge/cmip6-feedstock",
        "ref": "63f544892df0f6c7c61bf372bfd52366cd885aa7",
        "meta": {"title": "CMIP6", "description": "CMIP6 datasets converted to zarr stores from ESGF files", "pangeo_forge_version": "0.8.3", "pangeo_notebook_version": "2021.12.02", "recipes": [{"id": "CMIP6.PMIP.MIROC.MIROC-ES2L.past1000.r1i1p1f2.Amon.tas.gn.v20200318"}, {"id": "CMIP6.PMIP.MRI.MRI-ESM2-0.past1000.r1i1p1f1.Amon.tas.gn.v20200120"}, {"id": "CMIP6.PMIP.MPI-M.MPI-ESM1-2-LR.past2k.r1i1p1f1.Amon.tas.gn.v20210714"}], "provenance": {"providers": [{"name": "ESGF", "description": "Earth System Grid Federation", "roles": ["producer", "licensor"], "url": "https://esgf-node.llnl.gov/projects/cmip6/"}], "license": "CC-BY-4.0"}, "maintainers": [{"name": "Julius Busecke", "orcid": "0000-0001-8571-865X", "github": "jbusecke"}, {"name": "Charles Stern", "orcid": "0000-0002-4078-0852", "github": "cisaacstern"}], "bakery": {"id": "pangeo-ldeo-nsf-earthcube"}}
    },
    {
        "repo": "https://github.com/pangeo-forge/gpcp-feedstock",
        "ref": "2cde04745189665a1f5a05c9eae2a98578de8b7f",
        "meta": {"title": "Global Precipitation Climatology Project", "description": "Global Precipitation Climatology Project (GPCP) Daily Version 1.3 gridded, merged ty satellite/gauge precipitation Climate data Record (CDR) from 1996 to present.\n", "pangeo_forge_version": "0.9.0", "pangeo_notebook_version": "2022.06.02", "recipes": [{"id": "gpcp", "object": "recipe:recipe"}], "provenance": {"providers": [{"name": "NOAA NCEI", "description": "National Oceanographic & Atmospheric Administration National Centers for Environmental Information", "roles": ["host", "licensor"], "url": "https://www.ncei.noaa.gov/products/global-precipitation-climatology-project"}, {"name": "University of Maryland", "description": "University of Maryland College Park Earth System Science Interdisciplinary Center (ESSIC) and Cooperative Institute for Climate and Satellites (CICS).\n", "roles": ["producer"], "url": "http://gpcp.umd.edu/"}], "license": "No constraints on data access or use."}, "maintainers": [{"name": "Ryan Abernathey", "orcid": "0000-0001-5999-4917", "github": "rabernat"}], "bakery": {"id": "pangeo-ldeo-nsf-earthcube"}}
    }
]


def test_expand_meta():
    for invocation in invocations:
        _, tmp = tempfile.mkstemp()
        try:
            cmd = [
                'pangeo-forge-runner',
                '--repo', invocation['repo'],
                '--ref', invocation['ref'],
                'expand-meta',
                '--out', tmp
            ]
            subprocess.check_call(cmd)
            with open(tmp) as f:
                assert json.load(f) == invocation['meta']

        finally:
            os.remove(tmp)


