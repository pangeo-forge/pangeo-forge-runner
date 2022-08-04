import json
import subprocess


def test_json_error():
    """
    Test that app exceptions still show up as JSON
    """
    # Do soemthing that causes an exception, and test we get that info out
    cmd = [
        "pangeo-forge-runner",
        "expand-meta",
        "--repo",
        "https://example.com",  # This isn't a valid repo
        "--json",
    ]
    proc = subprocess.run(cmd, encoding="utf-8", capture_output=True)
    assert proc.returncode == 1
    found_exception = False
    for l in proc.stdout.splitlines():
        p = json.loads(l)
        if p["status"] == "failed":
            assert "exc_info" in p
            found_exception = True
    assert found_exception
