# Release

Releases are automated by the `release.yaml` GitHub Workflow,
which is triggered by tag events.

To cut a new release, those with push permissions to the repo, may run:

```console
git tag $VERSION
git push origin --tags
```

Where `$VERSION` is a three-element, dot-delimited semantic version of the form
`v{MAJOR}.{MINOR}.{PATCH}`, which is appropriately incremented from the prior tag.

And `origin` is assumed to be the remote corresponding to
`pangeo-forge/pangeo-forge-runner`.
