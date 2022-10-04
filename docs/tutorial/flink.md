# Run a recipe on a Flink cluster on AWS

`pangeo-forge-runner` supports baking your recipes on Apache Flink using
the [Apache Flink Runner](https://beam.apache.org/documentation/runners/flink/)
for Beam. After looking at various options, we have settled on supporting
Flink on Kubernetes using Apache's [Flink Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/).
This would allow baking recipes on *any* Kubernetes cluster!

In this tutorial, we'll bake a recipe on a Amazon [EKS](https://aws.amazon.com/eks/)
kubernetes cluster!

## Setting up the cluster

You need an EKS cluster with [Apache Flink Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
installed. Setting that up is out of the scope for this tutorial, but you can find some
useful terraform scripts for that [here](https://github.com/yuvipanda/pangeo-forge-cloud-federation/)
if you wish.

## Setting up your local machine

1. Install required tools on your machine.
   1. [aws](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
   2. [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl)

2. Authenticate to `aws` by running `aws configure`. If you don't already have the
   AWS Access Keys, you might need to [create one](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey).

3. Get credentials access to the kubernetes cluster by running
   `aws eks update-kubeconfig --name=<cluster-name> --region=<region>`.

   You can verify this works by running `kubectl get pod` - it should succeed,
   and show you that at least a `flink-kubernetes-operator` pod is running.

## Setting up configuration

Construct a `pangeo_forge_runner.py` that will have configuration on *where*
the data should *end up in*. In our case, we will use S3. You must already have
created an S3 bucket for this to work.

```python
# Let's put all our data into s3!
BUCKET_PREFIX = "s3://<bucket-name>/<some-prefix>/"

c.TargetStorage.fsspec_class = "s3fs.S3FileSystem"
# Target output should be partitioned by job id
c.TargetStorage.root_path = f"{BUCKET_PREFIX}/{{job}}/output"
c.TargetStorage.fsspec_args = {
    "key": "<your-aws-access-key>",
    "secret": "<your-aws-access-secret>",

}

c.InputCacheStorage.fsspec_class = c.TargetStorage.fsspec_class
c.InputCacheStorage.fsspec_args = c.TargetStorage.fsspec_args
# Input data cache should *not* be partitioned by job id, as we want to get the datafile
# from the source only once
c.InputCacheStorage.root_path = f"{BUCKET_PREFIX}/cache/input"

c.MetadataCacheStorage.fsspec_class = c.TargetStorage.fsspec_class
c.MetadataCacheStorage.fsspec_args = c.TargetStorage.fsspec_args
# Metadata cache should be per job, as kwargs changing can change metadata
c.MetadataCacheStorage.root_path = f"{BUCKET_PREFIX}/{{job}}/cache/metadata"
```


## Running the recipe

Now run a recipe!

```bash
pangeo-forge-runner bake --repo <url-to-github-repo> --ref <name-of-branch-or-commit-hash>
```

You can add `--prune` if you want to only test the recipe and run just the first
few steps.

This might take a minute to submit.

## Access the Flink Dashboard

After you run the `pangeo-forge-runner` command, amongst the many lines of output,
you should see something that looks like:

`You can run 'kubectl port-forward --pod-running-timeout=2m0s --address 127.0.0.1 <some-name> 0:8081' to make the Flink Dashboard available!`

If you copy the command provided in the message and run it, it should provide you
with a local address where the Flink Dashboard will be available!

```
$ kubectl port-forward --pod-running-timeout=2m0s --address 127.0.0.1 <some-name> 0:8081
Forwarding from 127.0.0.1:<some-number> -> 8081
```

Copy the `127.0.0.1:<some-number>` URL to your browser, and tada!
