# Run a recipe on a Flink cluster on AWS EKS

`pangeo-forge-runner` supports baking your recipes on Apache Flink using
the [Apache Flink Runner](https://beam.apache.org/documentation/runners/flink/)
for Beam. After looking at various options, we have settled on supporting
Flink on Kubernetes using Apache's [Flink Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/).
This would allow baking recipes on *any* Kubernetes cluster!

In this tutorial, we'll bake a recipe on a Amazon [EKS](https://aws.amazon.com/eks/)
kubernetes cluster that we use for [integration tests](https://github.com/pangeo-forge/pangeo-forge-runner/tree/main/tests/integration)!

Current support is for the following versions:

| **pangeo<br>forge<br>runner<br>branch** | **flink<br>operator<br>version** | **flink<br>version** |                  **apache<br>beam<br>version**                 |
|:----------------------------:|:--------------------------------:|:--------------------:|:--------------------------------------------------------------:|
| main                 | 1.5.0                            | 1.16                | 2.[47-51].0<br>(all versions listed https://repo.maven.apache.org/maven2/org/apache/beam/beam-runners-flink-1.16/) |


## Setting up EKS

You need an EKS cluster with [Apache Flink Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
installed. Setting that up is out of the scope for this tutorial, but you can find some
Terraform for that [here](https://github.com/pangeo-forge/pangeo-forge-cloud-federation).

## Setting up your local machine to execute `pangeo-forge-runner`

1. Install required tools on your machine.
   1. [aws](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
   2. [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl)
   3. [pangeo-forge-runner](https://pypi.org/project/pangeo-forge-runner/)


2. Authenticate to `aws` by running `aws configure`. If you don't already have the
   AWS Access Keys, you might need to [create one](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey).


3. Ask your administrator to add your IAM user arn to the correct k8s `aws-auth` configuration. They should then
ask you to run a command to get EKS credentials that looks something like this:

   ```bash
   $ AWS_PROFILE=<your-aws-profile> aws eks update-kubeconfig --name <cluster-name> --region <aws-cluster-region>
   ```

4. Verify everything is working by running the following command and you should see something similar below:

   ```bash
   $ kubectl -n default get flinkdeployment,deploy,pod,svc
   
   NAME                                        READY   UP-TO-DATE   AVAILABLE   AGE
   deployment.apps/flink-kubernetes-operator   1/1     1            1           15d

   NAME                                             READY   STATUS    RESTARTS        AGE
   pod/flink-kubernetes-operator-559fccd895-pfdwj   2/2     Running   2 (2d21h ago)   6d17h

   NAME                                     TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)             AGE
   service/flink-operator-webhook-service   ClusterIP   10.100.231.230   <none>        443/TCP             20d
   service/kubernetes                       ClusterIP   10.100.0.1       <none>        443/TCP             20d
   ```

## Setting up runner configuration

There are two file formats for constructing runner configuration that tell recipes *where*
the data should output and be cached. In the examples below `{{job_name}}` will be templated for you based
on the configuration for `Bake.job_name` (TODO: point to other docs). Also notice we going to store everything
in s3. There are other storage options as well (TODO: point to the other docs).

0. JSON configuration:

   ```json
   {
     "TargetStorage": {
       "fsspec_class": "s3fs.S3FileSystem",
       "fsspec_args": {
          "key": "<your-aws-access-key>",
          "secret": "<your-aws-access-secret>",
          "client_kwargs":{"region_name":"<your-aws-bucket-region>"}
        },
       // Target output should be partitioned by `{{job_name}}`
       "root_path": "s3://<bucket-name>/<some-prefix>/{{job_name}}/output"
     },
     "InputCacheStorage": {
       "fsspec_class": "s3fs.S3FileSystem",
         "fsspec_args": {
          "key": "<your-aws-access-key>",
          "secret": "<your-aws-access-secret>",
          "client_kwargs":{"region_name":"<your-aws-bucket-region>"}
        },
       // Input data cache should *not* be partitioned by `{{job_name}}`, as we want to get the datafile from the source only once
       "root_path": "s3://<bucket-name>/<some-prefix>/input/cache"
     },
     "MetadataCacheStorage": {
       "fsspec_class": "s3fs.S3FileSystem",
         "fsspec_args": {
          "key": "<your-aws-access-key>",
          "secret": "<your-aws-access-secret>",
          "client_kwargs":{"region_name":"<your-aws-bucket-region>"}
        },
       // Metadata cache should be per `{{job_name}}`, as kwargs changing can change metadata
       "root_path": "s3://<bucket-name>/<some-prefix>/{{job_name}}/metadata"
     }
   }
   ```

1. `traitlet` configuration:

   ```python
   BUCKET_PREFIX = "s3://<bucket-name>/<some-prefix>/"

   c.TargetStorage.fsspec_class = "s3fs.S3FileSystem"
   # Target output should be partitioned by `{{job_name}}`
   c.TargetStorage.root_path = f"{BUCKET_PREFIX}/{{job_name}}/output"
   c.TargetStorage.fsspec_args = {
       "key": "<your-aws-access-key>",
       "secret": "<your-aws-access-secret>",
       "client_kwargs":{"region_name":"<your-aws-bucket-region>"}
   }

   c.InputCacheStorage.fsspec_class = c.TargetStorage.fsspec_class
   c.InputCacheStorage.fsspec_args = c.TargetStorage.fsspec_args
   # Input data cache should *not* be partitioned by `{{job_name}}`, as we want to get the datafile from the source only once
   c.InputCacheStorage.root_path = f"{BUCKET_PREFIX}/cache/input"

   c.MetadataCacheStorage.fsspec_class = c.TargetStorage.fsspec_class
   c.MetadataCacheStorage.fsspec_args = c.TargetStorage.fsspec_args
   # Metadata cache should be per `{{job_name}}`, as kwargs changing can change metadata
   c.MetadataCacheStorage.root_path = f"{BUCKET_PREFIX}/{{job_name}}/cache/metadata"
   ```

A [subset of the configuration schema](https://github.com/pangeo-forge/pangeo-forge-recipes/blob/main/pangeo_forge_recipes/injections.py) 
gets dependency injected into the recipe by the runner. 

Various other runner options (TODO: talked about here and flink-specific here) can also be put into these file configurations or passed
directly during CLI `bake` calls. 

An example of something slightly more detailed where `-f <runner_config.py>` would point to a `traitlet` configuration file talked about above:

   ```bash
   pangeo-forge-runner bake \
       --repo=https://github.com/ranchodeluxe/gpcp-from-gcs-feedstock.git  \
       --ref="test/integration" \
       -f /Users/ranchodeluxe/apps/gpcp-from-gcs-feedstock/feedstock/runner_config.py \
       --FlinkOperatorBakery.job_manager_resources='{"memory": "6144m", "cpu": 1.0}' \
       --FlinkOperatorBakery.task_manager_resources='{"memory": "6144m", "cpu": 1.0}' \
       --FlinkOperatorBakery.flink_configuration='{"taskmanager.numberOfTaskSlots": "1", "taskmanager.memory.flink.size": "3072m", "taskmanager.memory.task.off-heap.size": "1024m", "taskmanager.memory.jvm-overhead.max": "4096m"}' \
       --FlinkOperatorBakery.parallelism=1 \
       --FlinkOperatorBakery.flink_version="1.16" \
       --Bake.job_name=gpcp \
       --Bake.container_image='apache/beam_python3.9_sdk:2.50.0' \
       --Bake.bakery_class="pangeo_forge_runner.bakery.flink.FlinkOperatorBakery"
   ```

Where you put things is your choice but please make sure you don't commit any AWS secrets into GH 

## Running the recipe

Now let's run a recipe! First we need to find a public recipe. Let's reuse the one for intergration tests: `"https://github.com/pforgetest/gpcp-from-gcs-feedstock.git"` 
Below is the minimal required args for running Flink:

   ```bash
   pangeo-forge-runner bake \
       --repo=https://github.com/pforgetest/gpcp-from-gcs-feedstock.git  \
       --ref="main" \
       -f <path-to-your-runner-config>.<json||py>
       --FlinkOperatorBakery.flink_version="1.16" \
       --Bake.job_name=gpcp \
       --Bake.container_image='apache/beam_python3.9_sdk:2.47.0' \
       --Bake.bakery_class="pangeo_forge_runner.bakery.flink.FlinkOperatorBakery"
   ```

You can add Bake.prune=True` if you want to only test the recipe and run just the first
few steps.

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


## Some thoughts about Flink Memory Allocation
