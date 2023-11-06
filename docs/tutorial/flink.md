# Run a recipe on a Flink cluster on AWS EKS

`pangeo-forge-runner` supports baking your recipes on Apache Flink using
the [Apache Flink Runner](https://beam.apache.org/documentation/runners/flink/)
for Beam. After looking at various options, we have settled on supporting
Flink on Kubernetes using Apache's [Flink Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/).
This would allow baking recipes on *any* Kubernetes cluster!

In this tutorial, we'll bake a recipe on a Amazon [EKS](https://aws.amazon.com/eks/)
kubernetes cluster that we use for [integration tests](https://github.com/pangeo-forge/pangeo-forge-runner/tree/main/tests/integration)!

Current support is for the following versions:

| **pangeo-forge-runner<br>version** | **flink<br>operator<br>version** | **flink<br>version** |                  **apache<br>beam<br>version**                 |
|:----------------------------:|:--------------------------------:|:--------------------:|:--------------------------------------------------------------:|
| 0.9.1 | 1.5.0                            | 1.16                | 2.[47-51].0<br>(all versions listed [here](https://repo.maven.apache.org/maven2/org/apache/beam/beam-runners-flink-1.16/)) |


## Setting up EKS

You need an EKS cluster with [Apache Flink Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/)
installed. Setting that up is out of the scope for this tutorial, but you can find some
Terraform for that [here](https://github.com/pangeo-forge/pangeo-forge-cloud-federation).

## Setting up your local machine to execute `pangeo-forge-runner`

1. Install required tools on your machine.
   1. [aws](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
   2. [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl)
   3. [pangeo-forge-runner>=0.9.1](https://pypi.org/project/pangeo-forge-runner/)


2. Authenticate to `aws` by running `aws configure`. If you don't already have the
   AWS Access Keys, you might need to [create one](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey).


3. Ask your administrator to add your IAM user arn to the correct k8s `aws-auth` configuration. Then the admin will 
ask you to run a command to get EKS credentials locally that might look like this:

   ```bash
   $ AWS_PROFILE=<your-aws-profile> aws eks update-kubeconfig --name <cluster-name> --region <aws-cluster-region>
   ```

4. Verify everything is working by running the following command. You should see the `flink-kubernetes-operator` resources like below:

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

There are two runner configuration file formats you can use to tell recipes *where*
your data output or cached. In the examples below `{{job_name}}` will be templated for you based
on the configuration for `Bake.job_name` (TODO: point to other docs). Also notice we're going to store everything
in s3. There are other storage options as well (TODO: point to the other docs).

1. JSON configuration:

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

2. `traitlet` configuration:

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

Here's a quick example of something slightly more complicated where you're passing flink-specific configuration options and runner options. 
Note that `-f <runner_config.py>` would point to your `traitlet` or JSON configuration file we just talked about above:

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

Whether you put things in a config file or pass via CLI, it's your choice but please make sure you don't commit any AWS secrets unintentionally

## Running the recipe

Now let's run a recipe! First we need to find a public recipe. 
Let's reuse the one for integration tests: `"https://github.com/pforgetest/gpcp-from-gcs-feedstock.git"`.
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

You can add `Bake.prune=True` too if you want to only test the recipe and run the first two time steps.

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

## Monitoring Job Output

## Flink Memory Allocation Tricks and Trapdoors

Sometimes you'll have jobs fail on Flink with errors about not enough `off-heap` memory or the JVM being OOM killed. Here
are some configuration options to think about when running jobs

The `kind: FlinkDeployment` resource has a goal which is to spin up a job manager. The job manager has a goal which is
to spin up the task managers (depending on your `--FlinkOperatorBakery.parallelism` setting). In k8s land
you can get a sense for which deployment/pod is which by considering your `--Bake.job_name`:

```bash
$ kubectl -n default get pod

NAME                                             READY   STATUS    RESTARTS       AGE
pod/flink-kubernetes-operator-559fccd895-pfdwj   2/2     Running   2 (3d1h ago)   6d21h
# NOTE: the job manager here gets provisioned as `<Bake.job_name>-<k8s-resource-hash>`
pod/nz-5ftesting-66d8644f49-wnndr                1/1     Running   0              55m
# NOTE: the task managers always have a similar suffix depending on your 
# `--FlinkOperatorBakery.parallelism` setting. Here it was set to `--FlinkOperatorBakery.parallelism=2`
pod/nz-5ftesting-task-manager-1-1                1/1     Running   0              55m
pod/nz-5ftesting-task-manager-1-2                1/1     Running   0              55m
```

If we grok the first 10 lines of the job manager we get a nice ascii output