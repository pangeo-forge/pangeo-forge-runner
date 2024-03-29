# Glossary

A glossary of common terms used throughout pangeo-forge-runner.

```{glossary}
`job_id`
   An autogenerated, non-human readable unique ID that represents a particular beam
   job. These are generated by the beam runner on submission, not set by the submitting user.
   
   For some bakeries (such as flink), the beam runner does *not* generate a unique ID during submission.
   In those cases, this will be the same as {term}`job_name`
   
   As this is only known *after* job submission, this is not available for template expansion in
   the job specification - so you can not use {job_id} in various TargetPaths, for example.
   
`job_name`
    A human readable, human set, but not necessarily globally unique ID that represents a particular
    beam job. These are set on the commandline with `--Bake.job_name=test-flink` (or similar traitlets
    config). If not set, pangeo-forge-runner will try to automatically generate a descriptive name.
    
    These can only contain lower case characters (a-z), digits (0-9) and dashes (-).
```
