# Concourse Pipeline Generation

To facilitate pipeline maintanance, a Python utility 'gen_pipeline.py`
is used to generate the production pipeline. It can also be used to build
custom pipelines for developer use.

The utility uses the [Jinja2](http://jinja.pocoo.org/) template
engine for Python. This allows the generation of portions of the
pipeline from common blocks of pipeline code. Logic (Python code) can
be embedded to further manipulate the generated pipeline.

You can think of the usage of this utility, supporting template and
generated pipeline file in similar terms to the autoconf, configure.in and
configure workflow.

## IMPORTANT

* Under no circumstances should any credentials be committed into
  public repos.
* The production pipeline should not be edited directly. Only the
  editing of the template should be performed.

## Workflow

The following workflow should be followed:

* Edit the template file (templates/gpdb-tpl.yml).
* Generate the pipeline.
* Use the Concourse `fly` command to set the pipeline (gpdb_master-generated.yml).
* Once the pipeline is validated to function properly, commit the updated template and pipeline.

## Requirements

### [Jinja2](http://jinja.pocoo.org/)
[Jinja2](http://jinja.pocoo.org/) can be readily installed using `easy_install` or `pip`.

You can install the most recent Jinja2 version using easy_install or pip:

```
easy_install Jinja2
pip install Jinja2
```

For in-depth information, please refer to it's documentation.

## Usage

### Help
The utility options can be retrieved with the following:
```
gen_pipeline.py -h|--help
```

## Example

### Create Production Pipeline

Use the following to generate a production pipeline
(`gpdb-prod.yml`). All supported platforms and test sections are
included. The output of the utility will provide details of the
pipeline generated. Following standard conventions, a basic `fly`
command is provided as output so the engineer can copy and paste this
into their terminal to set the pipeline.

```
$ ./gen_pipeline.py -t prod

======================================================================
  Generate Pipeline type: .. : prod
  Pipeline file ............ : gpdb_master-generated.yml
  Template file ............ : gpdb-tpl.yml
  OS Types ................. : ['centos6', 'centos7', 'sles', 'aix7', 'win', 'ubuntu16']
  Test sections ............ : ['ICW', 'CS', 'MPP', 'MM', 'DPM', 'UD', 'FileRep']
  test_trigger ............. : True
======================================================================

NOTE: You can now set the pipeline with the following:

fly -t gpdb-prod \
    set-pipeline \
    -p gpdb_master-generated \
    -c gpdb_master-generated.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml

```

The generated pipeline file `gpdb-prod.yml` will be set, validated and
ultimately committed (including the updated pipeline template) to the
source repository.

### Create Developer ICW pipeline for centos6 platform

Use the following to generate a developer ICW pipeline. NOTE: The
majority of jobs are only available for the `centos6` platform. The
generated pipeline and helper `fly` command are intended encourage the
engineer to set the pipeline with team (-t) and engineer (-u)
identifiable names.

```
$ ./gen_pipeline.py -t dpm -u curry -a DPM

======================================================================
  Generate Pipeline type: .. : dpm
  Pipeline file ............ : gpdb-dpm-curry.yml
  Template file ............ : gpdb-tpl.yml
  OS Types ................. : ['centos6']
  Test sections ............ : ['DPM']
  test_trigger ............. : True
======================================================================

NOTE: You can now set the pipeline with the following:

fly -t gpdb-dev \
    set-pipeline \
    -p gpdb-dpm-curry \
    -c gpdb-dpm-curry.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -v tf-bucket-path=dev/dpm/ \
    -v bucket-name=gpdb5-concourse-builds-dev

$ ./gen_pipeline.py -t cs -u durant -O centos6 sles -a ICW CS

======================================================================
  Generate Pipeline type: .. : cs
  Pipeline file ............ : gpdb-cs-durant.yml
  Template file ............ : gpdb-tpl.yml
  OS Types ................. : ['centos6', 'sles']
  Test sections ............ : ['ICW', 'CS']
  test_trigger ............. : True
======================================================================

NOTE: You can now set the pipeline with the following:

fly -t gpdb-dev \
    set-pipeline \
    -p gpdb-cs-durant \
    -c gpdb-cs-durant.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_common-ci-secrets.yml \
    -l ~/workspace/continuous-integration/secrets/gpdb_master-ci-secrets.yml \
    -v tf-bucket-path=dev/cs/ \
    -v bucket-name=gpdb5-concourse-builds-dev

```
