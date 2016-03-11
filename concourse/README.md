We are using [Concourse](http://concourse.ci/) to provide a CI and build environment for GPORCA.

Prerequisites:
- A running Concourse instance
- The fly cli, installed from Concourse
- An AWS account with an S3 bucket as a backing store
- SSH pull access for this codebase and [GPOS](https://github.com/greenplum-db/gpos)

To deploy the pipeline:
1. Copy the `concourse/vars_example.yml` and put in your secret keys associated with the above prerequisites
1. Run `fly --target=<YOURCONCOURSEURL> set-pipeline --pipeline=GPORCA --config=concourse/pipeline.yml --load-vars-from=<YOURSECRETSFROMSTEPONE>`
