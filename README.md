<p align="center">
<img src="./logo_cloudberry_database.png"><br>
<strong>Next Generation Unified Database for Analytics and AI</strong>
</p>

[![Slack](https://img.shields.io/badge/Join_Slack-6a32c9)](https://communityinviter.com/apps/cloudberrydb/welcome)
[![Twitter Follow](https://img.shields.io/twitter/follow/cloudberrydb)](https://twitter.com/cloudberrydb)
[![Website](https://img.shields.io/badge/Visit%20Website-eebc46)](https://cloudberrydb.org)
[![GitHub Discussions](https://img.shields.io/github/discussions/cloudberrydb/cloudberrydb)](https://github.com/orgs/cloudberrydb/discussions)
![GitHub commit activity(branch)](https://img.shields.io/github/commit-activity/m/cloudberrydb/cloudberrydb)
![GitHub contributors](https://img.shields.io/github/contributors/cloudberrydb/cloudberrydb)
![GitHub License](https://img.shields.io/github/license/cloudberrydb/cloudberrydb)
![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fcloudberrydb%2Fcloudberrydb.svg?type=shield)
[![cbdb pipeline](https://github.com/cloudberrydb/cloudberrydb/actions/workflows/build.yml/badge.svg)](https://github.com/cloudberrydb/cloudberrydb/actions/workflows/build.yml)

---------

Cloudberry Database (CBDB) is shipped with PostgreSQL 14.4 as its
kernel and is forked from Greenplum Database 6, which serves as our
code base.

## Features

Cloudberry Database is 100% compatible with Greenplum, and provides
all the Greenplum features you need. In addition, Cloudberry Database
possesses some features that Greenplum currently lacks or does not
support. Visit this [feature comparison
doc](https://cloudberrydb.org/docs/cbdb-vs-gp-features) for details.

## Code layout

The directory layout of the repository follows the same general layout
as upstream PostgreSQL. There are changes compared to PostgreSQL
throughout the codebase, but a few larger additions worth noting:

* __gpMgmt/__ : Contains CloudberryDB-specific command-line tools for
    managing the cluster. Scripts like gpinit, gpstart, and gpstop
    live here. They are mostly written in Python.

* __gpAux/__ : Contains CloudberryDB-specific release management
    scripts, and vendored dependencies. Some additional directories
    are submodules and will be made available over time.

* __gpcontrib/__ : Much like the PostgreSQL contrib/ directory, this
    directory contains extensions such as gpfdist, PXF and gpmapreduce
    which are CloudberryDB-specific.

* __doc/__ : In PostgreSQL, the user manual lives here. In Cloudberry
    Database, the user manual is maintained separately at [Cloudberry
    Database Website
    Repo](https://github.com/cloudberrydb/cloudberrydb-site/tree/main).

* __hd-ci/__ : Contains configuration files for the CBDB continuous
    integration system.

* __src/__

  * __src/backend/cdb/__ : Contains larger CloudberryDB-specific
    backend modules. For example, communication between segments,
    turning plans into parallelizable plans, mirroring, distributed
    transaction and snapshot management, etc. __cdb__ stands for
    __Cluster Database__ - it was a workname used in the early
    days. That name is no longer used, but the __cdb__ prefix remains.

  * __src/backend/gpopt/__ : Contains the so-called __translator__
    library, for using the GPORCA optimizer with Cloudberry
    Database. The translator library is written in C++ code, and
    contains glue code for translating plans and queries between the
    DXL format used by GPORCA, and the PostgreSQL internal
    representation.

  * __src/backend/gporca/__ : Contains the GPORCA optimizer code and
    tests. This is written in C++. See
    [README.md](src/backend/gporca/README.md) for more information and
    how to unit-test GPORCA.

  * __src/backend/fts/__ : FTS is a process that runs in the
    coordinator node, and periodically polls the segments to maintain
    the status of each segment.

## Building Cloudberry Database

You can follow [these guides](./readmes) to build the Cloudberry
Database on Linux OS(including CentOS, RHEL, and Ubuntu) and macOS.

## Documentation

For Cloudberry Database documentation, please check the [documentation
website](https://cloudberrydb.org/docs/). Our documents are still in
construction, welcome to help. If you're interested in [document
contribution](https://cloudberrydb.org/contribute/doc), you can submit
the pull request
[here](https://github.com/cloudberrydb/cloudberrydb-site/tree/main/docs).

We also recommend you take [PostgreSQL
Documentation](https://www.postgresql.org/docs/) and [Greenplum
Documentation](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/landing-index.html#differences-compared-to-open-source-greenplum-database)
as quick references.

## Contribution

Cloudberry Database is maintained actively by a group of community
database experts by individuals and companies. We believe in the
Apache Way "Community Over Code" and we want to make Cloudberry
Database a community-driven project.

Contributions can be diverse, such as code enhancements, bug fixes,
feature proposals, documents, marketing, and so on. No contribution is
too small, we encourage all types of contributions. Cloudberry
Database community welcomes contributions from anyone, new and
experienced! Our [contribution
guide](https://cloudberrydb.org/contribute/how-to-contribute) will
help you get started with the contribution.

| Type | Description |
|----|---------------|
| Code contribution | Learn how to contribute code to the Cloudberry Database, including coding preparation, conventions, workflow, review, and checklist following the [code contribution guide](https://cloudberrydb.org/contribute/code).|
| Submit the proposal | Proposing major changes to Cloudberry Database through [proposal guide](https://cloudberrydb.org/contribute/proposal).|
| Doc contribution | We need you to join us to help us improve the documentation, see the [doc contribution guide](https://cloudberrydb.org/contribute/doc).|

For better collaboration, it's important for developers to learn how
to work well with Git and GitHub, see the guide ["Working with Git &
GitHub"](https://cloudberrydb.org/contribute/git).

## Community & Support

We have many channels for community members to discuss, ask for help,
feedback, and chat:

| Type | Description |
|------|-------------|
| Slack | [Click to Join](https://communityinviter.com/apps/cloudberrydb/welcome) the real-time chat on Slack for QA, Dev, Events, and more. Don't miss out! Check out the [Slack guide](https://cloudberrydb.org/community/slack) to learn more. |
| Q&A | Ask for help when running/developing Cloudberry Database, visit [GitHub Discussions - QA](https://github.com/orgs/cloudberrydb/discussions/categories/q-a). |
| New ideas / Feature Requests | Share ideas for new features, visit [GitHub Discussions - Ideas](https://github.com/orgs/cloudberrydb/discussions/categories/ideas-feature-requests).  |
| Report bugs | Problems and issues in Cloudberry Database core. If you find bugs, welcome to submit them [here](https://github.com/cloudberrydb/cloudberrydb/issues).  |
| Report a security vulnerability | View our [security policy](https://github.com/cloudberrydb/cloudberrydb/security/policy) to learn how to report and contact us.  |
| Community events | Including meetups, webinars, conferences, and more events, visit the [Events page](https://cloudberrydb.org/community/events) and subscribe events calendar.  |
| Documentation | [Official documentation](https://cloudberrydb.org/docs/) for Cloudberry Database. You can explore it to discover more details about us. |

When you are involved, please follow our community [Code of
Conduct](https://cloudberrydb.org/community/coc) to help create a safe
space for everyone.

## Acknowledgment

Thanks to [PostgreSQL](https://www.postgresql.org/), [Greenplum
Database](https://greenplum.org/) and other great open source projects
to make Cloudberry Database has a sound foundation.

## License

Cloudberry Database is released under the [Apache License, Version
2.0](https://github.com/cloudberrydb/cloudberrydb/blob/main/LICENSE).
