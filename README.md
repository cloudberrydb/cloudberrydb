![Cloudberry Database](logo_cloudberry_database.png)

[![Slack](https://img.shields.io/badge/Join_Slack-6a32c9)](https://communityinviter.com/apps/cloudberrydb/welcome)
[![Twitter Follow](https://img.shields.io/twitter/follow/cloudberrydb)](https://twitter.com/cloudberrydb)
[![Website](https://img.shields.io/badge/Visit%20Website-eebc46)](https://cloudberrydb.org)
[![GitHub Discussions](https://img.shields.io/github/discussions/cloudberrydb/cloudberrydb)](https://github.com/orgs/cloudberrydb/discussions)
![GitHub commit activity(branch)](https://img.shields.io/github/commit-activity/m/cloudberrydb/cloudberrydb)
![GitHub contributors](https://img.shields.io/github/contributors/cloudberrydb/cloudberrydb)
![GitHub License](https://img.shields.io/github/license/cloudberrydb/cloudberrydb)
![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fcloudberrydb%2Fcloudberrydb.svg?type=shield)

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

* __gpMgmt/__ : Contains Cloudberry-specific command-line tools for
    managing the cluster. Scripts like gpinit, gpstart, and gpstop
    live here. They are mostly written in Python.

* __gpAux/__ : Contains Cloudberry-specific release management
    scripts, and vendored dependencies. Some additional directories
    are submodules and will be made available over time.

* __gpcontrib/__ : Much like the PostgreSQL contrib/ directory, this
    directory contains extensions such as gpfdist, PXF and gpmapreduce
    which are Cloudberry-specific.

* __doc/__ : In PostgreSQL, the user manual lives here. In Cloudberry
    Database, the user manual is maintained separately at [Cloudberry
    Database Website
    Repo](https://github.com/cloudberrydb/cloudberrydb-site/tree/main).

* __hd-ci/__ : Contains configuration files for the CBDB continuous
    integration system.

* __src/__

  * __src/backend/cdb/__ : Contains larger Cloudberry-specific backend
    modules. For example, communication between segments, turning
    plans into parallelizable plans, mirroring, distributed
    transaction and snapshot management, etc. __cdb__ stands for
    __Cluster Database__ - it was a workname used in the early
    days. That name is no longer used, but the __cdb__ prefix remains.
  
  * __src/backend/gpopt/__ : Contains the so-called __translator__
    library, for using the GPORCA optimizer with Cloudberry. The
    translator library is written in C++ code, and contains glue code
    for translating plans and queries between the DXL format used by
    GPORCA, and the PostgreSQL internal representation.

  * __src/backend/gporca/__ : Contains the GPORCA optimizer code and
    tests. This is written in C++. See
    [README.md](src/backend/gporca/README.md) for more information and
    how to unit-test GPORCA.

  * __src/backend/fts/__ : FTS is a process that runs in the
    coordinator node, and periodically polls the segments to maintain
    the status of each segment.

## Documentation

For Cloudberry Database documentation, please check the [documentation
website](https://cloudberrydb.org/docs/cbdb-overview). Our documents
are still in construction, welcome to help. If you're interested in
[document
contribution](https://cloudberrydb.org/community/docs-contributing-guide),
you can submit the pull request
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
feature proposals, documents, marketing and so on. No contribution is
too small, we encourage all types of contributions. We hope you can
enjoy it here.

Assume you have all the skills in collaboration, if not, please learn
more about [Git and GitHub](https://docs.github.com). For coding
guidelines, we try to follow [PostgreSQL Coding
Conventions](postgresql.org/docs/devel/source.html).

If the change you're working on touches functionality that is common
between PostgreSQL and Cloudberry Database, you may be asked to
forward-port it to PostgreSQL. This is not only so that we keep
reducing the delta between the two projects, but also so that any
change that is relevant to PostgreSQL can benefit from a much broader
review of the upstream PostgreSQL community. In general, keep both
code bases handy so you can be sure whether your changes need to be
forward-ported.

Before you commit your changes, please run the command to configure
the [commit message
template](https://github.com/cloudberrydb/cloudberrydb/blob/main/.gitmessage)
for your own git: `git config --global commit.template .gitmessage`

## Community

We have many channels for community members to discuss, ask for help,
feedback ,and chat:

- [GitHub
  Discussions](https://github.com/orgs/cloudberrydb/discussions): we
  use GitHub Discussions to broadcast news, answer questions, share
  ideas. You can start a new discussion under different categories,
  such as "Announcements", "Ideas / Feature Requests", "Proposal" and
  "Q&A".

- [GitHub
  Issues](https://github.com/cloudberrydb/cloudberrydb/issues): You
  can report bugs and issues with code in Cloudberry Database core.

- [Slack](https://communityinviter.com/apps/cloudberrydb/welcome):
  Slack is used for real-time chat, including QA, Dev, Events and
  more.

When you involve, please follow our community [Code of
Conduct](https://cloudberrydb.org/community/coc) to help create a safe
space for everyone.

## Acknowledgment

Thanks to [PostgreSQL](https://www.postgresql.org/), [Greenplum
Database](https://greenplum.org/) and other great open source projects
to make Cloudberry Database has a sound foundation.

## License

Cloudberry Database is released under the [Apache License, Version
2.0](https://github.com/cloudberrydb/cloudberrydb/blob/main/LICENSE).