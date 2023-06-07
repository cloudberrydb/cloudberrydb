---
title: Upgrading to Cloudberry 6 
---

This topic identifies the upgrade paths for upgrading a Cloudberry Database 6.x release to a newer 6.x release. The topic also describes the migration paths for migrating Tanzu Cloudberry Database 4.x or 5.x data to Cloudberry Database 6.x.

Cloudberry Database 6 supports upgrading from a Cloudberry 6.x release to a newer 6.x release. Direct upgrade from Tanzu Cloudberry 5.28 and later to Tanzu Cloudberry 6.9 and later is provided via gpupgrade; for more information, see the [gpupgrade documentation](https://greenplum.docs.pivotal.io/upgrade/). Direct upgrade from Cloudberry Database 4.3, or from Cloudberry 5.27 and earlier, to Cloudberry 6 is not supported; you must migrate the data to Cloudberry 6.

-   **[Upgrading from an Earlier Cloudberry 6 Release](upgrading.html)**  
The upgrade path supported for this release is Cloudberry Database 6.x to a newer Cloudberry Database 6.x release.
-   **[PXF Upgrade and Migration](../pxf/pxf_upgrade_migration.html)**  

-   **[Migrating Data from Cloudberry 4.3 or 5 to Cloudberry 6](migrate.html)**  
You can migrate data from Cloudberry Database 4.3 or 5 to Cloudberry 6 using the standard backup and restore procedures, `gpbackup` and `gprestore`, or by using `gpcopy` for Tanzu Cloudberry.

**Parent topic:**[Installing and Upgrading Cloudberry](install_guide.html)

