---
title: Procedural Language, Machine Learning, and Geospatial Extensions 
---

*Optional.* Use the Cloudberry package manager \(`gppkg`\) to install Cloudberry Database extensions such as PL/Java, PL/R, PostGIS, and MADlib, along with their dependencies, across an entire cluster. The package manager also integrates with existing scripts so that any packages are automatically installed on any new hosts introduced into the system following cluster expansion or segment host recovery.

See [gppkg](../utility_guide/ref/gppkg.html) for more information, including usage.

Extension packages can be downloaded from the Cloudberry Database page on [VMware Tanzu Network](https://network.pivotal.io/products/pivotal-gpdb). The extension documentation in the *[Cloudberry Database Reference Guide](../ref_guide/ref_guide.html)* contains information about installing extension packages and using extensions.

-   [Cloudberry PL/R Language Extension](../analytics/pl_r.html)
-   [Cloudberry PL/Java Language Extension](../analytics/pl_java.html)
-   [Cloudberry MADlib Extension for Analytics](../analytics/madlib.html)
-   [Cloudberry PostGIS Extension](../analytics/postGIS.html)

**Important:** If you intend to use an extension package with Cloudberry Database 6 you must install and use a Cloudberry Database extension package \(gppkg files and contrib modules\) that is built for Cloudberry Database 6. Any custom modules that were used with earlier versions must be rebuilt for use with Cloudberry Database 6.

**Parent topic:**[Installing Optional Extensions \(Tanzu Cloudberry\)](data_sci_pkgs.html)

