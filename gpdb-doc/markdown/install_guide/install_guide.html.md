---
title: Installing and Upgrading Cloudberry 
---

Information about installing, configuring, and upgrading Cloudberry Database software and configuring Cloudberry Database host machines.

-   **[Platform Requirements](platform-requirements.html)**  
This topic describes the Cloudberry Database 6 platform and operating system software requirements.
-   **[Introduction to Cloudberry](preinstall_concepts.html)**  
High-level overview of the Cloudberry Database system architecture.
-   **[Estimating Storage Capacity](capacity_planning.html)**  
To estimate how much data your Cloudberry Database system can accommodate, use these measurements as guidelines. Also keep in mind that you may want to have extra space for landing backup files and data load files on each segment host.
-   **[Configuring Your Systems](prep_os.html)**  
Describes how to prepare your operating system environment for Cloudberry Database software installation.
-   **[Installing the Cloudberry Database Software](install_gpdb.html)**  
Describes how to install the Cloudberry Database software binaries on all of the hosts that will comprise your Cloudberry Database system, how to enable passwordless SSH for the `gpadmin` user, and how to verify the installation.
-   **[Creating the Data Storage Areas](create_data_dirs.html)**  
Describes how to create the directory locations where Cloudberry Database data is stored for each coordinator, standby, and segment instance.
-   **[Validating Your Systems](validate.html)**  
Validate your hardware and network performance.
-   **[Initializing a Cloudberry Database System](init_gpdb.html)**  
Describes how to initialize a Cloudberry Database database system.
-   **[Installing Optional Extensions \(Tanzu Cloudberry\)](data_sci_pkgs.html)**  
Information about installing optional Tanzu Cloudberry Database extensions and packages, such as the Procedural Language extensions and the Python and R Data Science Packages.
-   **[Installing Additional Supplied Modules](install_modules.html)**  
The Cloudberry Database distribution includes several PostgreSQL- and Cloudberry-sourced `contrib` modules that you have the option to install.
-   **[Configuring Timezone and Localization Settings](localization.html)**  
Describes the available timezone and localization features of Cloudberry Database.
-   **[Upgrading to Cloudberry 6](upgrade_intro.html)**  
This topic identifies the upgrade paths for upgrading a Cloudberry Database 6.x release to a newer 6.x release. The topic also describes the migration paths for migrating Tanzu Cloudberry Database 4.x or 5.x data to Cloudberry Database 6.x.
-   **[Enabling iptables \(Optional\)](enable_iptables.html)**  
On Linux systems, you can configure and enable the `iptables` firewall to work with Cloudberry Database.
-   **[Installation Management Utilities](apx_mgmt_utils.html)**  
References for the command-line management utilities used to install and initialize a Cloudberry Database system.
-   **[Cloudberry Environment Variables](env_var_ref.html)**  
Reference of the environment variables to set for Cloudberry Database.
-   **[Example Ansible Playbook](ansible-example.html)**  
A sample Ansible playbook to install a Cloudberry Database software release onto the hosts that will comprise a Cloudberry Database system.

