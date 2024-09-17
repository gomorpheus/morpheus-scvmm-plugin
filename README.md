# Morpheus SCVMM Plugin
This library provides an integration between Microsoft SCVMM and Morpheus. A `CloudProvider` (for syncing the Cloud related objects), a `ProvisionProvider` (for provisioning into SCVMM), and a `BackupProvider` (for VM snapshots and backups) are implemented in this plugin.

### Requirements
Microsoft SCVMM - Version 2016 or greater

### Building
`./gradlew shadowJar`

### Configuration
The following options are required when setting up a Morpheus Cloud to a SCVMM environment using this plugin:
1. SCVMM host (i.e. 10.100.10.100)
2. WINRM Port (defaults to 5985)
3. Working Path - A directory on the host for Morpheus to use a working directory
4. VM Path - The SCVMM location of virtual machines. This should match the setting in SCVMM.
5. Disk Path - The SCVMM location of Virtual Hard Disks. This should match the setting in SCVMM.
3. Username
4. Password

For additional details on setting up the cloud refer to the [full documentation](https://docs.morpheusdata.com/en/latest/integration_guides/Clouds/scvmm/scvmm.html#add-a-scvmm-cloud).

#### Features
Cloud sync: hosts, networks, and virtual machines are fetched from SCVMM and inventoried in Morpheus. Any additions, updates, and removals to these objects are reflected in Morpheus.

Provisioning: Virtual machines can be provisioned from Morpheus.

Backup: VM snapshots can be created and restored from Morpheus.
