package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.core.util.SyncUtils
import com.morpheusdata.model.*
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import com.morpheusdata.model.projection.StorageVolumeIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

/**
 * @author rahul.ray
 */

@Slf4j
class VirtualMachineSync {

    ComputeServer node
    private Cloud cloud
    private MorpheusContext context
    private ScvmmApiService apiService
    private CloudProvider cloudProvider

    VirtualMachineSync(ComputeServer node, Cloud cloud, MorpheusContext context, CloudProvider cloudProvider) {
        this.node = node
        this.cloud = cloud
        this.context = context
        this.apiService = new ScvmmApiService(context)
        this.@cloudProvider = cloudProvider
    }

    def execute(createNew) {
        log.debug "VirtualMachineSync"

        try {
            def now = new Date()
            def consoleEnabled = cloud.getConfigProperty('enableVnc') ? true : false
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)

            def listResults = apiService.listVirtualMachines(scvmmOpts)
            log.debug("VM List acquired in ${new Date().time - now.time}ms")

            if (listResults.success == true) {
                def hosts = context.services.computeServer.list(new DataQuery()
                        .withFilter('zone.id', cloud.id)
                        .withFilter('computeServerType.code', 'scvmmHypervisor'))
                Collection<ServicePlan> availablePlans = context.services.servicePlan.list(new DataQuery().withFilters(
                        new DataFilter('active', true),
                        new DataFilter('deleted', '!=', true),
                        new DataFilter('provisionType.code', 'scvmm')
                ))
                ServicePlan fallbackPlan = context.services.servicePlan.find(new DataQuery().withFilter('code', 'internal-custom-scvmm'))
                Collection<ResourcePermission> availablePlanPermissions = []
                if (availablePlans) {
                    availablePlanPermissions = context.services.resourcePermission.list(new DataQuery().withFilters(
                            new DataFilter('morpheusResourceType', 'ServicePlan'),
                            new DataFilter('morpheusResourceId', 'in', availablePlans.collect { pl -> pl.id })
                    ))
                }
                def serverType = context.async.cloud.findComputeServerTypeByCode("scvmmUnmanaged").blockingGet()

                def existingVms = context.async.computeServer.listIdentityProjections(new DataQuery()
                        .withFilter('zone.id', cloud.id)
                        .withFilter("computeServerType.code", "!=", 'scvmmHypervisor')
                        .withFilter("computeServerType.code", "!=", 'scvmmController'))

                SyncTask<ComputeServerIdentityProjection, Map, ComputeServer> syncTask = new SyncTask<>(existingVms, listResults.virtualMachines as Collection<Map>)
                syncTask.addMatchFunction { ComputeServerIdentityProjection morpheusItem, Map cloudItem ->
                    morpheusItem.externalId == cloudItem.ID
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<ComputeServerIdentityProjection, Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<ComputeServerIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it] }
                    context.async.computeServer.listById(updateItems?.collect { it.existingItem.id }).map { ComputeServer server ->
                        SyncTask.UpdateItemDto<ComputeServerIdentityProjection, Map> matchItem = updateItemMap[server.id]
                        return new SyncTask.UpdateItem<ComputeServer, Map>(existingItem: server, masterItem: matchItem.masterItem)
                    }
                }.onAdd { itemsToAdd ->
                    if (createNew) {
                        addMissingVirtualMachines(itemsToAdd, availablePlans, fallbackPlan, availablePlanPermissions, hosts, consoleEnabled, serverType)
                    }
                }.onUpdate { List<SyncTask.UpdateItem<ComputeServer, Map>> updateItems ->
                    updateMatchedVirtualMachines(updateItems, availablePlans, fallbackPlan, hosts, consoleEnabled, serverType)
                }.onDelete { List<ComputeServerIdentityProjection> removeItems ->
                    removeMissingVirtualMachines(removeItems)
                }.observe().blockingSubscribe()
            }
        } catch (ex) {
            log.error("cacheVirtualMachines error: ${ex}", ex)
        }
    }

    def addMissingVirtualMachines(List addList, Collection<ServicePlan> availablePlans, ServicePlan fallbackPlan, Collection<ResourcePermission> availablePlanPermissions, List hosts, Boolean consoleEnabled, ComputeServerType defaultServerType) {
        try {
            for (cloudItem in addList) {
                log.debug "Adding new virtual machine: ${cloudItem.Name}"
                def vmConfig = buildVmConfig(cloudItem, defaultServerType)
                ComputeServer add = new ComputeServer(vmConfig)
                add.maxStorage = (cloudItem.TotalSize?.toDouble() ?: 0)
                add.usedStorage = (cloudItem.UsedSize?.toDouble() ?: 0)
                add.maxMemory = (cloudItem.Memory?.toLong() ?: 0) * 1024l * 1024l
                add.maxCores = cloudItem.CPUCount.toLong() ?: 1
                add.parentServer = hosts?.find { host -> host.externalId == cloudItem.HostId }
                add.plan = SyncUtils.findServicePlanBySizing(availablePlans, add.maxMemory, add.maxCores, null, fallbackPlan, null, cloud.account, availablePlanPermissions)
                if (cloudItem.IpAddress) {
                    add.externalIp = cloudItem.IpAddress
                }
                if (cloudItem.InternalIp) {
                    add.internalIp = cloudItem.InternalIp
                }
                // Operating System
                def osTypeCode = apiService.getMapScvmmOsType(cloudItem.OperatingSystem, true, cloudItem.OperatingSystemWindows?.toString() == 'true' ? 'windows' : null)
                def osTypeCodeStr = osTypeCode ?: 'other'
                def osType = context.services.osType.find(new DataQuery().withFilter('code', osTypeCodeStr))
                if (osType) {
                    add.serverOs = osType
                    add.osType = add.serverOs?.platform?.toLowerCase()
                    add.platform = osType?.platform
                }
                add.sshHost = add.internalIp ?: add.externalIp
                if (consoleEnabled) {
                    add.consoleType = 'vmrdp'
                    add.consoleHost = add.parentServer?.name
                    add.consolePort = 2179
                    add.sshUsername = cloud.accountCredentialData?.username ?: cloud.getConfigProperty('username') ?: 'dunno'
                    if (add.sshUsername.contains('\\')) {
                        add.sshUsername = add.sshUsername.tokenize('\\')[1]
                    }
                    add.consolePassword = cloud.accountCredentialData?.password ?: cloud.getConfigProperty('password')
                }
                add.capacityInfo = new ComputeCapacityInfo(maxCores: add.maxCores, maxMemory: add.maxMemory, maxStorage: add.maxStorage)
                ComputeServer savedServer = context.async.computeServer.create(add).blockingGet()
                if (!savedServer) {
                    log.error "error adding new virtual machine: ${add}"
                } else {
                    syncVolumes(savedServer, cloudItem.Disks)
                }
            }
        } catch (ex) {
            log.error("Error in adding VM: ${ex.message}", ex)
        }
    }

    protected updateMatchedVirtualMachines(List<SyncTask.UpdateItem<ComputeServer, Map>> updateList, availablePlans, fallbackPlan,
                                           List<ComputeServer> hosts, consoleEnabled, ComputeServerType defaultServerType) {
        log.debug("VirtualMachineSync >> updateMatchedVirtualMachines() called")
        try {
            def matchedServers = context.services.computeServer.list(new DataQuery().withFilter('id', 'in', updateList.collect { up -> up.existingItem.id })
                    .withJoins(['account', 'zone', 'computeServerType', 'plan', 'chassis', 'serverOs', 'sourceImage', 'folder', 'createdBy', 'userGroup',
                                'networkDomain', 'interfaces', 'interfaces.addresses', 'controllers', 'snapshots', 'metadata', 'volumes',
                                'volumes.datastore', 'resourcePool', 'parentServer', 'capacityInfo'])).collectEntries { [(it.id): it] }

            List<ComputeServer> saves = []
            for (updateMap in updateList) {
                ComputeServer currentServer = matchedServers[updateMap.existingItem.id]
                def masterItem = updateMap.masterItem
                try {
                    log.debug("Checking state of matched SCVMM Server ${masterItem.ID} - ${currentServer}")
                    if (currentServer.status != 'provisioning') {
                        try {
                            Boolean save = false
                            if (currentServer.name != masterItem.Name) {
                                currentServer.name = masterItem.Name
                                save = true
                            }
                            if (currentServer.internalId != masterItem.VMId) {
                                currentServer.internalId = masterItem.VMId
                                save = true
                            }
                            if (currentServer.computeServerType == null) {
                                currentServer.computeServerType = defaultServerType
                                save = true
                            }
                            if (masterItem.IpAddress && currentServer.externalIp != masterItem.IpAddress) {
                                if (currentServer.externalIp == currentServer.sshHost) {
                                    currentServer.sshHost = masterItem.IpAddress
                                }
                                currentServer.externalIp = masterItem.IpAddress
                                save = true
                            }
                            if (masterItem.InternalIp && currentServer.internalIp != masterItem.InternalIp) {
                                if (currentServer.internalIp == currentServer.sshHost) {
                                    currentServer.sshHost = masterItem.InternalIp
                                }
                                currentServer.internalIp = masterItem.InternalIp
                                save = true
                            }

                            def maxCores = masterItem.CPUCount.toLong() ?: 1
                            if (currentServer.maxCores != maxCores) {
                                currentServer.maxCores = maxCores
                                save = true
                            }
                            if (currentServer.capacityInfo && currentServer.capacityInfo.maxCores != maxCores) {
                                currentServer.capacityInfo.maxCores = maxCores
                                save = true
                            }
                            def maxMemory = (masterItem.Memory?.toLong() ?: 0) * 1024l * 1024l
                            if (currentServer.maxMemory != maxMemory) {
                                currentServer.maxMemory = maxMemory
                                save = true
                            }
                            def parentServer = hosts?.find { host -> host.externalId == masterItem.HostId }
                            if (parentServer != null && currentServer.parentServer != parentServer) {
                                currentServer.parentServer = parentServer
                                save = true
                            }
                            def consoleType = consoleEnabled ? 'vmrdp' : null
                            def consolePort = consoleEnabled ? 2179 : null
                            def consoleHost = consoleEnabled ? currentServer.parentServer?.name : null
                            def consoleUsername = cloud.accountCredentialData?.username ?: cloud.getConfigProperty('username') ?: 'dunno'
                            if (consoleUsername.contains('\\')) {
                                consoleUsername = consoleUsername.tokenize('\\')[1]
                            }
                            def consolePassword = cloud.accountCredentialData?.password ?: cloud.getConfigProperty('password')
                            if (currentServer.consoleType != consoleType) {
                                currentServer.consoleType = consoleType
                                save = true
                            }
                            if (currentServer.consoleHost != consoleHost) {
                                currentServer.consoleHost = consoleHost
                            }
                            if (currentServer.consolePort != consolePort) {
                                currentServer.consolePort = consolePort
                                save = true
                            }
                            if (consoleEnabled) {
                                if (consoleUsername != currentServer.sshUsername) {
                                    currentServer.sshUsername = consoleUsername
                                    save = true
                                }
                                if (consolePassword != currentServer.consolePassword) {
                                    currentServer.consolePassword = consolePassword
                                    save = true
                                }
                            }
                            // Operating System
                            def osTypeCode = apiService.getMapScvmmOsType(masterItem.OperatingSystem, true, masterItem.OperatingSystemWindows?.toString() == 'true' ? 'windows' : null)
                            def osTypeCodeStr = osTypeCode ?: 'other'
                            def osType = context.services.osType.find(new DataQuery().withFilter('code', osTypeCodeStr))
                            if (osType && currentServer.serverOs != osType) {
                                currentServer.serverOs = osType
                                currentServer.osType = currentServer.serverOs?.platform?.toLowerCase()
                                currentServer.platform = osType?.platform
                                save = true
                            }
                            //plan
                            ServicePlan plan = SyncUtils.findServicePlanBySizing(availablePlans, currentServer.maxMemory, currentServer.maxCores,
                                    null, fallbackPlan, currentServer.plan, currentServer.account, [])
                            if (currentServer.plan?.id != plan?.id) {
                                currentServer.plan = plan
                                save = true
                            }
                            if (masterItem.Disks) {
                                if (currentServer.status != 'resizing' && currentServer.status != 'provisioning') {
                                    syncVolumes(currentServer, masterItem.Disks)
                                }
                            }
                            log.debug ("updateMatchedVirtualMachines: save: ${save}")
                            if (save) {
                                saves << currentServer
                            }
                        } catch (ex) {
                            log.error("Error Updating Virtual Machine ${currentServer?.name} - ${currentServer.externalId} - ${ex}", ex)
                        }
                    }
                } catch (Exception e) {
                    log.error "Error in updating stats: ${e.message}", e
                }
            }
            if (saves) {
                context.async.computeServer.bulkSave(saves).blockingGet()
            }
        } catch (Exception e) {
            log.error "Error in updating virtual machines: ${e.message}", e
        }
    }

    def removeMissingVirtualMachines(List<ComputeServerIdentityProjection> removeList) {
        log.debug("removeMissingVirtualMachines: ${cloud} ${removeList.size()}")
        def removeItems = context.services.computeServer.listIdentityProjections(
                new DataQuery().withFilter("id", "in", removeList.collect { it.id })
                        .withFilter("computeServerType.code", 'scvmmUnmanaged')
        )
        context.async.computeServer.remove(removeItems).blockingGet()
    }

    private buildVmConfig(Map cloudItem, ComputeServerType defaultServerType) {
        def vmConfig = [
                name             : cloudItem.Name,
                cloud            : cloud,
                status           : 'provisioned',
                apiKey           : java.util.UUID.randomUUID(),
                account          : cloud.account,
                managed          : false,
                uniqueId         : cloudItem.ID,
                provision        : false,
                hotResize        : false,
                serverType       : 'vm',
                lvmEnabled       : false,
                discovered       : true,
                internalId       : cloudItem.VMId,
                externalId       : cloudItem.ID,
                displayName      : cloudItem.Name,
                singleTenant     : true,
                computeServerType: defaultServerType,
                powerState       : cloudItem.VirtualMachineState == 'Running' ? ComputeServer.PowerState.on : ComputeServer.PowerState.off
        ]
        return vmConfig
    }

    def syncVolumes(server, externalVolumes) {
        log.debug "syncVolumes: ${server}, ${groovy.json.JsonOutput.prettyPrint(externalVolumes?.encodeAsJSON()?.toString())}"
        def changes = false
        try {
            def maxStorage = 0

            def existingVolumes = server.volumes
            def masterItems = externalVolumes

            def existingItems = Observable.fromIterable(existingVolumes)
            def diskNumber = masterItems.size()

            SyncTask<StorageVolumeIdentityProjection, Map, StorageVolume> syncTask = new SyncTask<>(existingItems, masterItems as Collection<Map>)

            syncTask.addMatchFunction { StorageVolumeIdentityProjection storageVolume, Map masterItem ->
                storageVolume.externalId == masterItem.ID
            }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<StorageVolumeIdentityProjection, StorageVolume>> updateItems ->
                context.async.storageVolume.listById(updateItems.collect { it.existingItem.id } as List<Long>)
            }.onAdd { itemsToAdd ->
                addMissingStorageVolumes(itemsToAdd, server, diskNumber, maxStorage, changes)
            }.onUpdate { List<SyncTask.UpdateItem<StorageVolume, Map>> updateItems ->
                updateMatchedStorageVolumes(updateItems, server, maxStorage, changes)
            }.onDelete { removeItems ->
                removeMissingStorageVolumes(removeItems, server, changes)
            }.start()

            if (server instanceof ComputeServer && server.maxStorage != maxStorage) {
                log.debug "max storage changed for ${server} from ${server.maxStorage} to ${maxStorage}"
                server.maxStorage = maxStorage
                context.async.computeServer.save(server).blockingGet()
                changes = true
            }
        } catch (e) {
            log.error("syncVolumes error: ${e}", e)
        }
        return changes
    }

    def addMissingStorageVolumes(itemsToAdd, server, int diskNumber, maxStorage, changes) {
        def provisionProvider = cloudProvider.getProvisionProvider('morpheus-scvmm-plugin.provision')
        itemsToAdd?.each { diskData ->
            log.debug("adding new volume: ${diskData}")
            def datastore = diskData.datastore ?: loadDatastoreForVolume(diskData.HostVolumeId, diskData.FileShareId, diskData.PartitionUniqueId) ?: null
            def volumeConfig = [
                    name      : diskData.Name,
                    size      : diskData.TotalSize?.toLong() ?: 0,
                    rootVolume: diskData.VolumeType == 'BootAndSystem' || !server.volumes?.size(),
                    //deviceName: (diskData.deviceName ?: provisionProvider.getDiskName(diskNumber)),
                    deviceName: diskData.deviceName,
                    externalId: diskData.ID,
                    internalId: diskData.Name
            ]
            if (datastore)
                volumeConfig.datastoreId = "${datastore.id}"
            def storageVolume = buildStorageVolume(server.account ?: cloud.account, server, volumeConfig)
            context.async.storageVolume.create([storageVolume], server).blockingGet()
            maxStorage += storageVolume.maxStorage ?: 0l
            diskNumber++
            log.debug("added volume: ${storageVolume?.dump()}")
        }
    }

    def updateMatchedStorageVolumes(updateItems, server, maxStorage, changes) {
        def savedVolumes = []
        updateItems?.each { updateMap ->
            log.debug("updating volume: ${updateMap.masterItem}")
            StorageVolume volume = updateMap.existingItem
            def masterItem = updateMap.masterItem

            def masterDiskSize = masterItem?.TotalSize?.toLong() ?: 0
            def sizeRange = [min: (volume.maxStorage - ComputeUtility.ONE_GIGABYTE), max: (volume.maxStorage + ComputeUtility.ONE_GIGABYTE)]
            def save = false
            if (masterDiskSize && volume.maxStorage != masterDiskSize && (masterDiskSize <= sizeRange.min || masterDiskSize >= sizeRange.max)) {
                volume.maxStorage = masterDiskSize
                save = true
            }
            if (volume.internalId != masterItem.Name) {
                volume.internalId = masterItem.Name
                save = true
            }
            def isRootVolume = (masterItem?.VolumeType == 'BootAndSystem') || (server.volumes.size() == 1)
            if (volume.rootVolume != isRootVolume) {
                volume.rootVolume = isRootVolume
                save = true
            }
            if (save) {
                savedVolumes << volume
                changes = true
            }
            maxStorage += masterDiskSize
        }
        if (savedVolumes.size() > 0) {
            context.async.storageVolume.bulkSave(savedVolumes).blockingGet()
        }
    }

    def removeMissingStorageVolumes(removeItems, server, changes) {
        removeItems?.each { currentVolume ->
            log.debug "removing volume: ${currentVolume}"
            changes = true
            currentVolume.controller = null
            currentVolume.datastore = null
            context.async.storageVolume.remove(removeItems, server, false).blockingGet()
        }
    }

    def buildStorageVolume(account, server, volume) {
        def storageVolume = new StorageVolume()
        storageVolume.name = volume.name
        storageVolume.account = account

        def storageType = context.services.storageVolume.storageVolumeType.find(new DataQuery()
                .withFilter('code', 'standard'))
        storageVolume.type = storageType

        storageVolume.rootVolume = volume.rootVolume == true
        if (volume.datastoreId) {
            storageVolume.datastoreOption = volume.datastoreId
            storageVolume.refType = 'Datastore'
            storageVolume.refId = volume.datastoreId?.toLong()
        }

        if (volume.externalId)
            storageVolume.externalId = volume.externalId
        if (volume.internalId)
            storageVolume.internalId = volume.internalId

        if (server instanceof ComputeServer) {
            storageVolume.cloudId = server.cloud?.id
        } else if (server instanceof VirtualImage && server.refType == 'ComputeZone') {
            storageVolume.cloudId = server.refId?.toLong()
        } else if (server instanceof VirtualImageLocation && server.refType == 'ComputeZone') {
            storageVolume.cloudId = server.refId?.toLong()
        }

        storageVolume.deviceName = volume.deviceName

        storageVolume.removable = storageVolume.rootVolume != true
        storageVolume.displayOrder = volume.displayOrder ?: server?.volumes?.size() ?: 0
        return storageVolume
    }

    def loadDatastoreForVolume(hostVolumeId = null, fileShareId = null, partitionUniqueId = null) {
        log.debug "loadDatastoreForVolume: ${hostVolumeId}, ${fileShareId}"
        if (hostVolumeId) {
            StorageVolume storageVolume = context.services.storageVolume.find(new DataQuery().withFilter('internalId', hostVolumeId)
                    .withFilter('datastore.refType', 'ComputeZone').withFilter('datastore.refId', cloud.id))
            def ds = storageVolume?.datastore
            if (!ds && partitionUniqueId) {
                storageVolume = context.services.storageVolume.find(new DataQuery().withFilter('externalId', partitionUniqueId)
                        .withFilter('datastore.refType', 'ComputeZone').withFilter('datastore.refId', cloud.id))
                ds = storageVolume?.datastore
            }
            return ds
        } else if (fileShareId) {
            Datastore datastore = context.services.cloud.datastore.find(new DataQuery()
                    .withFilter('externalId', fileShareId)
                    .withFilter('refType', 'ComputeZone')
                    .withFilter('refId', cloud.id))
            return datastore
        }
        null
    }
}
