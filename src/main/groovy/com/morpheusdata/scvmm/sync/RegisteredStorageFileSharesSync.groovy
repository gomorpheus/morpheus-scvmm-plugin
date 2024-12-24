package com.morpheusdata.scvmm.sync

import com.morpheusdata.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Datastore
import com.morpheusdata.model.StorageVolume
import com.morpheusdata.model.projection.DatastoreIdentity
import com.morpheusdata.model.projection.StorageVolumeIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

/**
 * @author rahul.ray
 */

@Slf4j
class RegisteredStorageFileSharesSync {

    private Cloud cloud
    private ComputeServer node
    private MorpheusContext context
    private ScvmmApiService apiService

    RegisteredStorageFileSharesSync(Cloud cloud, ComputeServer node, MorpheusContext context) {
        this.cloud = cloud
        this.node = node
        this.context = context
        this.apiService = new ScvmmApiService(context)
    }

    def execute() {
        log.debug "RegisteredStorageFileSharesSync"
        try {
            def clusters = context.services.cloud.pool.list(new DataQuery()
                    .withFilter('refType', 'ComputeZone')
                    .withFilter('refId', cloud.id)
                    .withFilter('type', 'Cluster')
                    .withFilter('internalId', '!=', null))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
            def listResults = apiService.listRegisteredFileShares(scvmmOpts)
            log.debug("listResults: ${listResults}")
            if (listResults.success == true && listResults.datastores) {
                def objList = listResults?.datastores
                def serverType = context.async.cloud.findComputeServerTypeByCode('scvmmHypervisor').blockingGet()

                def domainRecords = context.async.cloud.datastore.listIdentityProjections(new DataQuery()
                        .withFilter('category', '=~', 'scvmm.registered.file.share.datastore.%')
                        .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id)
                        .withFilter('type', 'generic'))

                SyncTask<DatastoreIdentity, Map, Datastore> syncTask = new SyncTask<>(domainRecords, objList as Collection<Map>)

                syncTask.addMatchFunction { DatastoreIdentity morpheusItem, Map cloudItem ->
                    morpheusItem?.externalId == cloudItem?.ID
                }.onDelete { removeItems ->
                    log.debug("removing datastore: ${removeItems?.size()}")
                    context.async.cloud.datastore.remove(removeItems).blockingGet()
                }.onUpdate {  updateItems ->
                    updateMatchedFileShares(updateItems, objList)
                }.onAdd { itemsToAdd ->
                    addMissingFileShares(itemsToAdd, objList)
                }.withLoadObjectDetailsFromFinder {  updateItems ->
                    return context.async.cloud.datastore.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            } else {
                log.info("Not getting the RegisteredStorageFileShares data")
            }
        } catch (e) {
            log.error("RegisteredStorageFileSharesSync error: ${e}", e.getMessage())
        }
    }

    private addMissingFileShares(Collection<Map> addList, objList) {
        log.debug("RegisteredStorageFileSharesSync: addMissingFileShares: called")
        def dataStoreAdds = []
        try {
            def hostToShareMap = [:]

            def getOrCreateHostEntry = { hostId ->
                if(!hostToShareMap[hostId]) {
                    hostToShareMap[hostId] = [] as Set
                }
                return hostToShareMap[hostId]
            }
            addList?.each { cloudItem ->
                def externalId = cloudItem.ID
                def name = cloudItem.Name
                def totalSize = cloudItem.Capacity ? cloudItem.Capacity?.toLong() : 0
                def freeSpace = cloudItem.FreeSpace?.toLong() ?: 0
                def usedSpace = totalSize - freeSpace
                def online = cloudItem.IsAvailableForPlacement
                // Create the datastore
                def datastoreConfig = [
                        owner      : cloud.owner,
                        name       : cloudItem.Name,
                        externalId : cloudItem.ID,
                        refType    : 'ComputeZone',
                        refId      : cloud.id,
                        freeSpace  : freeSpace,
                        storageSize: totalSize,
                        cloud      : cloud,
                        category   : "scvmm.registered.file.share.datastore.${cloud.id}",
                        drsEnabled : false,
                        online     : online,
                        type       : 'generic'
                ]
                log.info "Created registered file share for id: ${cloudItem.ID}"
                Datastore datastore = new Datastore(datastoreConfig)
                dataStoreAdds << datastore
                // buildMap
                //def externalId = ds.ID
                // Build up a mapping of host (externalId) to registered file shares
                cloudItem.ClusterAssociations?.each { ca ->
                    def hostEntry = getOrCreateHostEntry(ca.HostID)
                    hostEntry << externalId
                }
                cloudItem.HostAssociations?.each { ha ->
                    def hostEntry = getOrCreateHostEntry(ha.HostID)
                    hostEntry << externalId
                }
            }
            //create dataStore
            if (dataStoreAdds.size() > 0) {
                context.async.cloud.datastore.bulkCreate(dataStoreAdds).blockingGet()
            }
            syncVolumeForEachHosts(hostToShareMap, objList)
        } catch (e) {
            log.error "Error in adding RegisteredStorageFileSharesSync ${e}", e
        }
    }

    private updateMatchedFileShares(List<SyncTask.UpdateItem<Datastore, Map>> updateList, objList) {
        log.debug("RegisteredStorageFileSharesSync >> updateMatchedFileShares >> Entered")
        def itemsToUpdate = []
        try {
            def hostToShareMap = [:]

            def getOrCreateHostEntry = { hostId ->
                if(!hostToShareMap[hostId]) {
                    hostToShareMap[hostId] = [] as Set
                }
                return hostToShareMap[hostId]
            }
            for (update in updateList) {
                Datastore existingItem = update.existingItem
                Map masterItem = update.masterItem
                def externalId = masterItem.ID
                def save = false
                if (existingItem.online != masterItem.IsAvailableForPlacement) {
                    existingItem.online = masterItem.IsAvailableForPlacement
                    save = true
                }
                if (existingItem.name != masterItem.Name) {
                    existingItem.name = masterItem.Name
                    save = true
                }
                def freeSpace = masterItem.FreeSpace?.toLong() ?: 0
                if (existingItem.freeSpace != freeSpace) {
                    existingItem.freeSpace = freeSpace
                    save = true
                }
                def totalSize = masterItem.Capacity ? masterItem.Capacity?.toLong() : 0
                if (existingItem.storageSize != totalSize) {
                    existingItem.storageSize = totalSize
                    save = true
                }
                if (save) {
                    itemsToUpdate << existingItem
                }

                masterItem.ClusterAssociations?.each { ca ->
                    def hostEntry = getOrCreateHostEntry(ca.HostID)
                    hostEntry << externalId
                }
                masterItem.HostAssociations?.each { ha ->
                    def hostEntry = getOrCreateHostEntry(ha.HostID)
                    hostEntry << externalId
                }
            }
            if (itemsToUpdate.size() > 0) {
                context.async.cloud.datastore.bulkSave(itemsToUpdate).blockingGet()
            }
            syncVolumeForEachHosts(hostToShareMap, objList)
        } catch (e) {
            log.error "Error in update updateMatchedFileShares ${e}", e
        }
    }

	/// hostToShareMap is a map of host externalId to a list of registered file share IDs
	/// objList is the list of registered file shares
    private syncVolumeForEachHosts (hostToShareMap, objList){
        try {
            def existingHostsList = context.services.computeServer.list(new DataQuery().withFilter('zone.id', cloud.id)
                    .withFilter('computeServerType.code', 'scvmmHypervisor'))
            def existingHostIds = []
            existingHostsList?.each {
                existingHostIds << it.id
            }
            def findMountPath = { dsID ->
                def obj = objList.find { it.ID == dsID}
                obj.MountPoints?.size() > 0 ? obj.MountPoints[0] : null
            }
            def morphDatastores = context.services.cloud.datastore.list(new DataQuery()
                    .withFilter('category', '=~', 'scvmm.registered.file.share.datastore.%')
                    .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id)
                    .withFilter('type', 'generic'))

            existingHostIds?.each{ it ->
                def hostId = it
                def volumeType = context.services.storageVolume.storageVolumeType.find(new DataQuery()
                        .withFilter('code', 'scvmm-registered-file-share-datastore'))
                ComputeServer host = context.services.computeServer.get(hostId)
                def existingStorageVolumes = host.volumes?.findAll { it.type == volumeType }
                def masterVolumeIds = hostToShareMap[host.externalId]
                log.debug "${hostId}:${host.externalId} has ${existingStorageVolumes?.size()} volumes already"
                def domainRecords = Observable.fromIterable(existingStorageVolumes)

                SyncTask<StorageVolumeIdentityProjection, Map, StorageVolume> syncTask = new SyncTask<>(domainRecords, masterVolumeIds as Collection<Map>)

                syncTask.addMatchFunction { StorageVolumeIdentityProjection storageVolume, Map cloudItem ->
                    storageVolume?.externalId == cloudItem?.ID
                }.onDelete { removeItems ->
                    log.debug("${hostId}: removing storageVolume: ${removeItems.size()}")
                    removeItems?.each { currentVolume ->
                        log.debug "removing volume: ${currentVolume}"
                        currentVolume.controller = null
                        currentVolume.datastore = null

                        context.async.storageVolume.save(currentVolume).blockingGet()
                        context.async.storageVolume.remove([currentVolume], server, true).blockingGet()
                        context.async.storageVolume.remove(currentVolume).blockingGet()
                    }
                }.onUpdate { List<SyncTask.UpdateItem<Datastore, Map>> updateItems ->
                    updateItems?.each { updateMap ->
                        StorageVolume storageVolume = updateMap.existingItem
                        def dsExternalId = updateMap.masterItem
                        log.debug "${hostId}: Updating existing volumes ${dsExternalId}"

                        Datastore match = morphDatastores.find { it.externalId == dsExternalId}

                        def save = false
                        if(storageVolume.maxStorage != match.storageSize) {
                            storageVolume.maxStorage = match.storageSize
                            save = true
                        }
                        def usedSpace = match.storageSize - match.freeSpace
                        if(storageVolume.usedStorage != usedSpace) {
                            storageVolume.usedStorage = usedSpace
                            save = true
                        }
                        if(storageVolume.name != match.name) {
                            storageVolume.name = match.name
                            save = true
                        }
                        def mountPoint = findMountPath(dsExternalId)
                        if(storageVolume.volumePath != mountPoint){
                            storageVolume.volumePath = mountPoint
                            save = true
                        }
                        if(storageVolume.datastore?.id != match.id) {
                            storageVolume.datastore = match
                            save = true
                        }

                        if(save) {
                            context.async.storageVolume.save(storageVolume).blockingGet()
                        }
                    }
                }.onAdd { itemsToAdd ->
                    itemsToAdd?.each { dsExternalId ->
                        Datastore match = morphDatastores.find { it.externalId == dsExternalId}
                        if(match) {
                            log.debug "${hostId}: Adding new volume ${dsExternalId}"
                            def newVolume = new StorageVolume(
                                    type:volumeType,
                                    maxStorage:match.storageSize,
                                    usedStorage:match.storageSize - match.freeSpace,
                                    externalId:dsExternalId,
                                    internalId:dsExternalId,
                                    name:match.name,
                                    volumePath:findMountPath(dsExternalId),
                                    cloudId:cloud?.id
                            )
                            newVolume.datastore = match
                            context.async.storageVolume.create([newVolume], host).blockingGet()
                            context.async.computeServer.save(host).blockingGet()
                        } else {
                            log.error "Matching datastore with id ${dsExternalId} not found!"
                        }
                    }
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<StorageVolumeIdentityProjection, Map>> updateItems ->
                    return context.async.storageVolume.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            }
        } catch (e) {
            log.error "error in cacheRegisteredStorageFileShares: ${e}", e
        }
    }
}