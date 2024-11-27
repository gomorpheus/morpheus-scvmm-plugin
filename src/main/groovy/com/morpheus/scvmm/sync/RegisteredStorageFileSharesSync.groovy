package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Datastore
import com.morpheusdata.model.Network
import com.morpheusdata.model.StorageVolume
import com.morpheusdata.model.projection.DatastoreIdentityProjection
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

                Observable<DatastoreIdentityProjection> domainRecords = context.async.cloud.datastore.listIdentityProjections(new DataQuery()
                        .withFilter('category', '=~', 'scvmm.registered.file.share.datastore.%')
                        .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id)
                        .withFilter('type', 'generic'))

                SyncTask<DatastoreIdentityProjection, Map, Datastore> syncTask = new SyncTask<>(domainRecords, objList as Collection<Map>)

                syncTask.addMatchFunction { DatastoreIdentityProjection morpheusItem, Map cloudItem ->
                    morpheusItem?.externalId == cloudItem?.ID
                }.onDelete { removeItems ->
                    log.debug("removing datastore: ${removeItems?.size()}")
                    context.async.cloud.datastore.bulkRemove(removeItems).blockingGet()
                }.onUpdate { List<SyncTask.UpdateItem<Datastore, Map>> updateItems ->
                    updateMatchedFileShares(updateItems)
                }.onAdd { itemsToAdd ->
                    addMissingFileShares(itemsToAdd)
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<DatastoreIdentityProjection, Map>> updateItems ->
                    return context.async.cloud.datastore.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            } else {
                log.error("Error not getting the RegisteredStorageFileShares")
            }
        } catch (e) {
            log.error("RegisteredStorageFileSharesSync error: ${e}", e.getMessage())
        }
    }

    private addMissingFileShares(Collection<Map> addList) {
        log.debug("RegisteredStorageFileSharesSync: addMissingFileShares: called")
        def dataStoreAdds = []
        try {
            def hostToShareMap = [:]
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

                // TODO: Below code need to be implemented once test data is available for RegisteredStorageFileShares
                // buildMap
                // def externalId = ds.ID
                // Build up a mapping of host (externalId) to registered file shares
                /*def getOrCreateHostEntry = { id ->
                    if (!hostToShareMap[id]) {
                        hostToShareMap[id] = [] as Set
                    }
                    return hostToShareMap[id]
                }
                cloudItem.ClusterAssociations?.each { ca ->
                    def hostEntry = getOrCreateHostEntry(ca.HostID)
                    hostEntry << externalId
                }
                cloudItem.HostAssociations?.each { ha ->
                    def hostEntry = getOrCreateHostEntry(ha.HostID)
                    hostEntry << externalId
                }*/
            }
            //create dataStore
            if (dataStoreAdds.size() > 0) {
                context.async.cloud.datastore.bulkCreate(dataStoreAdds).blockingGet()
            }
            //syncVolumeForEachHosts(hostToShareMap)
        } catch (e) {
            log.error "Error in adding RegisteredStorageFileSharesSync ${e}", e
        }
    }

    private updateMatchedFileShares(List<SyncTask.UpdateItem<Datastore, Map>> updateList) {
        log.debug("RegisteredStorageFileSharesSync >> updateMatchedFileShares >> Entered")
        List<Network> itemsToUpdate = []
        try {
            for (update in updateList) {
                Datastore existingItem = update.existingItem
                Map masterItem = update.masterItem
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
            }
            if (itemsToUpdate.size() > 0) {
                context.async.cloud.datastore.bulkSave(itemsToUpdate).blockingGet()
            }
        } catch (e) {
            log.error "Error in update updateMatchedFileShares ${e}", e
        }
    }

    // TODO: This code need to be implemented once test data is available for RegisteredStorageFileShares
    /*private syncVolumeForEachHosts (hostToShareMap){
        try {
            def existingHostsList = context.services.computeServer.list(new DataQuery().withFilter('zone.id', cloud.id)
                    .withFilter('computeServerType.code', 'scvmmHypervisor'))
            def existingHostIds = []
            existingHostsList?.each {
                existingHostIds << it.id
            }
            existingHostIds?.each{ it ->
                def hostId = it
                def volumeType = context.services.storageVolume.storageVolumeType.find(new DataQuery()
                        .withFilter('code', 'scvmm-registered-file-share-datastore'))
                ComputeServer host = context.services.computeServer.get(hostId)
                def existingStorageVolumes = host.volumes?.findAll { it.type == volumeType }
                def masterVolumeIds = hostToShareMap[host.externalId]
                log.debug "${hostId}:${host.externalId} has ${existingStorageVolumes?.size()} volumes already"

                def matchVolFunction = { StorageVolume storageVolume, masterVolumeId ->
                    storageVolume?.externalId == masterVolumeId
                }
                def syncVolLists = ComputeUtility.buildSyncLists(existingStorageVolumes, masterVolumeIds, matchVolFunction)

                syncVolLists.addList?.each { dsExternalId ->
                    Datastore match = morphDatastores.find { it.externalId == dsExternalId}
                    if(match) {
                        log.debug "${hostId}: Adding new volume ${dsExternalId}"
                        def newVolume = new StorageVolume(type:volumeType, maxStorage:match.storageSize, usedStorage:match.storageSize - match.freeSpace, externalId:dsExternalId,
                                internalId:dsExternalId, name:match.name, volumePath:findMountPath(dsExternalId), zoneId:opts.zone?.id)
                        newVolume.datastore = match
                        host.addToVolumes(newVolume)
                        newVolume.save(flush:true,failOnError:true)
                        host.save(flush:true)
                    } else {
                        log.error "Matching datastore with id ${dsExternalId} not found!"
                    }
                }
                syncVolLists.updateList?.each { updateMap ->
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
                        storageVolume.save(flush:true)
                    }
                }
                syncVolLists.removeList?.each { StorageVolume removeVol ->
                    log.debug("${hostId}: removing storageVolume: ${removeVol.id}")
                    Datastore.where { storageVolume == removeVol }.deleteAll()
                    host.removeFromVolumes(removeVol)
                    removeVol.delete(flush:true)
                }
            }
        } catch (e) {
            log.error "error in cacheRegisteredStorageFileShares thread: ${e}", e
        }
    }*/
}
