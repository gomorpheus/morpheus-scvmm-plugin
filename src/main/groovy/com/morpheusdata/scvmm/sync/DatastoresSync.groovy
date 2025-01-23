package com.morpheusdata.scvmm.sync

import com.morpheusdata.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.*
import com.morpheusdata.model.projection.DatastoreIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

/**
 * @author rahul.ray
 */

@Slf4j
class DatastoresSync {

    ComputeServer node
    private Cloud cloud
    private MorpheusContext context
    private ScvmmApiService apiService

    DatastoresSync(ComputeServer node, Cloud cloud, MorpheusContext context) {
        this.node = node
        this.cloud = cloud
        this.context = context
        this.apiService = new ScvmmApiService(context)
    }

    def execute() {
        log.debug "DatastoresSync"
        try {
            List<CloudPool> clusters = context.services.cloud.pool.list(new DataQuery()
                    .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id)
                    .withFilter('type', 'Cluster').withFilter('internalId', '!=', null))
            StorageVolumeType volumeType = context.services.storageVolume.storageVolumeType.find(new DataQuery().withFilter('code', 'scvmm-datastore'))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
            def listResults = apiService.listDatastores(scvmmOpts)
            log.debug("DatastoresSync: listResults: ${listResults}")
            if (listResults.success == true && listResults.datastores) {
                def objList = []
                def partitionUniqueIds = []
                listResults.datastores?.each { data ->
                    if (!partitionUniqueIds?.contains(data.partitionUniqueID)) {
                        objList << data
                    }
                    partitionUniqueIds << data.partitionUniqueID
                }
                log.debug("DatastoresSync: objList: ${objList}")
                List<ComputeServer> existingHosts = context.services.computeServer.list(new DataQuery().withFilter('zone.id', cloud.id)
                        .withFilter('computeServerType.code', 'scvmmHypervisor'))

                Observable<DatastoreIdentityProjection> existingItems = context.async.cloud.datastore.listIdentityProjections(new DataQuery()
                        .withFilter('category', '=~', 'scvmm.datastore.%').withFilter('refType', 'ComputeZone')
                        .withFilter('refId', cloud.id).withFilter('type', 'generic'))

                SyncTask<DatastoreIdentityProjection, Map, Datastore> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)
                syncTask.addMatchFunction { existingItem, cloudItem ->
                    existingItem.externalId?.toString() == cloudItem.partitionUniqueID?.toString()
                }.onDelete { removeItems ->
                    removeMissingDatastores(removeItems)
                }.onUpdate { updateItems ->
                    updateMatchedDatastores(updateItems, clusters, existingHosts, volumeType)
                }.onAdd { itemsToAdd ->
                    addMissingDatastores(itemsToAdd, clusters, existingHosts, volumeType)
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<DatastoreIdentityProjection, Map>> updateItems ->
                    return context.async.cloud.datastore.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            }
        } catch (ex) {
            log.error("DatastoresSync error: {}", ex.getMessage())
        }
    }

    private removeMissingDatastores(List<DatastoreIdentityProjection> removeList) {
        log.debug("removeMissingDatastores: ${cloud} ${removeList.size()}")
        try {
            context.services.cloud.datastore.bulkRemove(removeList)
        } catch (ex) {
            log.error("DatastoresSync: removeMissingDatastores error: {}", ex.getMessage())
        }
    }

    private void updateMatchedDatastores(List<SyncTask.UpdateItem<Datastore, Map>> updateList, clusters, existingHosts, volumeType) {
        try {
            log.debug("updateMatchedDatastores: ${updateList?.size()}")
            def host
            updateList.each { item ->
                def masterItem = item.masterItem
                def existingItem = item.existingItem
                host = existingHosts.find { it.hostname == masterItem.vmHost }
                def cluster
                if (masterItem.isClusteredSharedVolume) {
                    cluster = clusters.find { c ->
                        c.getConfigProperty('sharedVolumes')?.toString().contains(masterItem.name) && (!host || host.resourcePool.id == c.id)
                    }
                }
                def name = getName(masterItem, cluster, host)
                def doSave = false

                if (existingItem.online != masterItem.isAvailableForPlacement) {
                    existingItem.online = masterItem.isAvailableForPlacement
                    doSave = true
                }
                if (existingItem.name != name) {
                    existingItem.name = name
                    doSave = true
                }
                def freeSpace = masterItem.freeSpace?.toLong() ?: 0
                if (existingItem.freeSpace != freeSpace) {
                    existingItem.freeSpace = freeSpace
                    doSave = true
                }
                def totalSize = masterItem.size ? masterItem.size?.toLong() : masterItem.capacity ? masterItem.capacity?.toLong() : 0
                if (existingItem.storageSize != totalSize) {
                    existingItem.storageSize = totalSize
                    doSave = true
                }
                if (existingItem.zonePool?.id != cluster?.id) {
                    existingItem.zonePool = cluster
                    doSave = true
                }
                if (doSave) {
                    def savedDataStore = context.async.cloud.datastore.save(existingItem).blockingGet()
                    if (savedDataStore && host) {
                        syncVolume(masterItem, host, savedDataStore, volumeType, getDataStoreExternalId(masterItem))
                    }
                }
            }
        } catch (e) {
            log.error("Error in updateMatchedDatastores method: ${e}", e)
        }
    }

    private addMissingDatastores(Collection<Map> addList, clusters, existingHosts, volumeType) {
        log.debug("addMissingDatastores: addList?.size(): ${addList?.size()}")
        try {
            addList?.each { Map item ->
                def isSharedVolume = item.isClusteredSharedVolume
                def externalId = getDataStoreExternalId(item)
                ComputeServer host = existingHosts.find { it.hostname == item.vmHost }
                def cluster
                if (isSharedVolume) {
                    cluster = clusters.find { c ->
                        c.getConfigProperty('sharedVolumes')?.toString().contains(item.name) && (!host || host.resourcePool.id == c.id)
                    }
                }
                def datastoreConfig =
                        [
                                cloud      : cloud,
                                drsEnabled : false,
                                zonePool   : cluster,
                                refId      : cloud.id,
                                type       : 'generic',
                                owner      : cloud.owner,
                                refType    : 'ComputeZone',
                                name       : getName(item, cluster, host),
                                externalId : externalId,
                                online     : item.isAvailableForPlacement,
                                category   : "scvmm.datastore.${cloud.id}",
                                freeSpace  : item.freeSpace?.toLong() ?: 0,
                                active     : cloud.defaultDatastoreSyncActive,
                                storageSize: item.size ? item.size?.toLong() : item.capacity ? item.capacity?.toLong() : 0
                        ]
                log.debug("datastoreConfig: ${datastoreConfig}")
                Datastore datastore = new Datastore(datastoreConfig)
                def savedDataStore = context.async.cloud.datastore.create(datastore).blockingGet()
                log.debug("savedDataStore?.id: ${savedDataStore?.id}")
                if (savedDataStore && host) {
                    syncVolume(item, host, savedDataStore, volumeType, externalId)
                }
            }
        } catch (e) {
            log.error "Error in adding Datastores sync ${e}", e
        }
    }

    private syncVolume(item, ComputeServer host, savedDataStore, volumeType, externalId) {
        try {
            // See if the volume is attached to the server yet
            def totalSize = item.size ? item.size?.toLong() : item.capacity ? item.capacity?.toLong() : 0
            def freeSpace = item.freeSpace?.toLong() ?: 0
            def usedSpace = totalSize - freeSpace
            def mountPoint = item.mountPoints?.size() > 0 ? item.mountPoints[0] : null
            StorageVolume existingVolume = host.volumes.find { it.externalId == externalId }
            if (existingVolume) {
                def save = false
                if (existingVolume.internalId != item.id) {
                    existingVolume.internalId = item.id
                    save = true
                }

                if (existingVolume.maxStorage != totalSize) {
                    existingVolume.maxStorage = totalSize
                    save = true
                }

                if (existingVolume.usedStorage != usedSpace) {
                    existingVolume.usedStorage = usedSpace
                    save = true
                }
                if (existingVolume.name != item.name) {
                    existingVolume.name = item.name
                    save = true
                }
                if (existingVolume.volumePath != mountPoint) { // check: volumePath does not exist in StorageVolume
                    existingVolume.volumePath = mountPoint
                    save = true
                }
                if (existingVolume.datastore?.id != savedDataStore.id) {
                    existingVolume.datastore = savedDataStore
                    save = true
                }
                if (save) {
                    context.async.storageVolume.save(existingVolume).blockingGet()
                }
            } else {
                def newVolume = new StorageVolume(
                        type: volumeType,
                        name: item.name,
                        cloudId: cloud?.id,
                        maxStorage: totalSize,
                        usedStorage: usedSpace,
                        externalId: externalId,
                        internalId: item.id,
                        volumePath: mountPoint
                )
                newVolume.datastore = savedDataStore
                context.async.storageVolume.create([newVolume], host).blockingGet()
                log.debug("syncVolume: newVolume created")
            }

        } catch (ex) {
            log.error "Error in volumeSync Datastores ${ex}", ex
        }
    }

    private getDataStoreExternalId(cloudItem) {
        if (cloudItem.partitionUniqueID) {
            return cloudItem.partitionUniqueID
        } else if (cloudItem.isClusteredSharedVolume && cloudItem.storageVolumeID) {
            return cloudItem.storageVolumeID
        } else {
            return "${cloudItem.name}|${cloudItem.vmHost}"
        }
    }

    private getName(ds, cluster, host) {
        def name = ds.name
        if (ds.isClusteredSharedVolume && cluster?.name) {
            name = "${cluster.name} : ${ds.name}"
        } else if (host?.name) {
            name = "${host.name} : ${ds.name}"
        }
        return name
    }
}