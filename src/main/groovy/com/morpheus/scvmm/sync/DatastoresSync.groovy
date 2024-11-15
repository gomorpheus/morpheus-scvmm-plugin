package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.*
import com.morpheusdata.model.projection.DatastoreIdentityProjection
import groovy.util.logging.Slf4j

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
            def clusters = context.services.cloud.pool.find(new DataQuery()
                    .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id)
                    .withFilter('type', 'Cluster')
                    .withFilter('internalId', '!=', null))
            def volumeType = context.services.storageVolume.storageVolumeType.find(new DataQuery()
                    .withFilter('code', 'scvmm-datastore'))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
            def listResults = apiService.listDatastores(scvmmOpts)
            if (listResults.success == true && listResults.datastores) {
                def serverType = context.async.cloud.findComputeServerTypeByCode("scvmmHypervisor").blockingGet()
                def existingHosts = context.services.computeServer.list(new DataQuery()
                        .withFilter('zone.id', cloud.id)
                        .withFilter('computeServerType.code', 'scvmmHypervisor'))

                def existingItems = context.async.cloud.datastore.listIdentityProjections(new DataQuery()
                        .withFilter('category', '=~', 'scvmm.datastore.%').withFilter('refType', 'ComputeZone')
                        .withFilter('refId', cloud.id).withFilter('type', 'generic'))

                SyncTask<DatastoreIdentityProjection, Map, Datastore> syncTask = new SyncTask<>(existingItems, listResults?.datastores as Collection<Map>)
                // check: deprication
                syncTask.addMatchFunction { existingItem, cloudItem ->
                    existingItem.externalId == getDataStoreExternalId(cloudItem)
                }.onDelete { removeItems ->
                    removeMissingDatastores(removeItems)
                }.onUpdate { updateItems ->
                    updateMatchedDatastores(updateItems, clusters, existingHosts, volumeType)
                }.onAdd { itemsToAdd ->
                    addMissingDatastores(itemsToAdd, clusters, existingHosts, volumeType)
                }.withLoadObjectDetailsFromFinder { updateItems ->
                    return context.async.cloud.datastore.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            }

        } catch (ex) {
            log.error("DatastoresSync error:${ex}", ex)
        }
    }

    private removeMissingDatastores(Collection<DatastoreIdentityProjection> removeList) {
        log.debug("removeMissingDatastores: ${cloud} ${removeList.size()}")
        context.async.cloud.datastore.remove(removeList).blockingGet()
    }

    private void updateMatchedDatastores(List<SyncTask.UpdateItem<Datastore, Map>> updateList, CloudPool clusters, existingHosts, volumeType) {
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
                        c.getConfigProperty('sharedVolumes')?.contains(masterItem.name) && (!host || host.resourcePool.id == c.id)
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
                        syncVolume(item, host, savedDataStore, volumeType, getDataStoreExternalId(item))
                    }
                }
            }
        } catch (e) {
            log.error("Error in updateMatchedDatastores method: ${e}", e)
        }
    }

    private addMissingDatastores(Collection<Map> addList, CloudPool clusters, existingHosts, volumeType) {
        log.debug("IsolationNetworkSync >> addMissingNetworks >> called")
        try {
            addList?.each { Map item ->
                def isSharedVolume = item.isClusteredSharedVolume
                def externalId = getDataStoreExternalId(item)
                ComputeServer host = existingHosts.find { it.hostname == item.vmHost }
                def cluster
                if (isSharedVolume) {
                    cluster = clusters.find { c ->
                        c.getConfigProperty('sharedVolumes')?.contains(item.name) && (!host || host.resourcePool.id == c.id)
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
                log.info "Created datastore for id: ${getDataStoreExternalId(item)}"
                Datastore datastore = new Datastore(datastoreConfig)
                def savedDataStore = context.async.cloud.datastore.create(datastore).blockingGet()
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
                /*if(existingVolume.volumePath != mountPoint){ // check: volumePath does not exist in StorageVolume
                    existingVolume.volumePath = mountPoint
                    save = true
                }*/
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
                        internalId: item.id
                        //volumePath:mountPoint, // check:
                )
                newVolume.datastore = savedDataStore
                //host.addToVolumes(newVolume)
                //newVolume.save(flush:true,failOnError:true)
                //host.save(flush:true)
                context.async.storageVolume.create([newVolume], host).blockingGet()
            }

        } catch (ex) {
            log.error "Error in volumeSync Datastores ${ex}", ex
        }
    }

    private getDataStoreExternalId(ds) {
        if (ds.partitionUniqueID) {
            return ds.partitionUniqueID
        } else if (ds.isClusteredSharedVolume && ds.storageVolumeID) {
            return ds.storageVolumeID
        } else {
            return "${ds.name}|${ds.vmHost}"
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
