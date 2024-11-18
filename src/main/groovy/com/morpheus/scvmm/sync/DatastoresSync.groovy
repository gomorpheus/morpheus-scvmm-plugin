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
            log.info ("Ray :: DatastoresSync: cloud.id: ${cloud.id}")
            def clusters = context.services.cloud.pool.list(new DataQuery()
                    .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id)
                    .withFilter('type', 'Cluster')
                    .withFilter('internalId', '!=', null))
            log.info ("Ray :: DatastoresSync: clusters: ${clusters}")
            log.info ("Ray :: DatastoresSync: clusters?.size(): ${clusters?.size()}")
            def volumeType = context.services.storageVolume.storageVolumeType.find(new DataQuery()
                    .withFilter('code', 'scvmm-datastore'))
            log.info ("Ray :: DatastoresSync: volumeType: ${volumeType}")
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
            log.info ("Ray :: DatastoresSync: scvmmOpts: ${scvmmOpts}")
            def listResults = apiService.listDatastores(scvmmOpts)
            log.info ("Ray :: DatastoresSync: listResults: ${listResults}")
            log.info ("Ray :: DatastoresSync: listResults.success: ${listResults.success}")
            log.info ("Ray :: DatastoresSync: listResults.datastores: ${listResults.datastores}")
            if (listResults.success == true && listResults.datastores) {
                def existingHosts = context.services.computeServer.list(new DataQuery()
                        .withFilter('zone.id', cloud.id)
                        .withFilter('computeServerType.code', 'scvmmHypervisor'))
                log.info ("Ray :: DatastoresSync: existingHosts: ${existingHosts}")
                log.info ("Ray :: DatastoresSync: existingHosts?.size(): ${existingHosts?.size()}")

                def existingItems = context.async.cloud.datastore.listIdentityProjections(new DataQuery()
                        .withFilter('category', '=~', 'scvmm.datastore.%').withFilter('refType', 'ComputeZone')
                        .withFilter('refId', cloud.id).withFilter('type', 'generic'))
                log.info ("Ray :: DatastoresSync: existingItems: ${existingItems}")

                SyncTask<DatastoreIdentityProjection, Map, Datastore> syncTask = new SyncTask<>(existingItems, listResults?.datastores as Collection<Map>)
                log.info ("Ray :: DatastoresSync: syncTask: ${syncTask}")
                // check: deprication
                syncTask.addMatchFunction { existingItem, cloudItem ->
                    log.info ("=================================================Ray :: start >>> DatastoresSync>>addMatchFunction:=================================================")
                    def id = getDataStoreExternalId(cloudItem)
                    log.info ("Ray :: DatastoresSync>>addMatchFunction: existingItem.externalId: ${existingItem.externalId?.toString()}")
                    log.info ("Ray :: DatastoresSync>>addMatchFunction: id: ${getDataStoreExternalId(cloudItem)?.toString()}")
                    existingItem.externalId?.toString() == id?.toString()
                    log.info ("=================================================Ray :: end >>> DatastoresSync>>addMatchFunction:=================================================")
                }.onDelete { removeItems ->
                    log.info ("Ray :: DatastoresSync>>onDelete: removeItems: ${removeItems}")
                    log.info ("Ray :: DatastoresSync>>onDelete: removeItems?.size(): ${removeItems?.size()}")
                    removeMissingDatastores(removeItems)
                }.onUpdate { updateItems ->
                    log.info ("Ray :: DatastoresSync>>onUpdate: updateItems: ${updateItems}")
                    updateMatchedDatastores(updateItems, clusters, existingHosts, volumeType)
                }.onAdd { itemsToAdd ->
                    log.info ("Ray :: DatastoresSync>>onAdd: itemsToAdd: ${itemsToAdd}")
                    addMissingDatastores(itemsToAdd, clusters, existingHosts, volumeType)
                }.withLoadObjectDetailsFromFinder { updateItems ->
                    return context.async.cloud.datastore.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            }
        } catch (ex) {
            log.error("Ray :: DatastoresSync error: {}", ex.getMessage())
        }
    }

    private removeMissingDatastores(Collection<DatastoreIdentityProjection> removeList) {
        log.debug("removeMissingDatastores: ${cloud} ${removeList.size()}")
        log.info ("Ray :: removeMissingDatastores: ${cloud} ${removeList.size()}")
        try {
            context.async.cloud.datastore.remove(removeList).blockingGet()
        } catch (ex) {
            log.error("Ray :: DatastoresSync>>removeMissingDatastores error: {}", ex.getMessage())
        }
    }

    private void updateMatchedDatastores(List<SyncTask.UpdateItem<Datastore, Map>> updateList, clusters, existingHosts, volumeType) {
        try {
            log.debug("updateMatchedDatastores: ${updateList?.size()}")
            log.info ("Ray :: updateMatchedDatastores: clusters?.size(): ${clusters?.size()}")
            log.info ("Ray :: updateMatchedDatastores: existingHosts?.size(): ${existingHosts?.size()}")
            log.info ("Ray :: updateMatchedDatastores: volumeType: ${volumeType}")
            def host
            updateList.each { item ->
                def masterItem = item.masterItem
                log.info ("Ray :: updateMatchedDatastores: masterItem: ${masterItem}")
                def existingItem = item.existingItem
                log.info ("Ray :: updateMatchedDatastores: existingItem: ${existingItem}")
                log.info ("Ray :: updateMatchedDatastores: masterItem.vmHost: ${masterItem.vmHost}")
                host = existingHosts.find { it.hostname == masterItem.vmHost }
                log.info ("Ray :: updateMatchedDatastores: host: ${host}")
                log.info ("Ray :: updateMatchedDatastores: host?.id: ${host?.id}")
                def cluster
                log.info ("Ray :: updateMatchedDatastores: masterItem.isClusteredSharedVolume: ${masterItem.isClusteredSharedVolume}")
                log.info ("Ray :: updateMatchedDatastores: masterItem.name: ${masterItem.name}")
                if (masterItem.isClusteredSharedVolume) {
                    cluster = clusters.find { c ->
                        c.getConfigProperty('sharedVolumes')?.contains(masterItem.name) && (!host || host.resourcePool.id == c.id)
                    }
                }
                def name = getName(masterItem, cluster, host)
                log.info ("Ray :: updateMatchedDatastores: name: ${name}")
                def doSave = false

                log.info ("Ray :: updateMatchedDatastores: masterItem.isAvailableForPlacement: ${masterItem.isAvailableForPlacement}")
                if (existingItem.online != masterItem.isAvailableForPlacement) {
                    existingItem.online = masterItem.isAvailableForPlacement
                    doSave = true
                }
                if (existingItem.name != name) {
                    existingItem.name = name
                    doSave = true
                }
                def freeSpace = masterItem.freeSpace?.toLong() ?: 0
                log.info ("Ray :: updateMatchedDatastores: freeSpace: ${freeSpace}")
                if (existingItem.freeSpace != freeSpace) {
                    existingItem.freeSpace = freeSpace
                    doSave = true
                }
                def totalSize = masterItem.size ? masterItem.size?.toLong() : masterItem.capacity ? masterItem.capacity?.toLong() : 0
                log.info ("Ray :: updateMatchedDatastores: totalSize: ${totalSize}")
                if (existingItem.storageSize != totalSize) {
                    existingItem.storageSize = totalSize
                    doSave = true
                }
                if (existingItem.zonePool?.id != cluster?.id) {
                    existingItem.zonePool = cluster
                    doSave = true
                }
                log.info ("Ray :: updateMatchedDatastores: doSave: ${doSave}")
                if (doSave) {
                    def savedDataStore = context.async.cloud.datastore.save(existingItem).blockingGet()
                    log.info ("Ray :: updateMatchedDatastores: savedDataStore: ${savedDataStore}")
                    log.info ("Ray :: updateMatchedDatastores: savedDataStore?.id: ${savedDataStore?.id}")
                    if (savedDataStore && host) {
                        log.info ("Ray :: updateMatchedDatastores: before calling syncVolume....")
                        syncVolume(masterItem, host, savedDataStore, volumeType, getDataStoreExternalId(masterItem))
                        log.info ("Ray :: updateMatchedDatastores: after calling syncVolume....")
                    }
                }
            }
        } catch (e) {
            log.error("Ray :: Error in updateMatchedDatastores method: ${e}", e)
        }
    }

    private addMissingDatastores(Collection<Map> addList, clusters, existingHosts, volumeType) {
        log.debug("IsolationNetworkSync >> addMissingNetworks >> called")
        log.info ("Ray :: addMissingDatastores: addList?.size(): ${addList?.size()}")
        try {
            addList?.each { Map item ->
                def isSharedVolume = item.isClusteredSharedVolume
                log.info ("Ray :: addMissingDatastores: isSharedVolume: ${isSharedVolume}")
                def externalId = getDataStoreExternalId(item)
                log.info ("Ray :: addMissingDatastores: externalId: ${externalId}")
                log.info ("Ray :: addMissingDatastores: item.vmHost: ${item.vmHost}")
                ComputeServer host = existingHosts.find { it.hostname == item.vmHost }
                log.info ("Ray :: addMissingDatastores: host: ${host}")
                log.info ("Ray :: addMissingDatastores: host?.id: ${host?.id}")
                def cluster
                if (isSharedVolume) {
                    cluster = clusters.find { c ->
                        c.getConfigProperty('sharedVolumes')?.contains(item.name) && (!host || host.resourcePool.id == c.id)
                    }
                }
                log.info ("Ray :: addMissingDatastores: cluster: ${cluster}")
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
                log.info ("Ray :: addMissingDatastores: datastoreConfig: ${datastoreConfig}")
                log.info "Created datastore for id: ${externalId}"
                Datastore datastore = new Datastore(datastoreConfig)
                def savedDataStore = context.async.cloud.datastore.create(datastore).blockingGet()
                log.info ("Ray :: addMissingDatastores: savedDataStore: ${savedDataStore}")
                log.info ("Ray :: addMissingDatastores: savedDataStore?.id: ${savedDataStore?.id}")
                if (savedDataStore && host) {
                    log.info ("Ray :: addMissingDatastores: before calling syncVolume...")
                    syncVolume(item, host, savedDataStore, volumeType, externalId)
                    log.info ("Ray :: addMissingDatastores: after calling syncVolume...")
                }
            }
        } catch (e) {
            log.error "Ray :: Error in adding Datastores sync ${e}", e
        }
    }

    private syncVolume(item, ComputeServer host, savedDataStore, volumeType, externalId) {
        log.info ("Ray :: syncVolume: item: ${item}")
        log.info ("Ray :: syncVolume: host: ${host}")
        log.info ("Ray :: syncVolume: savedDataStore: ${savedDataStore}")
        log.info ("Ray :: syncVolume: volumeType: ${volumeType}")
        log.info ("Ray :: syncVolume: externalId: ${externalId}")
        try {
            // See if the volume is attached to the server yet
            def totalSize = item.size ? item.size?.toLong() : item.capacity ? item.capacity?.toLong() : 0
            log.info ("Ray :: syncVolume: totalSize: ${totalSize}")
            def freeSpace = item.freeSpace?.toLong() ?: 0
            log.info ("Ray :: syncVolume: freeSpace: ${freeSpace}")
            def usedSpace = totalSize - freeSpace
            log.info ("Ray :: syncVolume: usedSpace: ${usedSpace}")
            def mountPoint = item.mountPoints?.size() > 0 ? item.mountPoints[0] : null
            log.info ("Ray :: syncVolume: mountPoint: ${mountPoint}")
            StorageVolume existingVolume = host.volumes.find { it.externalId == externalId }
            log.info ("Ray :: syncVolume: existingVolume: ${existingVolume}")
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
                log.info ("Ray :: syncVolume: save: ${save}")
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
                log.info ("Ray :: syncVolume: newVolume: ${newVolume}")
                newVolume.datastore = savedDataStore
                //host.addToVolumes(newVolume)
                //newVolume.save(flush:true,failOnError:true)
                //host.save(flush:true)
                context.async.storageVolume.create([newVolume], host).blockingGet()
                log.info ("Ray :: syncVolume: newVolume created")
            }

        } catch (ex) {
            log.error "Ray :: Error in volumeSync Datastores ${ex}", ex
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
