package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.BulkSaveResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeZonePool
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkType
import com.morpheusdata.model.ResourcePermission
import com.morpheusdata.model.projection.CloudPoolIdentity
import com.morpheusdata.model.projection.ComputeZonePoolIdentityProjection
import com.morpheusdata.model.projection.NetworkIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

@Slf4j
class ClustersSync {
    private MorpheusContext morpheusContext
    private Cloud cloud
    private ScvmmApiService apiService

    ClustersSync(MorpheusContext morpheusContext, Cloud cloud) {
        this.cloud = cloud
        this.morpheusContext = morpheusContext
        this.apiService = new ScvmmApiService(morpheusContext)
    }

    def execute() {
        log.debug "ClustersSync"
        try {
            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('zone.id', cloud.id))
            log.info("RAZI :: server.id: ${server.id}")

            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)
            log.info("RAZI :: scvmmOpts: ${scvmmOpts}")
            def listResults = apiService.listClusters(scvmmOpts)
            log.debug("clusters: {}", listResults)
            log.info("RAZI :: listResults: ${listResults}")
            log.info("RAZI :: listResults.success: ${listResults.success}")
            log.info("RAZI :: listResults.clusters: ${listResults.clusters}")

            if (listResults.success == true && listResults.clusters) {
                def objList = listResults.clusters
                def clusterScope = cloud.getConfigProperty('cluster')
                log.info("RAZI :: clusterScope: ${clusterScope}")
                if (clusterScope) {
                    objList = objList?.findAll{it.id == clusterScope}
                }
                log.info("RAZI :: objList: ${objList}")

                def masterAccount = cloud.owner.masterAccount
//                def existingItems = ComputeZonePool.where{ refType == 'ComputeZone' && refId == opts.zone.id }.list()

                Observable<CloudPoolIdentity> existingItems = morpheusContext.async.cloud.pool.listIdentityProjections(new DataQuery()
                        .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id))

                SyncTask<CloudPoolIdentity, Map, CloudPool> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)

                syncTask.addMatchFunction { CloudPoolIdentity existingItem, Map syncItem ->
                    log.info("RAZI :: existingItem?.externalId: ${existingItem?.externalId}")
                    log.info("RAZI :: syncItem?.id: ${syncItem?.id}")
                    existingItem?.externalId == syncItem?.id
                }.onDelete { removeItems ->
                    log.info("RAZI :: removeMissingResourcePools >> call START")
                    removeMissingResourcePools(removeItems)
                    log.info("RAZI :: removeMissingResourcePools >> call STOP")
                }.onUpdate { List<SyncTask.UpdateItem<CloudPool, Map>> updateItems ->
                    log.info("RAZI :: updateMatchedResourcePools >> call START")
                    updateMatchedResourcePools(updateItems)
                    log.info("RAZI :: updateMatchedResourcePools >> call STOP")
                }.onAdd { itemsToAdd ->
                    log.info("RAZI :: addMissingResourcePools >> call START")
                    addMissingResourcePools(itemsToAdd)
                    log.info("RAZI :: addMissingResourcePools >> call STOP")
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<CloudPoolIdentity, Map>> updateItems ->
                    return morpheusContext.async.cloud.pool.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
                if(masterAccount == false) {
                    // TODO: below code would be implemented in core
//                    zonePoolService.chooseOwnerPoolDefaults(cloud.owner, cloud)
                }
            } else {
                log.error("Error not getting the listClusters")
            }
        } catch (e) {
            log.error("ClustersSync error: ${e}", e)
        }
    }

    private addMissingResourcePools(Collection<Map> addList){
        log.debug("addMissingResourcePools: ${addList.size()}")
        log.info("RAZI :: addMissingResourcePools >> addList.size(): ${addList.size()}")

        List<CloudPool> clusterAdds = []
        List<ResourcePermission> resourcePerms = []
        try{
            addList?.each { Map item ->
                log.debug("add cluster: {}", item)
                def poolConfig = [
                        owner       : cloud.owner,
                        name        : item.name,
                        externalId  : item.id,
                        internalId  : item.name,
                        refType     : 'ComputeZone',
                        refId       : cloud.id,
                        cloud        : cloud,
                        category    : "scvmm.cluster.${cloud.id}",
                        code        : "scvmm.cluster.${cloud.id}.${item.id}",
                        readOnly    : false,
                        type        : 'Cluster',
                        active      : cloud.defaultPoolSyncActive
                ]
                log.info("RAZI :: addMissingResourcePools >> poolConfig: ${poolConfig}")
                CloudPool clusterAdd = new CloudPool(poolConfig)
                log.info("RAZI :: addMissingResourcePools >> item.sharedVolumes: ${item.sharedVolumes}")
                clusterAdd.setConfigProperty('sharedVolumes', item.sharedVolumes)
                clusterAdds << clusterAdd
            }

            log.info("RAZI :: addMissingResourcePools >> clusterAdds.size(): ${clusterAdds.size()}")
            if (clusterAdds.size() > 0){
                morpheusContext.async.cloud.pool.bulkCreate(clusterAdds).blockingGet()
                morpheusContext.async.cloud.pool.bulkSave(clusterAdds).blockingGet()
            }

            clusterAdds?.each {cluster ->
                log.info("RAZI :: cluster.id: ${cluster.id}")
                log.info("RAZI :: cluster.externalId: ${cluster.externalId}")
                def permissionConfig = [
                        morpheusResourceType    : 'ComputeZonePool',
                        uuid                    : cluster.externalId,
                        morpheusResourceId      : cluster.id,
                        account                 : cloud.account
                ]
                ResourcePermission resourcePerm = new ResourcePermission(permissionConfig)
                resourcePerms << resourcePerm
            }
            log.info("RAZI :: addMissingResourcePools >> resourcePerms.size(): ${resourcePerms.size()}")
            resourcePerms?.each {parms ->
                log.info("RAZI :: uuid: ${parms.uuid}")
                log.info("RAZI :: morpheusResourceId: ${parms.morpheusResourceId}")
            }
            if(resourcePerms.size() > 0){
                morpheusContext.async.resourcePermission.bulkCreate(resourcePerms).blockingGet()
            }
        } catch (e) {
            log.error "Error in addMissingResourcePools: ${e}", e
        }
    }

    private updateMatchedResourcePools(List<SyncTask.UpdateItem<CloudPool, Map>> updateList){
        log.debug("updateMatchedResourcePools: ${updateList.size()}")
        log.info("RAZI :: updateMatchedResourcePools >> updateList.size(): ${updateList.size()}")

        List<CloudPool> itemsToUpdate = []
        try {
            updateList?.each { updateMap -> //masterItem, existingItem
                def doSave = false

                CloudPool existingItem = updateMap.existingItem
                def masterItem = updateMap.masterItem

                // Sometimes scvmm tells us that the cluster has no shared volumes even when it does! #175290155
                if (existingItem.getConfigProperty('sharedVolumes') != masterItem.sharedVolumes) {
                    if(!masterItem.sharedVolumes || (masterItem.sharedVolumes?.size() == 1 && masterItem.sharedVolumes[0] == null)) {
                        // No shared volumes
                        def nullCount = existingItem.getConfigProperty('nullSharedVolumeSyncCount')?.toLong() ?: 0
                        if (nullCount >= 5) {
                            existingItem.setConfigProperty('sharedVolumes', masterItem.sharedVolumes)
                        } else {
                            existingItem.setConfigProperty('nullSharedVolumeSyncCount', ++nullCount)
                        }
                    } else {
                        // Have shared volumes
                        existingItem.setConfigProperty('sharedVolumes', masterItem.sharedVolumes)
                        existingItem.setConfigProperty('nullSharedVolumeSyncCount', 0)
                    }
                    doSave = true
                }

                log.info("RAZI :: doSave: ${doSave}")
                if(doSave) {
                    itemsToUpdate << existingItem
                }
            }
            log.info("RAZI :: itemsToUpdate.size(): ${itemsToUpdate.size()}")
            if (itemsToUpdate.size() > 0) {
                morpheusContext.async.cloud.pool.bulkSave(itemsToUpdate).blockingGet()
            }
        } catch(e) {
            log.error "Error in updateMatchedResourcePools ${e}", e
        }
    }

    private removeMissingResourcePools(List<CloudPoolIdentity> removeList) {
        log.debug "removeMissingResourcePools: ${removeList?.size()}"
        log.info("RAZI :: updateMatchedResourcePools >> removeList.size(): ${removeList.size()}")

        def deleteList = []
        try {
            removeList?.each {  removeItem ->
                log.debug("removing: ${}", removeItem)
                //clear out associations
    //            ComputeServer.where{ resourcePool == removeItem }.updateAll(resourcePool:null)
                def serversToUpdate = morpheusContext.services.computeServer.list(new DataQuery()
                        .withFilter("resourcePool.id", removeItem.id))
                if (serversToUpdate) {
                    serversToUpdate.each { server -> server.resourcePool = null }
                    morpheusContext.async.computeServer.bulkSave(serversToUpdate).blockingGet()
                }
    //            Datastore.where{ zonePool == removeItem}.updateAll(zonePool:null)
                def datastoresToUpdate = morpheusContext.services.cloud.datastore.list(new DataQuery()
                        .withFilter('zonePool.id', removeItem.id))
                if (datastoresToUpdate) {
                    datastoresToUpdate.each { datastore -> datastore.zonePool = null }
                    morpheusContext.async.cloud.datastore.bulkSave(datastoresToUpdate).blockingGet()
                }
    //            Network.where{ zonePool == removeItem}.updateAll(zonePool:null)
                def networksToUpdate = morpheusContext.services.cloud.network.list(new DataQuery()
                        .withFilter('cloudPool.id', removeItem.id))
                if (networksToUpdate) {
                    networksToUpdate.each { network -> network.cloudPool = null }
                    morpheusContext.async.cloud.network.bulkSave(networksToUpdate).blockingGet()
                }
    //            ComputeZonePool.where{parent == removeItem}.updateAll(parent:null)
                def cloudPoolsToUpdate = morpheusContext.services.cloud.pool.list(new DataQuery()
                        .withFilter('parent.id', removeItem.id))
                if (cloudPoolsToUpdate) {
                    cloudPoolsToUpdate.each { cloudPool -> cloudPool.parent = null }
                    morpheusContext.async.cloud.pool.bulkSave(cloudPoolsToUpdate).blockingGet()
                }
                deleteList << removeItem
            }

            log.info("RAZI :: itemsToUpdate.size(): ${deleteList.size()}")
            if(deleteList.size() > 0) {
                morpheusContext.async.cloud.pool.bulkRemove(deleteList).blockingGet()
            }
        } catch (e) {
            log.error("Error in removeMissingResourcePools: ${e}", e)
        }
    }
}
