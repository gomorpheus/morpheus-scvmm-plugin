package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Account
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.CloudPool
import com.morpheusdata.model.ResourcePermission
import com.morpheusdata.model.projection.CloudPoolIdentity
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

            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)
            def listResults = apiService.listClusters(scvmmOpts)
            log.debug("clusters: {}", listResults)

            if (listResults.success == true && listResults.clusters) {
                def objList = listResults.clusters
                def clusterScope = cloud.getConfigProperty('cluster')
                if (clusterScope) {
                    objList = objList?.findAll{it.id == clusterScope}
                }

                def masterAccount = cloud.owner.masterAccount

                Observable<CloudPoolIdentity> existingItems = morpheusContext.async.cloud.pool.listIdentityProjections(new DataQuery()
                        .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id))

                SyncTask<CloudPoolIdentity, Map, CloudPool> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)

                syncTask.addMatchFunction { CloudPoolIdentity existingItem, Map syncItem ->
                    existingItem?.externalId == syncItem?.id
                }.onDelete { removeItems ->
                    removeMissingResourcePools(removeItems)
                }.onUpdate { List<SyncTask.UpdateItem<CloudPool, Map>> updateItems ->
                    updateMatchedResourcePools(updateItems)
                }.onAdd { itemsToAdd ->
                    addMissingResourcePools(itemsToAdd)
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<CloudPoolIdentity, Map>> updateItems ->
                    return morpheusContext.async.cloud.pool.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
                if(masterAccount == false) {
                    chooseOwnerPoolDefaults(cloud.owner)
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
                CloudPool clusterAdd = new CloudPool(poolConfig)
                clusterAdd.setConfigProperty('sharedVolumes', item.sharedVolumes)
                clusterAdds << clusterAdd
            }

            if (clusterAdds.size() > 0){
                morpheusContext.async.cloud.pool.bulkCreate(clusterAdds).blockingGet()
                morpheusContext.async.cloud.pool.bulkSave(clusterAdds).blockingGet()
            }

            clusterAdds?.each {cluster ->
                def permissionConfig = [
                        morpheusResourceType    : 'ComputeZonePool',
                        uuid                    : cluster.externalId,
                        morpheusResourceId      : cluster.id,
                        account                 : cloud.account
                ]
                ResourcePermission resourcePerm = new ResourcePermission(permissionConfig)
                resourcePerms << resourcePerm
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

                if(doSave) {
                    itemsToUpdate << existingItem
                }
            }

            if (itemsToUpdate.size() > 0) {
                morpheusContext.async.cloud.pool.bulkSave(itemsToUpdate).blockingGet()
            }
        } catch(e) {
            log.error "Error in updateMatchedResourcePools ${e}", e
        }
    }

    private removeMissingResourcePools(List<CloudPoolIdentity> removeList) {
        log.debug "removeMissingResourcePools: ${removeList?.size()}"

        def deleteList = []
        try {
            removeList?.each {  removeItem ->
                log.debug("removing: ${}", removeItem)
                //clear out associations
                def serversToUpdate = morpheusContext.services.computeServer.list(new DataQuery()
                        .withFilter("resourcePool.id", removeItem.id))
                if (serversToUpdate) {
                    serversToUpdate.each { server -> server.resourcePool = null }
                    morpheusContext.async.computeServer.bulkSave(serversToUpdate).blockingGet()
                }
                def datastoresToUpdate = morpheusContext.services.cloud.datastore.list(new DataQuery()
                        .withFilter('zonePool.id', removeItem.id))
                if (datastoresToUpdate) {
                    datastoresToUpdate.each { datastore -> datastore.zonePool = null }
                    morpheusContext.async.cloud.datastore.bulkSave(datastoresToUpdate).blockingGet()
                }
                def networksToUpdate = morpheusContext.services.cloud.network.list(new DataQuery()
                        .withFilter('cloudPool.id', removeItem.id))
                if (networksToUpdate) {
                    networksToUpdate.each { network -> network.cloudPool = null }
                    morpheusContext.async.cloud.network.bulkSave(networksToUpdate).blockingGet()
                }
                def cloudPoolsToUpdate = morpheusContext.services.cloud.pool.list(new DataQuery()
                        .withFilter('parent.id', removeItem.id))
                if (cloudPoolsToUpdate) {
                    cloudPoolsToUpdate.each { cloudPool -> cloudPool.parent = null }
                    morpheusContext.async.cloud.pool.bulkSave(cloudPoolsToUpdate).blockingGet()
                }
                deleteList << removeItem
            }

            if(deleteList.size() > 0) {
                morpheusContext.async.cloud.pool.bulkRemove(deleteList).blockingGet()
            }
        } catch (e) {
            log.error("Error in removeMissingResourcePools: ${e}", e)
        }
    }

    def chooseOwnerPoolDefaults(Account currentAccount) {
        //check for default store and set if not
        def pool = morpheusContext.services.cloud.pool.find(new DataQuery()
                .withFilter('owner', currentAccount)
                .withFilter('refType', 'ComputeZone')
                .withFilter('refId', cloud.id)
                .withFilter('defaultPool', true))

        if(pool && pool.readOnly == true) {
            pool.defaultPool = false
            morpheusContext.services.cloud.pool.save(pool)
            pool = null
        }

        if(!pool) {
            pool = morpheusContext.services.cloud.pool.find(new DataQuery()
                    .withFilter('owner', currentAccount)
                    .withFilter('refType', 'ComputeZone')
                    .withFilter('refId', cloud.id)
                    .withFilter('defaultPool', false)
                    .withFilter('readOnly', '!=', true))
            if(pool) {
                pool.defaultPool = true
                morpheusContext.services.cloud.pool.save(pool)
            }
        }
    }
}
