package com.morpheusdata.scvmm.sync

import com.morpheusdata.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeCapacityInfo
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.OsType
import com.morpheusdata.model.projection.ComputeServerIdentityProjection
import com.morpheusdata.scvmm.logging.LogInterface
import com.morpheusdata.scvmm.logging.LogWrapper
import groovy.util.logging.Slf4j

/**
 * @author rahul.ray
 */

class HostSync {

    private Cloud cloud
    private ComputeServer node
    private MorpheusContext context
    private ScvmmApiService apiService
    private LogInterface log = LogWrapper.instance

    HostSync(Cloud cloud, ComputeServer node, MorpheusContext context) {
        this.cloud = cloud
        this.node = node
        this.context = context
        this.apiService = new ScvmmApiService(context)

    }

    def execute() {
        log.debug "HostSync"
        try {
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
            def listResults = apiService.listHosts(scvmmOpts)
            log.debug("HostSync: listResults: ${listResults}")
            if (listResults.success == true && listResults.hosts) {
                // Determine the scope for the zone (both hostgroup and cluster)
                def hostGroupScope = cloud.getConfigProperty('hostGroup')
                def matchAllHostGroups = !hostGroupScope
                def clusterScope = cloud.getConfigProperty('cluster')
                def matchAllClusters = !clusterScope
                // clusters for the cloud
                def clusters = context.services.cloud.pool.list(new DataQuery()
                        .withFilter('refType', 'ComputeZone').withFilter('refId', cloud.id))
                log.debug("HostSync: clusters?.size(): ${clusters?.size()}")
                // Filter master list down to those that match the host group and cluster
                def objList = []
                listResults.hosts?.each { item ->
                    def hostGroupMatch = matchAllHostGroups || apiService.isHostInHostGroup(item.hostGroup, hostGroupScope)
                    def cluster = clusters.find { it.internalId == item.cluster }
                    def clusterMatch = matchAllClusters || cluster
                    if (hostGroupMatch && clusterMatch) {
                        objList << item
                    }
                }
                log.debug("HostSync: objList?.size(): ${objList?.size()}")
                if (objList?.size() > 0) {
                    def existingItems = context.async.computeServer.listIdentityProjections(
                            new DataQuery().withFilter("zone.id", cloud.id).withFilter("computeServerType.code", 'scvmmHypervisor')
                    )
                    SyncTask<ComputeServerIdentityProjection, Map, ComputeServer> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)
                    syncTask.addMatchFunction { ComputeServerIdentityProjection domainObject, Map cloudItem ->
                        domainObject.externalId == cloudItem?.id
                    }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<ComputeServerIdentityProjection, Map>> updateItems ->
                        context.async.computeServer.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                    }.onAdd { itemsToAdd ->
                        log.debug("HostSync, onAdd: ${itemsToAdd}")
                        addMissingHosts(itemsToAdd, clusters)
                    }.onUpdate { List<SyncTask.UpdateItem<ComputeServer, Map>> updateItems ->
                        log.debug("HostSync, onUpdate: ${updateItems}")
                        updateMatchedHosts(updateItems, clusters)
                    }.onDelete { List<ComputeServerIdentityProjection> removeItems ->
                        log.debug("HostSync, onDelete: ${removeItems}")
                        removeMissingHosts(removeItems)
                    }.start()
                }
            } else {
                log.error "Error in getting hosts : ${listResults}"
            }
        } catch (e) {
            log.error("HostSync error: ${e}", e)
        }
    }

    private updateMatchedHosts(List<SyncTask.UpdateItem<ComputeServer, Map>> updateList, clusters) {
        log.debug "HostSync: updateMatchedHosts: ${cloud.id} ${updateList.size()}"
        try {
            for (updateItem in updateList) {
                def existingItem = updateItem.existingItem
                def masterItem = updateItem.masterItem
                def cluster = clusters.find { it.internalId == masterItem.cluster }
                // May be null if Host not in a cluster
                if (existingItem.resourcePool != cluster) {
                    existingItem.resourcePool = cluster
                    def savedServer = context.async.computeServer.save(existingItem).blockingGet()
                    log.debug("savedServer?.id: ${savedServer?.id}")
                    if (savedServer) {
                        updateHostStats(savedServer, masterItem)
                    }
                }
            }
        } catch (e) {
            log.error("HostSync: updateMatchedHosts error: ${e}", e)
        }
    }

    private addMissingHosts(Collection<Map> addList, clusters) {
        log.debug "HostSync: addMissingHosts: ${cloud} ${addList.size()}"
        try {
            def serverType = context.async.cloud.findComputeServerTypeByCode("scvmmHypervisor").blockingGet()
            for (cloudItem in addList) {
                def serverOs = getHypervisorOs(cloudItem.os)
                def cluster = clusters.find { it.internalId == cloudItem.cluster }
                // May be null if Host not in a cluster
                def serverConfig =
                        [
                                account          : cloud.owner,
                                category         : "scvmm.host.${cloud.id}",
                                name             : cloudItem.computerName,
                                resourcePool     : cluster,
                                externalId       : cloudItem.id,
                                cloud            : cloud,
                                sshUsername      : 'Admnistrator',
                                apiKey           : java.util.UUID.randomUUID(),
                                status           : 'provisioned',
                                provision        : false,
                                singleTenant     : false,
                                serverType       : 'hypervisor',
                                computeServerType: serverType,
                                statusDate       : new Date(),
                                serverOs         : serverOs,
                                osType           : 'windows',
                                hostname         : cloudItem.name
                        ]
                log.debug("serverConfig: ${serverConfig}")
                def newServer = new ComputeServer(serverConfig)
                newServer.maxMemory = cloudItem.totalMemory?.toLong() ?: 0
                newServer.maxStorage = cloudItem.totalStorage?.toLong() ?: 0
                newServer.maxCpu = (cloudItem.cpuCount?.toLong() ?: 1)
                newServer.maxCores = (cloudItem.cpuCount?.toLong() ?: 1) * (cloudItem.coresPerCpu?.toLong() ?: 1)
                newServer.capacityInfo = new ComputeCapacityInfo(maxMemory: newServer.maxMemory, maxStorage: newServer.maxStorage, maxCores: newServer.maxCores)
                newServer.setConfigProperty('rawData', cloudItem.encodeAsJSON().toString())
                def savedServer = context.async.computeServer.create(newServer).blockingGet()
                log.debug("savedServer?.id: ${savedServer?.id}")
                if (savedServer) {
                    updateHostStats(savedServer, cloudItem)
                }
            }
        } catch (e) {
            log.error("HostSync: addMissingHosts error: ${e}", e)
        }
    }

    def removeMissingHosts(List<ComputeServerIdentityProjection> removeList) {
        log.debug "HostSync: removeMissingHosts: ${removeList.size()}"
        try {
            def parentServers = context.services.computeServer.list(
                    new DataQuery().withFilter("parentServer.id", "in", removeList.collect { it.id })
            )
            def updatedServers = []
            parentServers?.each { server ->
                server.parentServer = null
                updatedServers << server
            }
            if (updatedServers?.size() > 0) {
                context.async.computeServer.bulkSave(updatedServers).blockingGet()
            }
            context.async.computeServer.bulkRemove(removeList).blockingGet()
        } catch (ex) {
            log.error("HostSync: removeMissingHosts error: ${ex}", ex)
        }
    }

    def getHypervisorOs(name) {
        def rtn
        if (name?.indexOf('2016') > -1)
            rtn = new OsType(code: 'windows.server.2016')
        else
            rtn = new OsType(code: 'windows.server.2012')
        return rtn
    }

    def updateHostStats(ComputeServer server, hostMap) {
        log.debug("HostSync: updateHostStats: hostMap: ${hostMap}")
        try {
            //storage
            def maxStorage = hostMap.totalStorage?.toLong() ?: 0
            def maxUsedStorage = hostMap.usedStorage?.toLong() ?: 0
            //cpu
            def maxCores = (hostMap.cpuCount?.toLong() ?: 1) * (hostMap.coresPerCpu?.toLong() ?: 1)
            def maxCpu = (hostMap.cpuCount?.toLong() ?: 1)
            def cpuPercent = hostMap.cpuUtilization?.toLong()
            //memory
            def maxMemory = hostMap.totalMemory?.toLong() ?: 0
            def maxUsedMemory = maxMemory - ((hostMap.availableMemory?.toLong() ?: 0) * ComputeUtility.ONE_MEGABYTE)
            //power state
            def powerState = hostMap.hyperVState == 'Running' ? 'on' : hostMap.hyperVState == 'Stopped' ? 'off' : 'unknown'
            //save it all
            def updates = false
            if (powerState == 'on') {
                updates = true
            }
            def capacityInfo = server.capacityInfo ?: new ComputeCapacityInfo(maxMemory: maxMemory, maxStorage: maxStorage)
            if (maxCpu != server.maxCpu) {
                server.maxCpu = maxCpu
                updates = true
            }
            if (maxCores != server.maxCores) {
                server.maxCores = maxCores
                updates = true
            }
            if (maxMemory > server.maxMemory) {
                server.maxMemory = maxMemory
                capacityInfo?.maxMemory = maxMemory
                updates = true
            }
            if (maxUsedMemory != capacityInfo.usedMemory) {
                server.usedMemory = maxUsedMemory
                capacityInfo.usedMemory = maxUsedMemory
                updates = true
            }
            if (maxStorage != server.maxStorage) {
                server.maxStorage = maxStorage
                capacityInfo?.maxStorage = maxStorage
                updates = true
            }
            if (maxUsedStorage != capacityInfo.usedStorage) {
                server.usedStorage = maxUsedStorage
                capacityInfo.usedStorage = maxUsedStorage
                updates = true
            }
            if (server.powerState != powerState) {
                server.powerState = powerState
                updates = true
            }
            if (hostMap.name && hostMap.name != server.hostname) {
                server.hostname = hostMap.name
                updates = true
            }
            if (cpuPercent) {
                updates = true
            }
            if (updates == true) {
                server.capacityInfo = capacityInfo
                context.async.computeServer.save(server).blockingGet()
            }
        } catch (e) {
            log.warn("HostSync: error updating host stats: ${e}", e)
        }
    }
}