package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkSubnetType
import com.morpheusdata.model.NetworkType
import com.morpheusdata.model.projection.NetworkIdentityProjection
import groovy.util.logging.Slf4j

@Slf4j
class IsolationNetworkSync {
    private MorpheusContext morpheusContext
    private Cloud cloud
    private ScvmmApiService apiService

    IsolationNetworkSync(MorpheusContext morpheusContext, Cloud cloud, ScvmmApiService apiService) {
        this.cloud = cloud
        this.morpheusContext = morpheusContext
        this.apiService = apiService
    }

    def execute() {
        log.debug "IsolationNetworkSync"
        try {
//            def networkType = NetworkType.findByCode('scvmmVLANNetwork')
            def networkType = new NetworkType(code: 'scvmmVLANNetwork')

            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('zone.id', cloud.id))
//            def scvmmOpts = ScvmmComputeUtility.getScvmmZoneAndHypervisorOpts(opts.zone, node, scvmmProvisionService)
//            def listResults = ScvmmComputeUtility.listNoIsolationVLans(scvmmOpts)
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)
            def listResults = apiService.listNoIsolationVLans(scvmmOpts)

            if (listResults.success == true && listResults.networks) {
                def objList = listResults?.networks
                log.debug("objList: {}", objList)
                if(!objList) {
                    log.info "No networks returned!"
                }

                def existingItems = morpheusContext.async.cloud.network.listIdentityProjections(new DataQuery()
                        .withFilters(
                                new DataOrFilter(
                                        new DataFilter('owner', cloud.account),
                                        new DataFilter('owner', cloud.owner)
                                ),
                                new DataFilter('category', '=~', "scvmm.vlan.network.${cloud.id}.%")))

                SyncTask<NetworkIdentityProjection, Map, Network> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)

                syncTask.addMatchFunction { NetworkIdentityProjection network, Map networkItem ->
                    network?.externalId == networkItem?.ID
                }.onDelete { removeItems ->
                    morpheusContext.async.cloud.network.remove(removeItems).blockingGet()
                }.onUpdate { List<SyncTask.UpdateItem<Network, Map>> updateItems ->
                    updateMatchedNetworks(updateItems, networkTypes, subnetType, server)
                }.onAdd { itemsToAdd ->
                    addMissingNetworks(itemsToAdd, networkTypes, subnetType, server)
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<NetworkIdentityProjection, Map>> updateItems ->
                    return morpheusContext.async.cloud.network.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            } else {
                log.error("Error not getting the listNetworks")
            }
        } catch (e) {
            log.error("cacheNetworks error: ${e}", e)
        }
    }

}
