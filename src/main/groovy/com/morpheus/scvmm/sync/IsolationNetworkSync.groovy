package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkSubnet
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
            def networkType = new NetworkType(code: 'scvmmVLANNetwork')
            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('zone.id', cloud.id))

            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)
            def listResults = apiService.listNoIsolationVLans(scvmmOpts)

            if (listResults.success == true && listResults.networks) {
                def objList = listResults?.networks
                log.debug("objList: {}", objList)
                if (!objList) {
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
                    updateMatchedNetworks(updateItems)
                }.onAdd { itemsToAdd ->
                    addMissingNetworks(itemsToAdd, networkType, server)
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<NetworkIdentityProjection, Map>> updateItems ->
                    return morpheusContext.async.cloud.network.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            } else {
                log.error("Error not getting the listNetworks")
            }
        } catch (e) {
            log.error("IsolationNetworkSync error: ${e}", e)
        }
    }

    private addMissingNetworks(Collection<Map> addList, NetworkType networkType, ComputeServer server) {
        log.debug("IsolationNetworkSync >> addMissingNetworks >> called")
        def networkAdds = []
        try {
            addList?.each { networkItem ->

                def networkConfig = [
                        code      : "scvmm.vlan.network.${cloud.id}.${server.id}.${networkItem.ID}",
                        cidr      : networkItem.Subnet,
                        vlanId    : networkItem.VLanID,
                        category  : "scvmm.vlan.network.${cloud.id}.${server.id}",
                        cloud     : cloud,
                        dhcpServer: true,
                        uniqueId  : networkItem.ID,
                        name      : networkItem.Name,
                        externalId: networkItem.ID,
                        type      : networkType,
                        refType   : 'ComputeZone',
                        refId     : cloud.id,
                        owner     : cloud.owner
                ]

                Network networkAdd = new Network(networkConfig)
                networkAdds << networkAdd
            }

            //create networks
            morpheusContext.async.cloud.network.create(networkAdds).blockingGet()
        } catch (e) {
            log.error "Error in adding Isolation Network sync ${e}", e
        }
    }

    private updateMatchedNetworks(List<SyncTask.UpdateItem<Network, Map>> updateList) {
        log.debug("IsolationNetworkSync:updateMatchedNetworks: Entered")
        List<Network> itemsToUpdate = []
        try {
            for (update in updateList) {
                Network network = update.existingItem
                def masterItem = update.masterItem
                log.debug "processing update: ${network}"
                if (network) {
                    def save = false
                    if(network.cidr != masterItem.Subnet){
                        network.cidr = masterItem.Subnet
                        save = true
                    }
                    if(network.vlanId != masterItem.VLanID){
                        network.vlanId = masterItem.VLanID
                        save = true
                    }
                    if (save) {
                        itemsToUpdate << network
                    }
                }
            }
            if (itemsToUpdate.size() > 0) {
                morpheusContext.async.cloud.network.save(itemsToUpdate).blockingGet()
            }
        } catch(e) {
            log.error "Error in update Isolation Network sync ${e}", e
        }
    }
}
