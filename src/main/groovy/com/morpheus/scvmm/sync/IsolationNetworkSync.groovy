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
//            def networkType = NetworkType.findByCode('scvmmVLANNetwork')
            def networkType = new NetworkType(code: 'scvmmVLANNetwork')
            log.info("RAZI :: networkType: ${networkType}")

            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('zone.id', cloud.id))
//            def scvmmOpts = ScvmmComputeUtility.getScvmmZoneAndHypervisorOpts(opts.zone, node, scvmmProvisionService)
//            def listResults = ScvmmComputeUtility.listNoIsolationVLans(scvmmOpts)
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)
            def listResults = apiService.listNoIsolationVLans(scvmmOpts)
            log.info("RAZI :: listResults.success: ${listResults.success}")
            log.info("RAZI :: listResults.networks: ${listResults.networks}")

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
                    log.info("RAZI :: network?.externalId: ${network?.externalId}")
                    log.info("RAZI :: networkItem?.ID: ${networkItem?.ID}")
                    network?.externalId == networkItem?.ID
                }.onDelete { removeItems ->
                    log.info("RAZI :: onDelete >> call start")
                    morpheusContext.async.cloud.network.remove(removeItems).blockingGet()
                    log.info("RAZI :: onDelete >> call stop")
                }.onUpdate { List<SyncTask.UpdateItem<Network, Map>> updateItems ->
                    log.info("RAZI :: updateMatchedNetworks >> call start")
                    updateMatchedNetworks(updateItems, networkType)
                    log.info("RAZI :: updateMatchedNetworks >> call stop")
                }.onAdd { itemsToAdd ->
                    log.info("RAZI :: addMissingNetworks >> call start")
                    addMissingNetworks(itemsToAdd, networkType, server)
                    log.info("RAZI :: addMissingNetworks >> call stop")
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
        log.info("RAZI :: IsolationNetworkSync >> server.id: ${server.id}")
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
                        name      : networkItem.name,
                        externalId: networkItem.ID,
                        type      : networkType,
                        refType   : 'ComputeZone',
                        refId     : cloud.id,
                        owner     : cloud.owner
                ]
                log.info("RAZI :: networkConfig: ${networkConfig}")

                /*def networkConfig = [code:"scvmm.vlan.network.${opts.zone.id}.${node.id}.${networkItem.ID}", cidr: networkItem.Subnet, vlanId: networkItem.VLanID,
                                     category:"scvmm.vlan.network.${opts.zone.id}.${node.id}", zone:opts.zone, dhcpServer:true, uniqueId: networkItem.ID,
                                     name:networkItem.Name, externalId:networkItem.ID, type: networkType, refType:'ComputeZone', refId:"${opts.zone.id}", owner:opts.zone.owner]*/

                Network networkAdd = new Network(networkConfig)
                networkAdds << networkAdd
            }
            log.info("RAZI :: networkAdds.size(): ${networkAdds.size()}")

            //create networks
            morpheusContext.async.cloud.network.bulkCreate(networkAdds).blockingGet()
            log.info("RAZI :: networkAdds SUCCESS")
        } catch (e) {
            log.error "Error in adding Isolation Network sync ${e}", e
        }
    }

    private updateMatchedNetworks(List<SyncTask.UpdateItem<Network, Map>> updateList, NetworkType networkType) {
        log.debug("IsolationNetworkSync:updateMatchedNetworks: Entered")
        log.info("RAZI :: IsolationNetworkSync >> updateMatchedNetworks >> called")
        List<Network> itemsToUpdate = []
        try {
            for (update in updateList) {
                Network network = update.existingItem
                def masterItem = update.masterItem
                log.debug "processing update: ${network}"
                log.info("RAZI :: updateMatchedNetworks >> network: ${network}")
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
                    log.info("RAZI :: updateMatchedNetworks >> save: ${save}")
                    if (save) {
                        itemsToUpdate << network
                    }
                }
            }
            log.info("RAZI :: itemsToUpdate.size(): ${itemsToUpdate.size()}")
            if (itemsToUpdate.size() > 0) {
                morpheusContext.async.cloud.network.save(itemsToUpdate).blockingGet()
            }
        } catch(e) {
            log.error "Error in update Isolation Network sync ${e}", e
        }
    }
}
