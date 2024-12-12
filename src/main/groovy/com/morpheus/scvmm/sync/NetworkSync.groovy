package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.BulkCreateResult
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkSubnet
import com.morpheusdata.model.NetworkSubnetType
import com.morpheusdata.model.NetworkType
import com.morpheusdata.model.projection.NetworkIdentityProjection
import com.morpheusdata.model.projection.NetworkSubnetIdentityProjection
import groovy.util.logging.Slf4j

@Slf4j
class NetworkSync {

    private MorpheusContext morpheusContext
    private Cloud cloud
    private ScvmmApiService apiService

    NetworkSync(MorpheusContext morpheusContext, Cloud cloud) {
        this.cloud = cloud
        this.morpheusContext = morpheusContext
        this.apiService = new ScvmmApiService(morpheusContext)
    }

    def execute() {
        log.debug "NetworkSync"
        try {
            def networkType = new NetworkType(code: 'scvmmNetwork')
            NetworkSubnetType subnetType = new NetworkSubnetType(code: 'scvmm')

            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('cloud.id', cloud.id))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)
            def listResults = apiService.listNetworks(scvmmOpts)

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
                                new DataFilter('category', '=~', "scvmm.network.${cloud.id}.%")))

                SyncTask<NetworkIdentityProjection, Map, Network> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)
                syncTask.addMatchFunction { NetworkIdentityProjection network, Map networkItem ->
                    network?.externalId == networkItem?.ID
                }.onDelete { removeItems ->
                    morpheusContext.async.cloud.network.remove(removeItems).blockingGet()
                }.onUpdate { List<SyncTask.UpdateItem<Network, Map>> updateItems ->
                    updateMatchedNetworks(updateItems, subnetType)
                }.onAdd { itemsToAdd ->
                    addMissingNetworks(itemsToAdd, networkType, subnetType, server)
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

    private addMissingNetworks(Collection<Map> addList, NetworkType networkType, NetworkSubnetType subnetType, ComputeServer server) {
        log.debug("NetworkSync >> addMissingNetworks >> called")

        def networkAdds = []
        try {
            addList?.each { networkItem ->

                def networkConfig = [
                        code      : "scvmm.network.${cloud.id}.${server.id}.${networkItem.ID}",
                        category  : "scvmm.network.${cloud.id}.${server.id}",
                        cloud     : cloud,
                        dhcpServer: true,
                        uniqueId  : networkItem.ID,
                        name      : networkItem.Name,
                        externalId: networkItem.ID,
                        type      : networkType,
                        refType   : 'ComputeZone',
                        refId     : cloud.id,
                        owner     : cloud.owner,
                        active    : cloud.defaultNetworkSyncActive
                ]
                Network networkAdd = new Network(networkConfig)
                networkAdds << networkAdd
            }

            // Perform bulk create of networks
            if (networkAdds.size() > 0) {
                def result = morpheusContext.async.cloud.network.bulkCreate(networkAdds).blockingGet()

                // Now add subnets to the created networks
                result.persistedItems.each { networkAdd ->

                    def cloudItem = addList.find {it.Name == networkAdd.name} // Find corresponding cloud item
                    def subnet = cloudItem.Subnets?.get(0)?.Subnet
                    def networkCidr = NetworkUtility.getNetworkCidrConfig(subnet)

                    if (cloudItem) {
                        def subnetConfig = [
                                dhcpServer         : true,
                                account            : cloud.owner,
                                externalId         : cloudItem.ID,
                                networkSubnetType  : subnetType,
                                category           : "scvmm.subnet.${cloud.id}",
                                name               : cloudItem.Name,
                                vlanId             : cloudItem.VLanID,
                                cidr               : subnet,
                                netmask            : networkCidr.config?.netmask,
                                dhcpStart          : (networkCidr.ranges ? networkCidr.ranges[0].startAddress : null),
                                dhcpEnd            : (networkCidr.ranges ? networkCidr.ranges[0].endAddress : null),
                                subnetAddress      : subnet,
                                refType            : 'ComputeZone',
                                refId              : cloud.id
                        ]
                        def addSubnet = new NetworkSubnet(subnetConfig)
                        // Create subnet for the network
                        morpheusContext.async.networkSubnet.create([addSubnet], networkAdd).blockingGet()
                    }
                }
            }
        } catch (e) {
            log.error("Error in addMissingNetworks: ${e}", e)
        }
    }

    private updateMatchedNetworks(List<SyncTask.UpdateItem<Network, Map>> updateList, NetworkSubnetType subnetType) {
        log.debug("NetworkSync >> updateMatchedNetworks >> Entered")

        try {
            updateList?.each { updateMap ->

                Network network = updateMap.existingItem
                def matchedNetwork = updateMap.masterItem

                def existingSubnetIds = network.subnets.collect{it.id}
                def existingSubnets = morpheusContext.async.networkSubnet.list(new DataQuery()
                        .withFilter('id', 'in', existingSubnetIds))

                def masterSubnets = matchedNetwork.Subnets

                SyncTask<NetworkSubnetIdentityProjection, Map, NetworkSubnet> syncTask = new SyncTask<>(existingSubnets, masterSubnets as Collection<Map>)

                syncTask.addMatchFunction { NetworkSubnetIdentityProjection subnet, Map scvmmSubnet ->
                    subnet?.externalId == scvmmSubnet.ID
                }.onDelete { removeItems ->
                    morpheusContext.async.networkSubnet.remove(removeItems).blockingGet()
                }.onUpdate { List<SyncTask.UpdateItem<NetworkSubnet, Map>> updateItems ->
                    updateMatchedNetworkSubnet(updateItems)
                }.onAdd { itemsToAdd ->
                    addMissingNetworkSubnet(itemsToAdd, subnetType, network)
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<NetworkSubnetIdentityProjection, Map>> updateItems ->
                    return morpheusContext.async.networkSubnet.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            }
        } catch(e) {
            log.error("Error in updateMatchedNetworks: ${e}", e)
        }
    }

    private addMissingNetworkSubnet(Collection<Map> addList, NetworkSubnetType subnetType, Network network) {
        log.debug("addMissingNetworkSubnet: ${addList}")

        def subnetAdds = []
        try {
            addList?.each { scvmmSubnet ->
                log.debug("adding new SCVMM subnet: ${scvmmSubnet}")

                def networkCidr = NetworkUtility.getNetworkCidrConfig(scvmmSubnet.Subnet)
                def subnetInfo = [
                        dhcpServer       : true,
                        account          : cloud.owner,
                        externalId       : scvmmSubnet.ID,
                        networkSubnetType: subnetType,
                        category         : "scvmm.subnet.${cloud.id}",
                        name             : scvmmSubnet.Name,
                        cidr             : scvmmSubnet.Subnet,
                        netmask          : networkCidr.config?.netmask,
                        dhcpStart        : (networkCidr.ranges ? networkCidr.ranges[0].startAddress : null),
                        status           : NetworkSubnet.Status.AVAILABLE,
                        dhcpEnd          : (networkCidr.ranges ? networkCidr.ranges[0].endAddress : null),
                        subnetAddress    : scvmmSubnet.Subnet,
                        refType          : 'ComputeZone',
                        refId            : cloud.id
                ]

                NetworkSubnet subnetAdd = new NetworkSubnet(subnetInfo)
                subnetAdds << subnetAdd
            }

            //create networkSubnets
            morpheusContext.async.networkSubnet.create(subnetAdds, network).blockingGet()
        } catch (e) {
            log.error("Error in addMissingNetworkSubnet: ${e}", e)
        }
    }

    private updateMatchedNetworkSubnet(List<SyncTask.UpdateItem<NetworkSubnet, Map>> updateList){
        log.debug("updateMatchedNetworkSubnet: ${updateList}")

        List<NetworkSubnet> itemsToUpdate = []
        try{
            updateList?.each { subnetUpdateMap ->
                def matchedSubnet = subnetUpdateMap.masterItem
                NetworkSubnet subnet = subnetUpdateMap.existingItem
                log.debug("updating subnet: ${matchedSubnet}")

                def networkCidr = NetworkUtility.getNetworkCidrConfig(matchedSubnet.Subnet)

                if (subnet) {
                    def save = false
                    if (subnet.name != matchedSubnet.Name) {
                        subnet.name = matchedSubnet.Name
                        save = true
                    }
                    if (subnet.getConfigProperty('subnetName') != matchedSubnet.Name) {
                        subnet.setConfigProperty('subnetName', matchedSubnet.Name)
                        save = true
                    }

                    if (subnet.cidr != matchedSubnet.Subnet) {
                        subnet.cidr = matchedSubnet.Subnet
                        save = true
                    }
                    if (subnet.getConfigProperty('subnetCidr') != matchedSubnet.Subnet) {
                        subnet.setConfigProperty('subnetCidr', matchedSubnet.Subnet)
                        save = true
                    }
                    if (subnet.subnetAddress != matchedSubnet.Subnet) {
                        subnet.subnetAddress = matchedSubnet.Subnet
                        save = true
                    }
                    if (subnet.netmask != networkCidr.config?.netmask) {
                        subnet.netmask = networkCidr.config?.netmask
                        save = true
                    }

                    def dhcpStart = networkCidr.ranges ? networkCidr.ranges[0].startAddress : null
                    if (subnet.dhcpStart != dhcpStart) {
                        subnet.dhcpStart = dhcpStart
                        save = true
                    }

                    def dhcpEnd = networkCidr.ranges ? networkCidr.ranges[0].endAddress : null
                    if (subnet.dhcpEnd != dhcpEnd) {
                        subnet.dhcpEnd = dhcpEnd
                        save = true
                    }

                    if (subnet.vlanId != matchedSubnet.VLanID) {
                        subnet.vlanId = matchedSubnet.VLanID
                        save = true
                    }

                    if (save) {
                        itemsToUpdate << subnet
                    }
                }
            }

            if (itemsToUpdate.size() > 0) {
                morpheusContext.async.networkSubnet.save(itemsToUpdate).blockingGet()
            }
        } catch(e) {
            log.error "Error in updateMatchedNetworkSubnet ${e}", e
        }

    }
}