package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.ResourcePermission
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import groovy.util.logging.Slf4j
import org.apache.commons.net.util.SubnetUtils

@Slf4j
class IpPoolsSync {

    private MorpheusContext morpheusContext
    private Cloud cloud
    private ScvmmApiService apiService

    IpPoolsSync(MorpheusContext morpheusContext, Cloud cloud) {
        this.cloud = cloud
        this.morpheusContext = morpheusContext
        this.apiService = new ScvmmApiService(morpheusContext)
    }

    def execute() {
        log.debug "IpPoolsSync"
        log.info "IpPoolsSync >> execute() called"
        try {
            /*def networks = Network.withCriteria {
                or {
                    eq('owner', opts.zone.account)
                    eq('owner', opts.zone.owner)
                }
                or {
                    like('category', "scvmm.network.${opts.zone.id}.%")
                    like('category', "scvmm.vlan.network.${opts.zone.id}.%")
                }
            }*/
            def networks = morpheusContext.services.cloud.network.list(new DataQuery()
                    .withFilters(
                            new DataOrFilter(
                                    new DataFilter('owner', cloud.account),
                                    new DataFilter('owner', cloud.owner)
                            ),
                            new DataOrFilter(
                                    new DataFilter('category', '=~', "scvmm.network.${cloud.id}.%"),
                                    new DataFilter('category', '=~', "scvmm.vlan.network.${cloud.id}.%")
                            )
                    ))
            log.info("RAZI :: networks: ${networks}")

            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('zone.id', cloud.id))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)
            log.info("RAZI :: scvmmOpts: ${scvmmOpts}")

            def listResults = apiService.listNetworkIPPools(scvmmOpts)
            log.info("RAZI :: listResults: ${listResults}")

            if (listResults.success == true) {
                def poolType = new NetworkPoolType(code: 'scvmm')
                def objList = listResults.ipPools
                log.info("RAZI :: objList: ${objList}")
                def networkMapping = listResults.networkMapping
                log.info("RAZI :: networkMapping: ${networkMapping}")

                def existingItems = morpheusContext.async.cloud.network.pool.listIdentityProjections(new DataQuery()
                        .withFilter('account.id', cloud.account.id)
                        .withFilter('category', "scvmm.ipPool.${cloud.id}"))
                log.info("RAZI :: existingItems: ${existingItems}")

                SyncTask<NetworkPoolIdentityProjection, Map, NetworkPool> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)
                syncTask.addMatchFunction { NetworkPool networkPool, poolItem ->
                    log.info("RAZI :: networkPool?.externalId: ${networkPool?.externalId}")
                    log.info("RAZI :: poolItem?.ID: ${poolItem?.ID}")
                    networkPool?.externalId == poolItem?.ID
                }.onDelete { removeItems ->
                    log.info("RAZI :: onDelete call START")
                    morpheusContext.async.cloud.network.pool.remove(removeItems).blockingGet()
                    log.info("RAZI :: onDelete call STOP")
                }.onUpdate { List<SyncTask.UpdateItem<NetworkPool, Map>> updateItems ->
                    log.info("RAZI :: updateMatchedIpPools call START")
                    updateMatchedIpPools(updateItems, networks, networkMapping)
                    log.info("RAZI :: updateMatchedIpPools call STOP")
                }.onAdd { itemsToAdd ->
                    log.info("RAZI :: addMissingIpPools call START")
                    addMissingIpPools(itemsToAdd, networks, poolType, networkMapping)
                    log.info("RAZI :: addMissingIpPools call STOP")
                }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map>> updateItems ->
                    return morpheusContext.async.cloud.network.pool.listById(updateItems.collect { it.existingItem.id } as List<Long>)
                }.start()
            }
        } catch (e) {
            log.error("ipPoolsSync error: ${e}", e)
        }
    }

    private addMissingIpPools(Collection<Map> addList, List<Network> networks, NetworkPoolType poolType, networkMapping) {
        log.debug("addMissingIpPools: ${addList.size()}")
        log.info("RAZI :: addList: ${addList}")

        def networkPoolAdds = []
        try {
            addList?.each { it ->
                log.info("RAZI :: it.Subnet: ${it.Subnet}")
                def info = new SubnetUtils(it.Subnet)?.getInfo()
                log.info("RAZI :: info: ${info}")
                def netmask = info.netmask
                log.info("RAZI :: netmask: ${netmask}")
                def subnetAddress = info.networkAddress
                log.info("RAZI :: subnetAddress: ${subnetAddress}")
                log.info("RAZI :: it.DefaultGateways: ${it.DefaultGateways}")
                def gateway = it.DefaultGateways ? it.DefaultGateways.first() : null
                log.info("RAZI :: gateway: ${gateway}")
                def addConfig = [
                        account      : cloud.account,
//                        code         : "scvmm.ipPool.${cloud.id}.${it.ID}",
                        typeCode     : "scvmm.ipPool.${cloud.id}.${it.ID}",
                        category     : "scvmm.ipPool.${cloud.id}",
                        name         : it.Name,
                        displayName  : "${it.Name} (${it.Subnet})",
                        externalId   : it.ID,
                        ipCount      : it.TotalAddresses ?: 0,
                        ipFreeCount  : it.AvailableAddresses ?: 0,
//                        dnsSuffixList: it.DNSSearchSuffixes,
                        dhcpServer   : true,
//                        dnsServers   : it.DNSServers,
                        gateway      : gateway,
                        poolEnabled  : true,
                        netmask      : netmask,
                        subnetAddress: subnetAddress,
                        type         : poolType,
                        refType      : 'ComputeZone',
                        refId        : "${cloud.id}"
                ]
                log.info("RAZI :: addConfig: ${addConfig}")
                NetworkPool add = new NetworkPool(addConfig)
//                networkPoolAdds << networkPoolAdd
                morpheusContext.async.network.pool.create(add).blockingGet()
                morpheusContext.async.network.pool.save(add).blockingGet()

                log.info("RAZI :: it.IPAddressRangeStart: ${it.IPAddressRangeStart}")
                log.info("RAZI :: it.IPAddressRangeEnd: ${it.IPAddressRangeEnd}")
                if(it.IPAddressRangeStart && it.IPAddressRangeEnd) {
                    def rangeConfig = [
                            networkPool: add,
                            startAddress: it.IPAddressRangeStart,
                            endAddress: it.IPAddressRangeEnd,
                            addressCount: (it.TotalAddresses ?: 0).toInteger(),
//                            reservationCount: (it.AvailableAddresses ?: 0).toInteger(),
                            reservationCount: 5L,
                            externalId: it.ID
                    ]
//                    def newRange = new NetworkPoolRange(networkPool: add, startAddress: it.IPAddressRangeStart, endAddress: it.IPAddressRangeEnd, addressCount: (it.TotalAddresses ?: 0).toInteger(), reservationCount: (it.AvailableAddresses ?: 0).toInteger(), externalId: it.ID)
                    def newRange = new NetworkPoolRange(rangeConfig)
//                    newRange.reservationCount = 10
                    log.info("RAZI :: newRange1: ${newRange}")
                    morpheusContext.async.cloud.network.pool.poolRange.create(newRange).blockingGet()
                    log.info("RAZI :: newRange.reservationCount: ${newRange.reservationCount}")
                    log.info("RAZI :: newRange2: ${newRange}")
                    morpheusContext.async.cloud.network.pool.poolRange.save(newRange).blockingGet()
                    log.debug("scvmm new range id: ${newRange.id}")
                    log.info("RAZI :: scvmm new range id: ${newRange.id}")
                    add.addToIpRanges(newRange)
                }
                morpheusContext.async.cloud.network.pool.save(add).blockingGet()

                log.info("RAZI :: add.id: ${add.id}")
                def resourcePerm = new ResourcePermission(morpheusResourceType:'NetworkPool', morpheusResourceId:add.id, account:cloud.account)
                log.info("RAZI :: resourcePerm: ${resourcePerm}")
                morpheusContext.async.resourcePermission.create(resourcePerm).blockingGet()

                updateNetworkForPool(networks, add, it.NetworkID, it.SubnetID, networkMapping)
                log.info("RAZI :: addMissingIpPools >> updateNetworkForPool call END")
            }
//            log.info("RAZI :: networkPoolAdds.size(): ${networkPoolAdds.size()}")
//            if (networkPoolAdds.size() > 0){
//                    log.info("RAZI :: NetworkPools creat START")
//                    morpheusContext.async.cloud.network.pool.bulkCreate(networkPoolAdds).blockingGet()
//                    log.info("RAZI :: NetworkPools creat STOP")
//            }
        } catch (e) {
            log.error("Error in addMissingIpPools: ${e}", e)
        }
    }

    def updateNetworkForPool(List<Network> networks, NetworkPool pool, networkId, subnetId, networkMapping) {
        log.debug "updateNetworkForPool: ${networks} ${pool} ${networkId} ${subnetId} ${networkMapping}"
        try {
            // Find the matching network for the pool
            def networkExternalId = networkMapping?.find { it.ID == networkId }?.ID
            Network network = networks?.find { it.externalId == networkExternalId }

            log.info("RAZI :: updateNetworkForPool >> network: ${network}")
            log.info("RAZI :: updateNetworkForPool >> subnetId: ${subnetId}")
            if(network) {
                def doSave = false

                if(network.pool != pool) {
                    network.pool = pool
                    doSave = true
                }
                if(pool.gateway && network.gateway != pool.gateway) {
                    network.gateway = pool.gateway
                    doSave = true
                }
                if(pool.dnsServers?.size() && pool.dnsServers[0] && network.dnsPrimary != pool.dnsServers[0]) {
                    network.dnsPrimary = pool.dnsServers[0] ?: null
                    doSave = true
                }
                if(pool.dnsServers?.size() > 1 && pool.dnsServers[1] && network.dnsSecondary != pool.dnsServers[1]) {
                    network.dnsSecondary = pool.dnsServers[1] ?: null
                    doSave = true
                }
                if(pool.netmask && network.netmask != pool.netmask) {
                    network.netmask = pool.netmask
                    doSave = true
                }
                if(network.allowStaticOverride != true) {
                    network.allowStaticOverride = true
                    doSave = true
                }
                log.info("RAZI :: if(network) >> doSave: ${doSave}")
                if(doSave) {
    //                network.save(flush: true)
                    morpheusContext.async.cloud.network.save(network).blockingGet()
                }
            }

            if(subnetId && network) {
                def subnet = network.subnets.find { it ->
                    it.externalId?.startsWith(subnetId)
                }
                if(subnet) {
                    def doSave = false

                    if(subnet.pool != pool) {
                        subnet.pool = pool
                        doSave = true
                    }
                    if(pool.gateway && subnet.gateway != pool.gateway) {
                        subnet.gateway = pool.gateway
                        doSave = true
                    }
                    if(pool.dnsServers?.size() && pool.dnsServers[0] && subnet.dnsPrimary != pool.dnsServers[0]) {
                        subnet.dnsPrimary = pool.dnsServers[0] ?: null
                        doSave = true
                    }
                    if(pool.dnsServers?.size() > 1 && pool.dnsServers[1] && subnet.dnsSecondary != pool.dnsServers[1]) {
                        subnet.dnsSecondary = pool.dnsServers[1] ?: null
                        doSave = true
                    }
                    if(pool.netmask && subnet.netmask != pool.netmask) {
                        subnet.netmask = pool.netmask
                        doSave = true
                    }
                    if(subnet.allowStaticOverride != true) {
                        subnet.allowStaticOverride = true
                        doSave = true
                    }
                    log.info("RAZI :: if(subnetId && network) >> doSave: ${doSave}")
                    if(doSave) {
    //                    subnet.save(flush: true)
                        morpheusContext.async.networkSubnet.save(subnet).blockingGet()
                    }
                }
            }
        } catch (e) {
            log.error("Error in updateNetworkForPool: ${e}", e)
        }
    }

    private updateMatchedIpPools(List<SyncTask.UpdateItem<NetworkPool, Map>> updateList, networks, networkMapping) {
        log.debug("updateMatchedIpPools : ${updateList.size()}")
        log.info("RAZI :: updateList: ${updateList}")

        try {
            updateList?.each { updateMap ->
                NetworkPool existingItem = updateMap.existingItem
                def masterItem = updateMap.masterItem
                if (existingItem) {
                    // Update the range (if needed)
                    log.info("RAZI :: masterItem.IPAddressRangeStart: ${masterItem.IPAddressRangeStart}")
                    log.info("RAZI :: masterItem.IPAddressRangeEnd: ${masterItem.IPAddressRangeEnd}")
                    if(masterItem.IPAddressRangeStart && masterItem.IPAddressRangeEnd) {
                        log.info("RAZI :: existingItem.ipRanges: ${existingItem.ipRanges}")
                        if(!existingItem.ipRanges) {
                            def range = new NetworkPoolRange(networkPool: existingItem, startAddress: masterItem.IPAddressRangeStart, endAddress: masterItem.IPAddressRangeEnd, addressCount: (masterItem.TotalAddresses ?: 0).toInteger(), externalId: masterItem.ID)
//                            range.save()
                            morpheusContext.async.cloud.network.pool.poolRange.save(range).blockingGet()
                            existingItem.addToIpRanges(range)
//                            existingItem.save(flush: true)
                            morpheusContext.async.cloud.network.pool.save(existingItem).blockingGet()
                        } else {
                            NetworkPoolRange range = existingItem.ipRanges.first()
                            if(range.startAddress != masterItem.IPAddressRangeStart ||
                                    range.endAddress != masterItem.IPAddressRangeEnd ||
                                    range.addressCount != (masterItem.TotalAddresses ?: 0).toInteger() ||
                                    range.externalId != masterItem.ID) {
                                range.startAddress = masterItem.IPAddressRangeStart
                                range.endAddress = masterItem.IPAddressRangeEnd
                                range.addressCount = (masterItem.TotalAddresses ?: 0).toInteger()
                                range.externalId = masterItem.ID
//                                range.save(flush: true)
                                morpheusContext.async.cloud.network.pool.poolRange.save(range).blockingGet()
                            }
                        }
                    }

                    // Update the pool (if needed)
                    def doSave = false
                    if(existingItem.name != masterItem.Name) {
                        existingItem.name = masterItem.Name
                        doSave = true
                    }

                    def displayName = "${masterItem.Name} (${masterItem.Subnet})"
                    if(existingItem.displayName != displayName) {
                        existingItem.displayName = displayName
                        doSave = true
                    }

                    if(existingItem.ipCount != (masterItem.TotalAddresses ?: 0).toInteger()) {
                        existingItem.ipCount = (masterItem.TotalAddresses ?: 0).toInteger()
                        doSave = true
                    }

                    if(existingItem.ipFreeCount != (masterItem.AvailableAddresses ?: 0).toInteger()) {
                        existingItem.ipFreeCount = (masterItem.AvailableAddresses ?: 0).toInteger()
                        doSave = true
                    }

                    if(existingItem.dnsSuffixList != masterItem.DNSSearchSuffixes) {
                        existingItem.dnsSuffixList = masterItem.DNSSearchSuffixes
                        doSave = true
                    }

                    if(existingItem.dnsServers != masterItem.DNSServers) {
                        existingItem.dnsServers = masterItem.DNSServers
                        doSave = true
                    }

                    def gateway = masterItem.DefaultGateways ? masterItem.DefaultGateways.first() : null
                    if(existingItem.gateway != gateway) {
                        existingItem.gateway = gateway
                        doSave = true
                    }

                    def info = new SubnetUtils(masterItem.Subnet).getInfo()
                    if(existingItem.netmask != info.netmask) {
                        existingItem.netmask = info.netmask
                        doSave = true
                    }

                    if(existingItem.subnetAddress != info.networkAddress) {
                        existingItem.subnetAddress = info.networkAddress
                        doSave = true
                    }

                    log.info("RAZI :: updateMatchedIpPools >> doSave: ${doSave}")
                    if (doSave == true) {
//                        existingItem.save(flush: true)
                        morpheusContext.async.cloud.network.pool.save(existingItem).blockingGet()
                    }

//                    def existingPermission = ResourcePermission.where { morpheusResourceType == 'NetworkPool' && morpheusResourceId == existingItem.id && account == opts.zone.account }.get()
                    def existingPermission = morpheusContext.services.resourcePermission.find(new DataQuery()
                            .withFilter('morpheusResourceType', 'NetworkPool')
                            .withFilter('morpheusResourceId', existingItem.id)
                            .withFilter('account', cloud.account))
                    log.info("RAZI :: updateMatchedIpPools >> existingPermission: ${existingPermission}")
                    if(!existingPermission) {
                        def resourcePerm = new ResourcePermission(morpheusResourceType:'NetworkPool', morpheusResourceId:existingItem.id, account:cloud.account)
//                        resourcePerm.save(flush:true)
                        morpheusContext.async.resourcePermission.save(resourcePerm).blockingGet()
                    }

                    updateNetworkForPool(networks, existingItem, masterItem.NetworkID, masterItem.SubnetID, networkMapping)
                    log.info("RAZI :: updateMatchedIpPools >> updateNetworkForPool call END")
                }
            }
        } catch (e) {
            log.error("Error in updateMatchedIpPools: ${e}", e)
        }
    }
}
