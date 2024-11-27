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

            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('zone.id', cloud.id))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)

            def listResults = apiService.listNetworkIPPools(scvmmOpts)

            if (listResults.success == true) {
                def poolType = new NetworkPoolType(code: 'scvmm')
                def objList = listResults.ipPools
                def networkMapping = listResults.networkMapping

                def existingItems = morpheusContext.async.cloud.network.pool.listIdentityProjections(new DataQuery()
                        .withFilter('account.id', cloud.account.id)
                        .withFilter('category', "scvmm.ipPool.${cloud.id}"))

                SyncTask<NetworkPoolIdentityProjection, Map, NetworkPool> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)
                syncTask.addMatchFunction { NetworkPool networkPool, poolItem ->
                    networkPool?.externalId == poolItem?.ID
                }.onDelete { removeItems ->
                    morpheusContext.async.cloud.network.pool.remove(removeItems).blockingGet()
                }.onUpdate { List<SyncTask.UpdateItem<NetworkPool, Map>> updateItems ->
                    updateMatchedIpPools(updateItems, networks, networkMapping)
                }.onAdd { itemsToAdd ->
                    addMissingIpPools(itemsToAdd, networks, poolType, networkMapping)
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

        try {
            addList?.each { it ->

                def info = new SubnetUtils(it.Subnet)?.getInfo()
                def netmask = info.netmask
                def subnetAddress = info.networkAddress
                def gateway = it.DefaultGateways ? it.DefaultGateways.first() : null
                def addConfig = [
                        account      : cloud.account,
                        code         : "scvmm.ipPool.${cloud.id}.${it.ID}",
                        category     : "scvmm.ipPool.${cloud.id}",
                        name         : it.Name,
                        displayName  : "${it.Name} (${it.Subnet})",
                        externalId   : it.ID,
                        ipCount      : it.TotalAddresses ?: 0,
                        ipFreeCount  : it.AvailableAddresses ?: 0,
                        dnsSuffixList: it.DNSSearchSuffixes,
                        dhcpServer   : true,
                        dnsServers   : it.DNSServers,
                        gateway      : gateway,
                        poolEnabled  : true,
                        netmask      : netmask,
                        subnetAddress: subnetAddress,
                        type         : poolType,
                        refType      : 'ComputeZone',
                        refId        : "${cloud.id}"
                ]
                def add = new NetworkPool(addConfig)
                morpheusContext.async.cloud.network.pool.create(add).blockingGet()
                morpheusContext.async.cloud.network.pool.save(add).blockingGet()

                if(it.IPAddressRangeStart && it.IPAddressRangeEnd) {
                    def newRange = new NetworkPoolRange(networkPool: add, startAddress: it.IPAddressRangeStart, endAddress: it.IPAddressRangeEnd, addressCount: (it.TotalAddresses ?: 0).toInteger(), externalId: it.ID)
                    morpheusContext.async.cloud.network.pool.poolRange.create(newRange).blockingGet()
                    morpheusContext.async.cloud.network.pool.poolRange.save(newRange).blockingGet()
                    log.debug("scvmm new range externalId: ${newRange.externalId}")
                    add.addToIpRanges(newRange)
                }
                morpheusContext.async.cloud.network.pool.save(add).blockingGet()

                def resourcePerm = new ResourcePermission(morpheusResourceType:'NetworkPool', morpheusResourceId:add.id, account:cloud.account)
                morpheusContext.async.resourcePermission.create(resourcePerm).blockingGet()

                updateNetworkForPool(networks, add, it.NetworkID, it.SubnetID, networkMapping)
            }
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

        try {
            updateList?.each { updateMap ->
                NetworkPool existingItem = updateMap.existingItem
                def masterItem = updateMap.masterItem
                if (existingItem) {
                    // Update the range (if needed)
                    if(masterItem.IPAddressRangeStart && masterItem.IPAddressRangeEnd) {
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

                    if (doSave == true) {
//                        existingItem.save(flush: true)
                        morpheusContext.async.cloud.network.pool.save(existingItem).blockingGet()
                    }

//                    def existingPermission = ResourcePermission.where { morpheusResourceType == 'NetworkPool' && morpheusResourceId == existingItem.id && account == opts.zone.account }.get()
                    def existingPermission = morpheusContext.services.resourcePermission.find(new DataQuery()
                            .withFilter('morpheusResourceType', 'NetworkPool')
                            .withFilter('morpheusResourceId', existingItem.id)
                            .withFilter('account', cloud.account))
                    if(!existingPermission) {
                        def resourcePerm = new ResourcePermission(morpheusResourceType:'NetworkPool', morpheusResourceId:existingItem.id, account:cloud.account)
//                        resourcePerm.save(flush:true)
                        morpheusContext.async.resourcePermission.save(resourcePerm).blockingGet()
                    }

                    updateNetworkForPool(networks, existingItem, masterItem.NetworkID, masterItem.SubnetID, networkMapping)
                }
            }
        } catch (e) {
            log.error("Error in updateMatchedIpPools: ${e}", e)
        }
    }
}
