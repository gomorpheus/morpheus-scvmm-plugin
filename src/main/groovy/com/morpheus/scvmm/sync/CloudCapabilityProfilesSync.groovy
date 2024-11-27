package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkType
import com.morpheusdata.model.projection.CloudIdentityProjection
import com.morpheusdata.model.projection.NetworkIdentityProjection
import groovy.util.logging.Slf4j

@Slf4j
class CloudCapabilityProfilesSync {

    private MorpheusContext morpheusContext
    private Cloud cloud
    private ScvmmApiService apiService

    CloudCapabilityProfilesSync(MorpheusContext morpheusContext, Cloud cloud, ScvmmApiService apiService) {
        this.cloud = cloud
        this.morpheusContext = morpheusContext
        this.apiService = apiService
    }

    def execute() {
        log.debug "CloudCapabilityProfilesSync"
        try {
            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('zone.id', cloud.id))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)

            def existingItems = morpheusContext.async.cloud.listIdentityProjections(new DataQuery())

            if(cloud.regionCode) {
                def cloudResults = apiService.getCloud(scvmmOpts)
                if(cloudResults.success == true && cloudResults?.cloud?.CapabilityProfiles) {
//                    cloud.setConfigProperty('capabilityProfiles', cloudResults?.cloud.CapabilityProfiles)
//                    opts.zone.save(flush:true)
                    def objList = cloudResults?.cloud?.CapabilityProfiles
                    SyncTask<CloudIdentityProjection, Map, Cloud> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)
                    syncTask.onAdd {addList ->
                        cloud.setConfigProperty('capabilityProfiles', addList)
                        morpheusContext.async.cloud.save(cloud)
                    }
                }
            } else {
                def capabilityProfileResults = apiService.getCapabilityProfiles(scvmmOpts)
                if(capabilityProfileResults.success == true && capabilityProfileResults?.capabilityProfiles) {
//                    cloud.setConfigProperty('capabilityProfiles', capabilityProfileResults.capabilityProfiles.collect { it.Name })
//                    opts.zone.save(flush:true)
                    def objList = capabilityProfileResults?.capabilityProfiles
                    SyncTask<CloudIdentityProjection, Map, Cloud> syncTask = new SyncTask<>(existingItems, objList as Collection<Map>)
                    syncTask.onAdd {addList ->
                        cloud.setConfigProperty('capabilityProfiles', addList)
                        morpheusContext.async.cloud.save(cloud)
                    }
                }
            }

        } catch (e) {
            log.error("CloudCapabilityProfilesSync error: ${e}", e)
        }
    }
}
