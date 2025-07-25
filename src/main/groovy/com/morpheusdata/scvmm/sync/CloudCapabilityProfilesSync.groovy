package com.morpheusdata.scvmm.sync

import com.morpheusdata.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.model.Cloud
import com.morpheusdata.scvmm.logging.LogInterface
import com.morpheusdata.scvmm.logging.LogWrapper
import groovy.util.logging.Slf4j

class CloudCapabilityProfilesSync {

    private MorpheusContext morpheusContext
    private Cloud cloud
    private ScvmmApiService apiService
    private LogInterface log = LogWrapper.instance

    CloudCapabilityProfilesSync(MorpheusContext morpheusContext, Cloud cloud) {
        this.cloud = cloud
        this.morpheusContext = morpheusContext
        this.apiService = new ScvmmApiService(morpheusContext)
    }

    def execute() {
        log.debug "CloudCapabilityProfilesSync"
        try {
            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('cloud.id', cloud.id))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)

            if(cloud.regionCode) {
                def cloudResults = apiService.getCloud(scvmmOpts)
                if(cloudResults.success == true && cloudResults?.cloud?.CapabilityProfiles) {
                    cloud.setConfigProperty('capabilityProfiles', cloudResults?.cloud.CapabilityProfiles)
                    morpheusContext.services.cloud.save(cloud)
                }
            } else {
                def capabilityProfileResults = apiService.getCapabilityProfiles(scvmmOpts)
                if(capabilityProfileResults.success == true && capabilityProfileResults?.capabilityProfiles) {
                    cloud.setConfigProperty('capabilityProfiles', capabilityProfileResults.capabilityProfiles.collect { it.Name })
                    morpheusContext.services.cloud.save(cloud)
                }
            }
        } catch (e) {
            log.error("CloudCapabilityProfilesSync error: ${e}", e)
        }
    }
}
