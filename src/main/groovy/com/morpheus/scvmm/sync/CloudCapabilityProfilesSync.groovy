package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.model.Cloud
import groovy.util.logging.Slf4j

@Slf4j
class CloudCapabilityProfilesSync {

    private MorpheusContext morpheusContext
    private Cloud cloud
    private ScvmmApiService apiService

    CloudCapabilityProfilesSync(MorpheusContext morpheusContext, Cloud cloud) {
        this.cloud = cloud
        this.morpheusContext = morpheusContext
        this.apiService = new ScvmmApiService(morpheusContext)
    }

    def execute() {
        log.debug "CloudCapabilityProfilesSync"
        try {
            def server = morpheusContext.services.computeServer.find(new DataQuery().withFilter('zone.id', cloud.id))
            def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, server)
            log.info("RAZI :: scvmmOpts: ${scvmmOpts}")

            log.info("RAZI :: cloud.regionCode: ${cloud.regionCode}")
            if(cloud.regionCode) {
                def cloudResults = apiService.getCloud(scvmmOpts)
                log.info("RAZI :: cloudResults: ${cloudResults}")
                log.info("RAZI :: cloudResults.success: ${cloudResults.success}")
                log.info("RAZI :: cloudResults?.cloud?.CapabilityProfiles: ${cloudResults?.cloud?.CapabilityProfiles}")
                if(cloudResults.success == true && cloudResults?.cloud?.CapabilityProfiles) {
                    cloud.setConfigProperty('capabilityProfiles', cloudResults?.cloud.CapabilityProfiles)
                    morpheusContext.services.cloud.save(cloud)
                    log.info("RAZI :: if(cloud.regionCode) >> cloud save SUCCESS")
                }
            } else {
                def capabilityProfileResults = apiService.getCapabilityProfiles(scvmmOpts)
                log.info("RAZI :: capabilityProfileResults: ${capabilityProfileResults}")
                log.info("RAZI :: capabilityProfileResults.success: ${capabilityProfileResults.success}")
                log.info("RAZI :: capabilityProfileResults?.capabilityProfiles: ${capabilityProfileResults?.capabilityProfiles}")
                if(capabilityProfileResults.success == true && capabilityProfileResults?.capabilityProfiles) {
                    cloud.setConfigProperty('capabilityProfiles', capabilityProfileResults.capabilityProfiles.collect { it.Name })
                    morpheusContext.services.cloud.save(cloud)
                    log.info("RAZI :: if(cloud.regionCode) >> else >> cloud save SUCCESS")
                }
            }
        } catch (e) {
            log.error("CloudCapabilityProfilesSync error: ${e}", e)
        }
    }
}
