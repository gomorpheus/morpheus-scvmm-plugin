package com.morpheusdata.scvmm

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.OptionSourceProvider
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.MorpheusUtils
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import groovy.util.logging.Slf4j

@Slf4j
class ScvmmOptionSourceProvider implements OptionSourceProvider {

	ScvmmPlugin plugin
	MorpheusContext morpheusContext
	private ScvmmApiService apiService

	ScvmmOptionSourceProvider(ScvmmPlugin plugin, MorpheusContext context) {
		this.plugin = plugin
		this.morpheusContext = context
		this.apiService = new ScvmmApiService(context)
	}

	@Override
	MorpheusContext getMorpheus() {
		return this.morpheusContext
	}

	@Override
	Plugin getPlugin() {
		return this.plugin
	}

	@Override
	String getCode() {
		return 'scvmm-option-source'
	}

	@Override
	String getName() {
		return 'SCVMM Option Source'
	}

	@Override
	List<String> getMethodNames() {
		return new ArrayList<String>([
				'scvmmCloud', 'scvmmHostGroup', 'scvmmCluster', 'scvmmLibraryShares', 'scvmmSharedControllers'])
	}

	def getApiConfig(cloud) {
		def rtn = apiService.getScvmmZoneOpts(morpheusContext, cloud) + apiService.getScvmmInitializationOpts(cloud)
		return rtn
	}

	def setupCloudConfig(params) {
		params = params instanceof Object[] ? params.getAt(0) : params

		def config = [
				host: params.config?.host ?: params["config[host]"],
				username: params.config?.username ?: params["config[username]"],
				hostGroup: params.config?.hostGroup ?: params["config[hostGroup]"]
		]
		def password = params.config?.password ?: params["config[password]"]


		Cloud cloud
		// get the correct credentials
		if (params.zoneId) {
			// load the cloud
			cloud = morpheusContext.services.cloud.get(params.zoneId.toLong())
			if(params.credential.type != "local") {
				// not local creds, load from cloud or form
				if (params.credential && params.credential?.type != "local") {
					// might have new credential, load from form data
					def credentials = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
					cloud.accountCredentialLoaded = true
					cloud.accountCredentialData = credentials?.data
				} else {
					// no form data, load credentials from cloud
					def credentials = morpheusContext.services.accountCredential.loadCredentials(cloud)
					cloud.accountCredentialData = credentials.data
					cloud.accountCredentialLoaded = true
				}
			} else if(password != '*' * 12) {
				// new password, update it
				config.password = password
			}
		} else {
			cloud = new Cloud()
			if (params.credential && params.credential?.type != "local") {
				def credentials = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
				cloud.accountCredentialLoaded = true
				cloud.accountCredentialData = credentials?.data
				log.debug("cloud.accountCredentialData: ${cloud.accountCredentialData}")
			} else {
				// local credential, set the local cred config
				config.password = password
			}
		}

		// set the config map
		cloud.setConfigMap(config)

		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))

		cloud.apiProxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null

		return cloud
	}

	def scvmmCloud(params) {
		def cloud = setupCloudConfig(params)
		def apiConfig = getApiConfig(cloud)
		def results = []
		if(apiConfig.sshUsername && apiConfig.sshPassword) {
			results = apiService.listClouds(apiConfig)
		}
		log.debug("listClouds: ${results}")
		return results?.size() > 0 ? results.clouds?.collect { [name: it.Name, value: it.ID] } : [[name:"No Clouds Found: verify credentials above", value:""]]
	}

	def scvmmHostGroup(params) {
		log.debug("scvmmHostGroup: ${params}")
		def cloud = setupCloudConfig(params)
		def apiConfig = getApiConfig(cloud)
		def results = []
		if(apiConfig.sshUsername && apiConfig.sshPassword) {
			results = apiService.listHostGroups(apiConfig)
		}
		log.debug("listHostGroups: ${results}")
		return results.hostGroups?.size() > 0 ? results.hostGroups?.collect { [name: it.path, value: it.path] } :  [[name:"No Host Groups found", value:""]]
	}

	def scvmmCluster(params) {
		log.debug("scvmmCluster: ${params}")
		def cloud = setupCloudConfig(params)
		def apiConfig = getApiConfig(cloud)
		def results = []
		if(apiConfig.sshUsername && apiConfig.sshPassword) {
			results = apiService.listClusters(apiConfig)
		}
		log.debug("listClusters: ${results}")
		return results.clusters.size() > 0 ? results.clusters?.collect { [name: it.name, value: it.id] } : [[name:"No Clusters found: check your config", value:""]]
	}

	def scvmmLibraryShares(params) {
		log.debug("scvmmLibraryShares: ${params}")
		def cloud = setupCloudConfig(params)
		def apiConfig = getApiConfig(cloud)
		def results = []
		if(apiConfig.sshUsername && apiConfig.sshPassword) {
			results = apiService.listLibraryShares(apiConfig)
		}
		log.debug("listLibraryShares: ${results}")
		return results.libraryShares.size() > 0 ? results.libraryShares?.collect { [name: it.Path, value: it.Path] } : [[name:"No Library Shares found", value:""]]
	}

	def scvmmSharedControllers(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug("scvmmSharedControllers: ${params}")

		def orFilters = []
		if(params.zoneId) {
			orFilters << new DataFilter('zone.id', params.zoneId)
		}
		if(params.config?.host) {
			orFilters << new DataFilter('sshHost', params.config.host)
		}

		def sharedControllers = morpheusContext.services.computeServer.find(new DataQuery().withFilters(
			new DataFilter('enabled', true),
			new DataFilter('computeServerType.code', 'scvmmController'),
			new DataOrFilter(orFilters)
		))

		return sharedControllers?.collect{[name: it.name, value: it.id]}
	}

	def scvmmCapabilityProfile(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		def tmpZone = params.zoneId ? morpheusContext.services.cloud.get(params.zoneId?.toLong()) : null

		def capabilityProfiles = tmpZone?.getConfigProperty('capabilityProfiles')

		def profiles = []
		if(capabilityProfiles) {
			capabilityProfiles.each{ it ->
				profiles << [name: it, value: it]
			}
		} else {
			profiles << [name:'Not required', value: -1]
		}
		return profiles
	}

}
