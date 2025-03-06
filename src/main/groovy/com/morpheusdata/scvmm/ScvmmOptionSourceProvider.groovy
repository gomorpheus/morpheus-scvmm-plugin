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
		return apiService.getScvmmZoneOpts(morpheusContext, cloud) + apiService.getScvmmInitializationOpts(cloud)
	}

	def setupCloudConfig(params) {
		params = params instanceof Object[] ? params.getAt(0) : params

		def config = [
				host: params.config?.host ?: params["config[host]"],
				username: params.config?.username ?: params["config[username]"],
				hostGroup: params.config?.hostGroup ?: params["config[hostGroup]"]
		]

		def password = params.config?.password ?: params["config[password]"]
		if (password == '*' * 12 && params.zoneId) {
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
		} else {
			config.password = password
		}

		def cloud
		if (params.zoneId) {
			cloud = morpheusContext.services.cloud.get(params.zoneId.toLong())
			def credentials = morpheusContext.services.accountCredential.loadCredentials(cloud)
			cloud.accountCredentialData = credentials.data
			cloud.accountCredentialLoaded = true

			config.username = credentials.data?.username
			config.password = credentials.data?.password
		} else {
			cloud = new Cloud()
			cloud.setConfigMap(config)
		}

		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))

		if (params.credential) {
			def accountCredential = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
			cloud.accountCredentialLoaded = true
			cloud.accountCredentialData = accountCredential?.data
		}

		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy

		return cloud
	}

	def scvmmCloud(params) {
		log.debug("scvmmCloud: ${params}")
		def cloud = setupCloudConfig(params)
		def results = apiService.listClouds(getApiConfig(cloud))
		log.debug("listClouds: ${results}")
		return results.clouds?.collect { [name: it.Name, value: it.ID] }
	}

	def scvmmHostGroup(params) {
		log.debug("scvmmHostGroup: ${params}")
		def cloud = setupCloudConfig(params)
		def results = apiService.listHostGroups(getApiConfig(cloud))
		log.debug("listHostGroups: ${results}")
		return results.hostGroups?.collect { [name: it.path, value: it.path] }
	}

	def scvmmCluster(params) {
		log.debug("scvmmCluster: ${params}")
		def cloud = setupCloudConfig(params)
		def results = apiService.listClusters(getApiConfig(cloud))
		log.debug("listClusters: ${results}")
		return results.clusters?.collect { [name: it.name, value: it.id] }
	}

	def scvmmLibraryShares(params) {
		log.debug("scvmmLibraryShares: ${params}")
		def cloud = setupCloudConfig(params)
		def results = apiService.listLibraryShares(getApiConfig(cloud))
		log.debug("listLibraryShares: ${results}")
		return results.libraryShares?.collect { [name: it.Path, value: it.Path] }
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
