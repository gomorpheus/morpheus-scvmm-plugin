package com.morpheus.scvmm

import com.morpheusdata.core.MorpheusAccountCredentialService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.OptionSourceProvider
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServerType
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

	private getUsername(Cloud cloud) {
		cloud.accountCredentialData?.username ?: cloud.getConfigProperty('username') ?: 'dunno'
	}

	private getPassword(Cloud cloud) {
		cloud.accountCredentialData?.password ?: cloud.getConfigProperty('password')
	}

	def getScvmmInitializationOpts(cloud) {
		def cloudConfig = cloud.getConfigMap()
		def diskRoot = cloudConfig.diskPath?.length() > 0 ? cloudConfig.diskPath : apiService.defaultRoot + '\\Disks'
		def zoneRoot = cloudConfig.workingPath?.length() > 0 ? cloudConfig.workingPath : apiService.defaultRoot
		return [sshHost:cloudConfig.host, sshUsername:getUsername(cloud), sshPassword:getPassword(cloud), zoneRoot:zoneRoot,
				diskRoot:diskRoot]
	}

	def listClouds(cloud) {
		apiService.listClouds(apiService.getScvmmZoneOpts(morpheusContext, cloud) + getScvmmInitializationOpts(cloud))
	}

	def listHostGroups(cloud) {
		apiService.listHostGroups(apiService.getScvmmZoneOpts(morpheusContext, cloud) + getScvmmInitializationOpts(cloud))
	}

	def listClusters(cloud) {
		apiService.listClusters(apiService.getScvmmZoneOpts(morpheusContext, cloud) + getScvmmInitializationOpts(cloud))
	}

	def listLibraryShares(cloud) {
		apiService.listLibraryShares(apiService.getScvmmZoneOpts(morpheusContext, cloud) + getScvmmInitializationOpts(cloud))
	}

	def scvmmCloud(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug "scvmmCloud: ${params}"
		def config = [
				host:params.config?.host ?: params["config[host]"],
				username:params.config?.username ?: params["config[username]"]
		]
		def password = params.config?.password ?: params["config[password]"]
		if(password == '*' * 12 && params.zoneId) {
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
		} else {
			config.password = password
		}
		def cloud = new Cloud()
		cloud.setConfigMap(config)
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))
		if(params.credential) {
			def accountCredential = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
			cloud.accountCredentialLoaded = true
			cloud.accountCredentialData = accountCredential?.data
		}
		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy
		log.debug("listing clouds {}", config)
		def results = listClouds(cloud)
		log.debug("cloud results {}", results)
		return results.clouds?.collect{[name: it.Name, value: it.ID]}
	}

	def scvmmHostGroup(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug("scvmmHostGroup: {}", params)
		def config = [
				host      : params.config?.host ?: params["config[host]"],
				username  : params.config?.username ?: params["config[username]"]
		]
		def password = params.config?.password ?: params["config[password]"]
		if (password == '*' * 12 && params.zoneId) {
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
		} else {
			config.password = password
		}

		def cloud = new Cloud()
		cloud.setConfigMap(config)
		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))
		if(params.credential) {
			def accountCredential = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
			cloud.accountCredentialLoaded = true
			cloud.accountCredentialData = accountCredential?.data
		}
		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy
		log.debug "Listing host groups ${config}"
		def results = listHostGroups(cloud)
		log.debug "Results ${results}"
		return results.hostGroups?.collect{[name: it.path, value: it.path]}
	}

	def scvmmCluster(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug "scvmmCluster: ${params}"
		def config = [
				host      : params.config?.host ?: params["config[host]"],
				username  : params.config?.username ?: params["config[username]"],
				hostGroup : params.config?.hostGroup ?: params["config[hostGroup]"]
		]
		def password = params.config?.password ?: params["config[password]"]
		if (password == '*' * 12 && params.zoneId) {
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
		} else {
			config.password = password
		}

		def cloud = new Cloud()
		cloud.setConfigMap(config)
		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))
		if(params.credential) {
			def accountCredential = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
			cloud.accountCredentialLoaded = true
			cloud.accountCredentialData = accountCredential?.data
		}
		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy
		log.debug "Listing clusters ${config}"
		def results = listClusters(cloud)
		log.debug "Results ${results}"
		return results.clusters?.collect{[name: it.name, value: it.id]}
	}

	def scvmmLibraryShares(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug "scvmmLibraryShares: ${params}"
		def config = [
				host      : params.config?.host ?: params["config[host]"],
				username  : params.config?.username ?: params["config[username]"]
		]
		def password = params.config?.password ?: params["config[password]"]
		if (password == '*' * 12 && params.zoneId) {
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
		}
		else {
			config.password = password
		}

		def cloud = new Cloud()
		cloud.setConfigMap(config)
		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))
		if(params.credential) {
			def accountCredential = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
			cloud.accountCredentialLoaded = true
			cloud.accountCredentialData = accountCredential?.data
		}
		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy
		log.debug "Listing library shares ${config}"
		def results = listLibraryShares(cloud)
		return results.libraryShares?.collect{[name: it.Path, value: it.Path]}
	}

	def scvmmSharedControllers(params) {
		params = params instanceof Object[] ? params.getAt(0) : params
		log.debug("scvmmSharedControllers: ${params}")
		def cloud
		if(params.zoneId){
			cloud = morpheusContext.services.cloud.get(params.zoneId?.toLong())
		} else {
			cloud = new Cloud()
		}
		def existingController = morpheusContext.services.computeServer.find(new DataQuery()
				.withFilter('computeServerType.code', 'scvmmController')
				.withFilter('cloud.id', cloud?.id))

		def query = new DataQuery()
				.withFilter('enabled', true)
				.withFilter('computeServerType.code', 'scvmmController')
		if(existingController) {
			query.withFilter('id', '!=', existingController.id)
		}
		def sharedControllers = morpheusContext.services.computeServer.find(query)
		return sharedControllers?.collect{[name: it.name, value: it.id]}
	}

}
