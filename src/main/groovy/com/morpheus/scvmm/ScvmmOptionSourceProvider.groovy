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
		return new ArrayList<String>(['scvmmCloud', 'scvmmHostGroup', 'scvmmCluster', 'scvmmLibraryShares', 'scvmmSharedControllers'])
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

	def scvmmCloud(params) {
		//log.debug params
        log.info("RAZI :: scvmmCloud >> params: ${params}")
		log.info("RAZI :: scvmmCloud >> params.config: ${params.config}")
		log.info("RAZI :: scvmmCloud >> params[\"config[scvmmHost]\"]: ${params["config[scvmmHost]"]}")
		log.info("RAZI :: scvmmCloud >> params[\"config[username]\"]: ${params["config[username]"]}")
		def config = [
				host:params.config?.scvmmHost ?: params["config[scvmmHost]"],
				username:params.config?.username ?: params["config[username]"]
		]
		log.info("RAZI :: scvmmCloud >> config: ${config}")
		def password = params.config?.password ?: params["config[password]"]
		log.info("RAZI :: scvmmCloud >> password: ${password}")
		if(password == '*' * 12 && params.zoneId) {
//			config.password = ComputeZone.read(params.zoneId.toLong()).configMap.password
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
			log.info("RAZI :: scvmmCloud >> if >> config.password: ${config.password}")
		} else {
			config.password = password
			log.info("RAZI :: scvmmCloud >> else >> config.password: ${config.password}")
		}
		def zoneId = params?.size() > 0 ? params.getAt(0).zoneId?.toLong() : null
		def host = params?.size() > 0 ? params.getAt(0).scvmmHost : null
		def username = params?.size() > 0 ? params.getAt(0).username : null
		def password1 = params?.size() > 0 ? params.getAt(0).password : null
		log.info("RAZI :: zoneId: ${zoneId}")
		log.info("RAZI :: host: ${host}")
		log.info("RAZI :: username: ${username}")
		log.info("RAZI :: password1: ${password1}")
//		def zone = new ComputeZone()
		def cloud = new Cloud()
		cloud.setConfigMap(config)
//		zone.zoneType = ComputeZoneType.findByCode('scvmm')
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))
//		zone.credentialData = credentialService.loadCredentialConfig(params.credential, config).data
		log.info("RAZI :: scvmmCloud >> params.credential: ${params.credential}")
		log.info("RAZI :: scvmmCloud >> config1: ${config}")
		def accountCredential = morpheusContext.services.accountCredential.loadCredentialConfig(params.credential, config)
		log.info("RAZI :: scvmmCloud >> accountCredential: ${accountCredential}")
		cloud.accountCredentialLoaded = true
		cloud.accountCredentialData = accountCredential?.data
//		def proxy = params.config?.apiProxy ? NetworkProxy.get(params.config.long('apiProxy')) : null
		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy
		log.debug("listing clouds {}", config)
		def results = listClouds(cloud)
		log.info("RAZI :: scvmmCloud >> results: ${results}")
		log.debug("cloud results {}", results)
		return results.clouds?.collect{[name: it.Name, value: it.ID]}
	}

	def scvmmHostGroup(params) {
		log.debug("scvmmHostGroup: {}", params)
		def config = [
				host      : params.config?.host ?: params["config[host]"],
				username  : params.config?.username ?: params["config[username]"]
		]
		log.info("RAZI :: scvmmHostGroup >> config: ${config}")
		def password = params.config?.password ?: params["config[password]"]
		log.info("RAZI :: scvmmHostGroup >> password: ${password}")
		if (password == '*' * 12 && params.zoneId) {
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
			log.info("RAZI :: scvmmHostGroup >> if >> config.password: ${config.password}")
		} else {
			config.password = password
			log.info("RAZI :: scvmmHostGroup >> else >> config.password: ${config.password}")
		}

		def cloud = new Cloud()
		cloud.setConfigMap(config)
		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
//		zone.zoneType = ComputeZoneType.findByCode('scvmm')
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))
//		zone.credentialData = credentialService.loadCredentialConfig(params.credential, config).data
//		zone.credentialLoaded = true
		log.info("RAZI :: scvmmHostGroup >> params.credential: ${params.credential}")
		log.info("RAZI :: scvmmHostGroup >> config1: ${config}")
		cloud.accountCredentialData = morpheusContext.async.accountCredential.loadCredentialConfig(params.credential, config).blockingGet()
		log.info("RAZI :: scvmmHostGroup >> cloud.accountCredentialData: ${cloud.accountCredentialData}")
		cloud.accountCredentialLoaded = true
//		def proxy = params.config?.apiProxy ? NetworkProxy.get(params.config.long('apiProxy')) : null
		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy
		log.debug "Listing host groups ${config}"
		def results = apiService.listHostGroups([zone: cloud])
		log.info("RAZI :: scvmmHostGroup >> results: ${results}")
		log.debug "Results ${results}"
		return results.hostGroups?.collect{[name: it.path, value: it.path]}
	}

	def scvmmCluster(params) {
		log.debug "scvmmCluster: ${params}"
		def config = [
				host      : params.config?.host ?: params["config[host]"],
				username  : params.config?.username ?: params["config[username]"],
				hostGroup : params.config?.hostGroup ?: params["config[hostGroup]"]
		]
		log.info("RAZI :: scvmmCluster >> config: ${config}")
		def password = params.config?.password ?: params["config[password]"]
		log.info("RAZI :: scvmmCluster >> password: ${password}")
		if (password == '*' * 12 && params.zoneId) {
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
			log.info("RAZI :: scvmmCluster >> if >> config.password: ${config.password}")
		} else {
			config.password = password
			log.info("RAZI :: scvmmCluster >> else >> config.password: ${config.password}")
		}

		def cloud = new Cloud()
		cloud.setConfigMap(config)
		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
//		zone.zoneType = ComputeZoneType.findByCode('scvmm')
//		zone.credentialData = credentialService.loadCredentialConfig(params.credential, config).data
//		zone.credentialLoaded = true
//		def proxy = params.config?.apiProxy ? NetworkProxy.get(params.config.long('apiProxy')) : null
//		zone.apiProxy = proxy
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))
		log.info("RAZI :: scvmmCluster >> params.credential: ${params.credential}")
		log.info("RAZI :: scvmmCluster >> config1: ${config}")
		cloud.accountCredentialData = morpheusContext.async.accountCredential.loadCredentialConfig(params.credential, config).blockingGet()
		log.info("RAZI :: scvmmCluster >> cloud.accountCredentialData: ${cloud.accountCredentialData}")
		cloud.accountCredentialLoaded = true
		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy
		log.debug "Listing clusters ${config}"
		def results = apiService.listClusters([zone: cloud])
		log.info("RAZI :: scvmmCluster >> results: ${results}")
		log.debug "Results ${results}"
		return results.clusters?.collect{[name: it.name, value: it.id]}
	}

	def scvmmLibraryShares(params) {
		log.debug "scvmmLibraryShares: ${params}"
		def config = [
				host      : params.config?.host ?: params["config[host]"],
				username  : params.config?.username ?: params["config[username]"]
		]
		log.info("RAZI :: scvmmLibraryShares >> config: ${config}")
		def password = params.config?.password ?: params["config[password]"]
		log.info("RAZI :: scvmmLibraryShares >> password: ${password}")
		if (password == '*' * 12 && params.zoneId) {
			config.password = morpheusContext.services.cloud.get(params.zoneId.toLong()).configMap.password
			og.info("RAZI :: scvmmLibraryShares >> if >> config.password: ${config.password}")
		}
		else {
			config.password = password
			log.info("RAZI :: scvmmLibraryShares >> else >> config.password: ${config.password}")
		}

		def cloud = new Cloud()
		cloud.setConfigMap(config)
		cloud.regionCode = params.config?.cloud ?: params["config[cloud]"]
		cloud.cloudType = morpheusContext.services.cloud.type.find(new DataQuery().withFilter('code', 'scvmm'))
		log.info("RAZI :: scvmmLibraryShares >> params.credential: ${params.credential}")
		log.info("RAZI :: scvmmLibraryShares >> config1: ${config}")
		cloud.accountCredentialData = morpheusContext.async.accountCredential.loadCredentialConfig(params.credential, config).blockingGet()
		log.info("RAZI :: scvmmLibraryShares >> cloud.accountCredentialData: ${cloud.accountCredentialData}")
		cloud.accountCredentialLoaded = true
		def proxy = params.config?.apiProxy ? morpheusContext.services.network.networkProxy.get(params.config.long('apiProxy')) : null
		cloud.apiProxy = proxy
		log.debug "Listing library shares ${config}"
		def results = apiService.listLibraryShares([zone: cloud])
		log.info("RAZI :: scvmmLibraryShares >> results: ${results}")
		log.debug "Results ${results}"
		return results.libraryShares?.collect{[name: it.Path, value: it.Path]}
	}

	def scvmmSharedControllers(params) {
		log.debug("scvmmSharedControllers: {}", params)

//		def accountId = params.accountId?.toLong()
//		def zone = ComputeZone.get(params.zoneId?.toLong())
		log.info("RAZI :: scvmmSharedControllers >> params.zoneId?.toLong(): ${params.zoneId?.toLong()}")
		def cloud = morpheusContext.services.cloud.get(params.zoneId?.toLong())
		log.info("RAZI :: scvmmSharedControllers >> cloud.id: ${cloud.id}")
//		def type = ComputeServerType.where { code == 'scvmmController'}.get([cache:true])
//		def type = new ComputeServerType(code: 'scvmmController')
//		def existingController = ComputeServer.where { computeServerType == type && zone == zone }.get()
		def existingController = morpheusContext.services.computeServer.find(new DataQuery()
				.withFilter('computeServerType.code', 'scvmmController')
				.withFilter('cloud.id', cloud.id))
		/*def sharedControllers = ComputeServer.withCriteria {
			createAlias('account','account')
			if(existingController) {
				ne('id', existingController.id)
			}
			eq('enabled', true)
			eq('computeServerType', type)
			order('name')
		}*/
		def query = new DataQuery()
				.withFilter('enabled', true)
				.withFilter('computeServerType.code', 'scvmmController')
		if(existingController) {
			query.withFilter('id', '!=', existingController.id)
		}
		def sharedControllers = morpheusContext.services.computeServer.find(query)
		log.info("RAZI :: scvmmSharedControllers >> sharedControllers: ${sharedControllers}")
		return sharedControllers?.collect{[name: it.name, value: it.id]}
	}

}
