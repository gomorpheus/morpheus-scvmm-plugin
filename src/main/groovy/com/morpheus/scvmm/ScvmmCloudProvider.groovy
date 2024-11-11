package com.morpheus.scvmm

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.providers.ProvisionProvider
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.MorpheusUtils
import com.morpheusdata.model.*
import com.morpheusdata.request.ValidateCloudRequest
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

@Slf4j
class ScvmmCloudProvider implements CloudProvider {
	public static final String CLOUD_PROVIDER_CODE = 'morpheus-scvmm-plugin.cloud'

	protected MorpheusContext context
	protected ScvmmPlugin plugin
	ScvmmApiService apiService

	public ScvmmCloudProvider(ScvmmPlugin plugin, MorpheusContext context) {
		super()
		this.@plugin = plugin
		this.@context = context
		this.apiService = new ScvmmApiService(context)
	}

	/**
	 * Grabs the description for the CloudProvider
	 * @return String
	 */
	@Override
	String getDescription() {
		return 'Describe me!'
	}

	/**
	 * Returns the Cloud logo for display when a user needs to view or add this cloud. SVGs are preferred.
	 * @since 0.13.0
	 * @return Icon representation of assets stored in the src/assets of the project.
	 */
	@Override
	Icon getIcon() {
		// TODO: change icon paths to correct filenames once added to your project
		return new Icon(path:'cloud.svg', darkPath:'cloud-dark.svg')
	}

	/**
	 * Returns the circular Cloud logo for display when a user needs to view or add this cloud. SVGs are preferred.
	 * @since 0.13.6
	 * @return Icon
	 */
	@Override
	Icon getCircularIcon() {
		// TODO: change icon paths to correct filenames once added to your project
		return new Icon(path:'cloud-circular.svg', darkPath:'cloud-circular-dark.svg')
	}

	/**
	 * Provides a Collection of OptionType inputs that define the required input fields for defining a cloud integration
	 * @return Collection of OptionType
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		Collection<OptionType> options = []
		return options
	}

	/**
	 * Grabs available provisioning providers related to the target Cloud Plugin. Some clouds have multiple provisioning
	 * providers or some clouds allow for service based providers on top like (Docker or Kubernetes).
	 * @return Collection of ProvisionProvider
	 */
	@Override
	Collection<ProvisionProvider> getAvailableProvisionProviders() {
	    return this.@plugin.getProvidersByType(ProvisionProvider) as Collection<ProvisionProvider>
	}

	/**
	 * Grabs available backup providers related to the target Cloud Plugin.
	 * @return Collection of BackupProvider
	 */
	@Override
	Collection<BackupProvider> getAvailableBackupProviders() {
		Collection<BackupProvider> providers = []
		return providers
	}

	/**
	 * Provides a Collection of {@link NetworkType} related to this CloudProvider
	 * @return Collection of NetworkType
	 */
	@Override
	Collection<NetworkType> getNetworkTypes() {
		Collection<NetworkType> networks = []
		return networks
	}

	/**
	 * Provides a Collection of {@link NetworkSubnetType} related to this CloudProvider
	 * @return Collection of NetworkSubnetType
	 */
	@Override
	Collection<NetworkSubnetType> getSubnetTypes() {
		Collection<NetworkSubnetType> subnets = []
		return subnets
	}

	/**
	 * Provides a Collection of {@link StorageVolumeType} related to this CloudProvider
	 * @return Collection of StorageVolumeType
	 */
	@Override
	Collection<StorageVolumeType> getStorageVolumeTypes() {
		Collection<StorageVolumeType> volumeTypes = []
		return volumeTypes
	}

	/**
	 * Provides a Collection of {@link StorageControllerType} related to this CloudProvider
	 * @return Collection of StorageControllerType
	 */
	@Override
	Collection<StorageControllerType> getStorageControllerTypes() {
		Collection<StorageControllerType> controllerTypes = []
		return controllerTypes
	}

	/**
	 * Grabs all {@link ComputeServerType} objects that this CloudProvider can represent during a sync or during a provision.
	 * @return collection of ComputeServerType
	 */
	@Override
	Collection<ComputeServerType> getComputeServerTypes() {
		Collection<ComputeServerType> serverTypes = []
		return serverTypes
	}

	/**
	 * Validates the submitted cloud information to make sure it is functioning correctly.
	 * If a {@link ServiceResponse} is not marked as successful then the validation results will be
	 * bubbled up to the user.
	 * @param cloudInfo cloud
	 * @param validateCloudRequest Additional validation information
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse validate(Cloud cloudInfo, ValidateCloudRequest validateCloudRequest) {
		log.debug("validate cloud: {}", cloudInfo)
		def rtn = [success: false, zone: cloudInfo, errors: [:]]
		try {
			if (rtn.zone) {
				def requiredFields = ['host', 'workingPath', 'diskPath', 'libraryShare']
				def scvmmOpts = apiService.getScvmmZoneOpts(context, cloudInfo)
				def zoneConfig = cloudInfo.getConfigMap()
				rtn.errors = validateRequiredConfigFields(requiredFields, zoneConfig)
				// Verify that a shared controller is selected if we already have an scvmm zone pointed to this host (MUST SHARE THE CONTROLLER)
				def validateControllerResults = validateSharedController(cloudInfo)
				if (!validateControllerResults.success) {
					rtn.errors['sharedController'] = validateControllerResults.msg ?: 'You must specify a shared controller'
				}
				if (!validateCloudRequest?.credentialUsername) {
					rtn.msg = 'Enter a username'
					rtn.errors.username = 'Enter a username'
				} else if (!validateCloudRequest?.credentialPassword) {
					rtn.msg = 'Enter a password'
					rtn.errors.password = 'Enter a password'
				}
				if (rtn.errors.size() == 0) {
					//set install agent
					def installAgent = MorpheusUtils.parseBooleanConfig(zoneConfig.installAgent)
					cloudInfo.setConfigProperty('installAgent', installAgent)
					//build opts
					scvmmOpts += [
							hypervisor : [:],
							sshHost    : zoneConfig.host,
							sshUsername: validateCloudRequest?.credentialUsername,
							sshPassword: validateCloudRequest?.credentialPassword,
							zoneRoot   : zoneConfig.workingPath,
							diskRoot   : zoneConfig.diskPath
					]
					def vmSitches = apiService.listClouds(scvmmOpts)
					log.debug("vmSitches: ${vmSitches}")
					if (vmSitches.success == true)
						rtn.success = true
					if (rtn.success == false)
						rtn.msg = 'Error connecting to scvmm'
				}
			} else {
				rtn.message = 'No zone found'
			}
		} catch (e) {
			log.error("An Exception Has Occurred", e)
		}
		return ServiceResponse.create(rtn)
	}

	/**
	 * Called when a Cloud From Morpheus is first saved. This is a hook provided to take care of initial state
	 * assignment that may need to take place.
	 * @param cloudInfo instance of the cloud object that is being initialized.
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse initializeCloud(Cloud cloudInfo) {
		return ServiceResponse.success()
	}

	/**
	 * Zones/Clouds are refreshed periodically by the Morpheus Environment. This includes things like caching of brownfield
	 * environments and resources such as Networks, Datastores, Resource Pools, etc.
	 * @param cloudInfo cloud
	 * @return ServiceResponse. If ServiceResponse.success == true, then Cloud status will be set to Cloud.Status.ok. If
	 * ServiceResponse.success == false, the Cloud status will be set to ServiceResponse.data['status'] or Cloud.Status.error
	 * if not specified. So, to indicate that the Cloud is offline, return `ServiceResponse.error('cloud is not reachable', null, [status: Cloud.Status.offline])`
	 */
	@Override
	ServiceResponse refresh(Cloud cloudInfo) {
		return ServiceResponse.success()
	}

	def checkCommunication(cloud, node) {
		log.debug("checkCommunication: {} {}", cloud, node)
		def rtn = [success: false]
		try {
			def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
			def listResults = apiService.listAllNetworks(scvmmOpts)
			if (listResults.success == true && listResults.networks) {
				rtn.success = true
			}
		} catch (e) {
			log.error("checkCommunication error:${e}", e)
		}
		return rtn
	}

	def getScvmmController(Cloud cloud) {
		def sharedControllerId = cloud.getConfigProperty('sharedController')
		def sharedController = sharedControllerId ? context.services.computeServer.get(sharedControllerId.toLong()) : null
		if (sharedController) {
			return sharedController
		}
		def rtn = context.services.computeServer.find(new DataQuery()
				.withFilter('zone.id', cloud.id)
				.withFilter('computeServerType.code', 'scvmmController')
				.withJoin('computeServerType'))
		if (rtn == null) {
			//old zone with wrong type
			rtn = context.services.computeServer.find(new DataQuery()
					.withFilter('zone.id', cloud.id)
					.withFilter('computeServerType.code', 'scvmmController')
					.withJoin('computeServerType'))
			if (rtn == null) {
				rtn = context.services.computeServer.find(new DataQuery()
						.withFilter('zone.id', cloud.id)
						.withFilter('serverType', 'hypervisor'))
			}
			//if we have tye type
			if (rtn) {
				def serverType = context.async.cloud.findComputeServerTypeByCode("scvmmController").blockingGet()
				rtn.computeServerType = serverType
				context.async.computeServer.save(rtn).blockingGet()
			}
		}
		return rtn
	}

	/**
	 * Zones/Clouds are refreshed periodically by the Morpheus Environment. This includes things like caching of brownfield
	 * environments and resources such as Networks, Datastores, Resource Pools, etc. This represents the long term sync method that happens
	 * daily instead of every 5-10 minute cycle
	 * @param cloudInfo cloud
	 */
	@Override
	void refreshDaily(Cloud cloudInfo) {
		log.debug("refreshDaily: {}", cloudInfo)
		try {
			def scvmmController = getScvmmController(cloudInfo)
			if (scvmmController) {
				def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloudInfo, scvmmController)
				def hostOnline = ConnectionUtils.testHostConnectivity(scvmmOpts.sshHost, 5985, false, true, null)
				log.debug("hostOnline: {}", hostOnline)
				if (hostOnline) {
					def checkResults = checkCommunication(cloudInfo, scvmmController)
					if (checkResults.success == true) {
						removeOrphanedResourceLibraryItems(cloudInfo, scvmmController)
					}
				}
			}
		} catch (e) {
			log.error "Error on refreshDailyZone: ${e}", e
		}
	}

	/**
	 * Called when a Cloud From Morpheus is removed. This is a hook provided to take care of cleaning up any state.
	 * @param cloudInfo instance of the cloud object that is being removed.
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse deleteCloud(Cloud cloudInfo) {
		return ServiceResponse.success()
	}

	/**
	 * Returns whether the cloud supports {@link CloudPool}
	 * @return Boolean
	 */
	@Override
	Boolean hasComputeZonePools() {
		return false
	}

	/**
	 * Returns whether a cloud supports {@link Network}
	 * @return Boolean
	 */
	@Override
	Boolean hasNetworks() {
		return true
	}

	/**
	 * Returns whether a cloud supports {@link CloudFolder}
	 * @return Boolean
	 */
	@Override
	Boolean hasFolders() {
		return false
	}

	/**
	 * Returns whether a cloud supports {@link Datastore}
	 * @return Boolean
	 */
	@Override
	Boolean hasDatastores() {
		return true
	}

	/**
	 * Returns whether a cloud supports bare metal VMs
	 * @return Boolean
	 */
	@Override
	Boolean hasBareMetal() {
		return false
	}

	/**
	 * Indicates if the cloud supports cloud-init. Returning true will allow configuration of the Cloud
	 * to allow installing the agent remotely via SSH /WinRM or via Cloud Init
	 * @return Boolean
	 */
	@Override
	Boolean hasCloudInit() {
		return true
	}

	/**
	 * Indicates if the cloud supports the distributed worker functionality
	 * @return Boolean
	 */
	@Override
	Boolean supportsDistributedWorker() {
		return false
	}

	/**
	 * Called when a server should be started. Returning a response of success will cause corresponding updates to usage
	 * records, result in the powerState of the computeServer to be set to 'on', and related instances set to 'running'
	 * @param computeServer server to start
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse startServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	/**
	 * Called when a server should be stopped. Returning a response of success will cause corresponding updates to usage
	 * records, result in the powerState of the computeServer to be set to 'off', and related instances set to 'stopped'
	 * @param computeServer server to stop
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse stopServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	/**
	 * Called when a server should be deleted from the Cloud.
	 * @param computeServer server to delete
	 * @return ServiceResponse
	 */
	@Override
	ServiceResponse deleteServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	/**
	 * Grabs the singleton instance of the provisioning provider based on the code defined in its implementation.
	 * Typically Providers are singleton and instanced in the {@link Plugin} class
	 * @param providerCode String representation of the provider short code
	 * @return the ProvisionProvider requested
	 */
	@Override
	ProvisionProvider getProvisionProvider(String providerCode) {
		return getAvailableProvisionProviders().find { it.code == providerCode }
	}

	/**
	 * Returns the default provision code for fetching a {@link ProvisionProvider} for this cloud.
	 * This is only really necessary if the provision type code is the exact same as the cloud code.
	 * @return the provision provider code
	 */
	@Override
	String getDefaultProvisionTypeCode() {
		return ScvmmProvisionProvider.PROVISION_PROVIDER_CODE
	}

	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	@Override
	MorpheusContext getMorpheus() {
		return this.@context
	}

	/**
	 * Returns the instance of the Plugin class that this provider is loaded from
	 * @return Plugin class contains references to other providers
	 */
	@Override
	Plugin getPlugin() {
		return this.@plugin
	}

	/**
	 * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
	 * that is seeded or generated related to this provider will reference it by this code.
	 * @return short code string that should be unique across all other plugin implementations.
	 */
	@Override
	String getCode() {
		return CLOUD_PROVIDER_CODE
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'SCVMM'
	}

	def validateRequiredConfigFields(fieldArray, config) {
		def errors = [:]
		fieldArray.each { field ->
			if (config[field] != null && config[field]?.size() == 0) {
				def display = field.replaceAll(/([A-Z])/, / $1/).toLowerCase()
				errors[field] = "Enter a ${display}"
			}
		}
		return errors
	}

	def validateSharedController(Cloud cloud) {
		log.debug "validateSharedController: ${cloud}"
		def rtn = [success: true]

		def sharedControllerId = cloud.getConfigProperty('sharedController')
		if (!sharedControllerId) {
			if (cloud.id) {
				def existingControllerInZone = context.services.computeServer.find(new DataQuery()
						.withFilter('computeServerType.code', 'scvmmController')
						.withFilter('zone.id', cloud.id.toLong()))
				if (existingControllerInZone) {
					rtn.success = true
					return rtn
				}
			}
			def existingController = context.services.computeServer.find(new DataQuery()
					.withFilter('enabled', true)
					.withFilter('account.id', cloud.account.id)
					.withFilter('computeServerType.code', 'scvmmController')
					.withFilter('externalIp', cloud.getConfigProperty('host')))
			if (existingController) {
				log.debug "Found another controller: ${existingController.id} in zone: ${existingController.cloud} that should be used"
				rtn.success = false
				rtn.msg = 'You must specify a shared controller'
			}
		} else {
			// Verify that the controller selected has the same Host ip as defined in the cloud
			def sharedController = context.services.computeServer.get(sharedControllerId?.toLong())
			if (sharedController?.externalIp != cloud.getConfigProperty('host')) {
				rtn.success = false
				rtn.msg = 'The selected controller is on a different host than specified for this cloud'
			}
		}
		return rtn
	}

	def removeOrphanedResourceLibraryItems(cloud, node) {
		log.debug("removeOrphanedResourceLibraryItems: {} {}", cloud, node)
		def rtn = [success:false]
		try {
			def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
			apiService.removeOrphanedResourceLibraryItems(scvmmOpts)
		} catch(e) {
			log.error("removeOrphanedResourceLibraryItems error:${e}", e)
		}
		return rtn
	}
}
