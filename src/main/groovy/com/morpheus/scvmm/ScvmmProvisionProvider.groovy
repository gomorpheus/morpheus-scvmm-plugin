package com.morpheus.scvmm

import com.morpheusdata.core.AbstractProvisionProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataAndFilter
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.ProvisionProvider
import com.morpheusdata.core.providers.WorkloadProvisionProvider
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.model.*
import com.morpheusdata.model.provisioning.WorkloadRequest
import com.morpheusdata.response.InitializeHypervisorResponse
import com.morpheusdata.response.PrepareWorkloadResponse
import com.morpheusdata.response.ProvisionResponse
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

@Slf4j
class ScvmmProvisionProvider extends AbstractProvisionProvider implements WorkloadProvisionProvider, ProvisionProvider.HypervisorProvisionFacet {
	//public static final String PROVISION_PROVIDER_CODE = 'morpheus-scvmm-plugin.provision'
	public static final String PROVIDER_CODE = 'scvmm.provision'
	public static final String PROVISION_TYPE_CODE = 'scvmm'

	protected MorpheusContext context
	protected ScvmmPlugin plugin
	ScvmmApiService apiService

	public ScvmmProvisionProvider(ScvmmPlugin plugin, MorpheusContext context) {
		super()
		this.@context = context
		this.@plugin = plugin
		this.apiService = new ScvmmApiService(context)
	}

	/**
	 * Initialize a compute server as a Hypervisor. Common attributes defined in the {@link InitializeHypervisorResponse} will be used
	 * to update attributes on the hypervisor, including capacity information. Additional details can be updated by the plugin provider
	 * using the `context.services.computeServer.save(server)` API.
	 * @param cloud cloud associated to the hypervisor
	 * @param server representing the hypervisor
	 * @return a {@link ServiceResponse} containing an {@link InitializeHypervisorResponse}. The response attributes will be
	 * used to fill in necessary attributes of the server.
	 */
	@Override
	ServiceResponse<InitializeHypervisorResponse> initializeHypervisor(Cloud cloud, ComputeServer server) {
		log.debug("initializeHypervisor: cloud: {}, server: {}", cloud, server)
		ServiceResponse<InitializeHypervisorResponse> rtn = new ServiceResponse<>(new InitializeHypervisorResponse())
		try {
			def sharedController = cloud.getConfigProperty('sharedController')
			if(sharedController) {
				// No controller needed.. we are sharing another cloud's controller
				rtn.success = true
			} else {
				def opts = apiService.getScvmmZoneOpts(context, cloud)
				opts += apiService.getScvmmControllerOpts(cloud, server)
				def serverInfo = apiService.getScvmmServerInfo(opts)
				log.debug("serverInfo: ${serverInfo}")
				if (serverInfo.success == true && serverInfo.hostname) {
					server.hostname = serverInfo.hostname
				}
				def maxStorage = serverInfo?.disks ? serverInfo?.disks.toLong() : 0
				def maxMemory = serverInfo?.memory ? serverInfo?.memory.toLong() : 0
				def maxCores = 1

				rtn.data.serverOs = new OsType(code: 'windows.server.2012')
				rtn.data.commType = 'winrm' //ssh, minrm
				rtn.data.maxMemory = maxMemory
				rtn.data.maxCores = maxCores
				rtn.data.maxStorage = maxStorage
				rtn.success = true
				if (server.agentInstalled != true) {
					def prepareResults = apiService.prepareNode(opts)
				}
			}
		} catch (e) {
			log.error("initialize hypervisor error:${e}", e)
		}
		return rtn
	}

	/**
	 * This method is called before runWorkload and provides an opportunity to perform action or obtain configuration
	 * that will be needed in runWorkload. At the end of this method, if deploying a ComputeServer with a VirtualImage,
	 * the sourceImage on ComputeServer should be determined and saved.
	 * @param workload the Workload object we intend to provision along with some of the associated data needed to determine
	 *                 how best to provision the workload
	 * @param workloadRequest the RunWorkloadRequest object containing the various configurations that may be needed
	 *                        in running the Workload. This will be passed along into runWorkload
	 * @param opts additional configuration options that may have been passed during provisioning
	 * @return Response from API
	 */
	@Override
	ServiceResponse<PrepareWorkloadResponse> prepareWorkload(Workload workload, WorkloadRequest workloadRequest, Map opts) {
		ServiceResponse<PrepareWorkloadResponse> resp = new ServiceResponse<PrepareWorkloadResponse>(
			true, // successful
			'', // no message
			null, // no errors
			new PrepareWorkloadResponse(workload:workload) // adding the workload to the response for convenience
		)
		return resp
	}

	/**
	 * Some older clouds have a provision type code that is the exact same as the cloud code. This allows one to set it
	 * to match and in doing so the provider will be fetched via the cloud providers {@link CloudProvider#getDefaultProvisionTypeCode()} method.
	 * @return code for overriding the ProvisionType record code property
	 */
	@Override
	String getProvisionTypeCode() {
		return PROVISION_TYPE_CODE
	}

	/**
	 * Provide an icon to be displayed for ServicePlans, VM detail page, etc.
	 * where a circular icon is displayed
	 * @since 0.13.6
	 * @return Icon
	 */
	@Override
	Icon getCircularIcon() {
		// TODO: change icon paths to correct filenames once added to your project
		return new Icon(path:'provision-circular.svg', darkPath:'provision-circular-dark.svg')
	}

	/**
	 * Provides a Collection of OptionType inputs that need to be made available to various provisioning Wizards
	 * @return Collection of OptionTypes
	 */
	@Override
	Collection<OptionType> getOptionTypes() {
		Collection<OptionType> options = []
		options << new OptionType(
				name: 'skip agent install',
				code: 'provisionType.scvmm.noAgent',
				category: 'provisionType.scvmm',
				inputType: OptionType.InputType.CHECKBOX,
				fieldName: 'noAgent',
				fieldContext: 'config',
				fieldCode: 'gomorpheus.optiontype.SkipAgentInstall',
				fieldLabel: 'Skip Agent Install',
				fieldGroup:'Advanced Options',
				displayOrder: 4,
				required: false,
				enabled: true,
				editable:false,
				global:false,
				placeHolder:null,
				helpBlock:'Skipping Agent installation will result in a lack of logging and guest operating system statistics. Automation scripts may also be adversely affected.',
				defaultValue:null,
				custom:false,
				fieldClass:null
		)
		options << new OptionType(
				name: 'capability profile',
				code: 'provisionType.scvmm.capabilityProfile',
				category: 'provisionType.scvmm',
				inputType: OptionType.InputType.SELECT,
				fieldName: 'scvmmCapabilityProfile',
				fieldContext: 'config',
				fieldCode: 'gomorpheus.optiontype.CapabilityProfile',
				fieldLabel: 'Capability Profile111',
				fieldGroup:'Options',
				displayOrder: 11,
				required: true,
				enabled: true,
				editable:true,
				global:false,
				placeHolder:null,
				helpBlock:'',
				defaultValue:null,
				custom:false,
				fieldClass:null,
				optionSource: 'scvmmCapabilityProfile',
				optionSourceType:'scvmm'
		)
		return options
	}

	/**
	 * Provides a Collection of OptionType inputs for configuring node types
	 * @since 0.9.0
	 * @return Collection of OptionTypes
	 */
	@Override
	Collection<OptionType> getNodeOptionTypes() {
		Collection<OptionType> nodeOptions = []
		return nodeOptions
	}

	/**
	 * Provides a Collection of StorageVolumeTypes that are available for root StorageVolumes
	 * @return Collection of StorageVolumeTypes
	 */
	@Override
	Collection<StorageVolumeType> getRootVolumeStorageTypes() {
		Collection<StorageVolumeType> volumeTypes = []
		// TODO: create some storage volume types and add to collection
		return volumeTypes
	}

	/**
	 * Provides a Collection of StorageVolumeTypes that are available for data StorageVolumes
	 * @return Collection of StorageVolumeTypes
	 */
	@Override
	Collection<StorageVolumeType> getDataVolumeStorageTypes() {
		Collection<StorageVolumeType> dataVolTypes = []
		// TODO: create some data volume types and add to collection
		return dataVolTypes
	}

	/**
	 * Provides a Collection of ${@link ServicePlan} related to this ProvisionProvider that can be seeded in.
	 * Some clouds do not use this as they may be synced in from the public cloud. This is more of a factor for
	 * On-Prem clouds that may wish to have some precanned plans provided for it.
	 * @return Collection of ServicePlan sizes that can be seeded in at plugin startup.
	 */
	@Override
	Collection<ServicePlan> getServicePlans() {
		def servicePlans = []
		servicePlans << new ServicePlan([code: 'scvmm-1024', editable: true, name: '1 Core, 1GB Memory', description: '1 Core, 1GB Memory', sortOrder: 1, maxStorage: 10l * 1024l * 1024l * 1024l, maxMemory: 1l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 1, customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])
		servicePlans << new ServicePlan([code: 'scvmm-2048', editable: true, name: '1 Core, 2GB Memory', description: '1 Core, 2GB Memory', sortOrder: 2, maxStorage: 20l * 1024l * 1024l * 1024l, maxMemory: 2l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 1, customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])
		servicePlans << new ServicePlan([code: 'scvmm-4096', editable: true, name: '1 Core, 4GB Memory', description: '1 Core, 4GB Memory', sortOrder: 3, maxStorage: 40l * 1024l * 1024l * 1024l, maxMemory: 4l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 1, customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])
		servicePlans << new ServicePlan([code: 'scvmm-8192', editable: true, name: '2 Core, 8GB Memory', description: '2 Core, 8GB Memory', sortOrder: 4, maxStorage: 80l * 1024l * 1024l * 1024l, maxMemory: 8l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 2, customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])
		servicePlans << new ServicePlan([code: 'scvmm-16384', editable: true, name: '2 Core, 16GB Memory', description: '2 Core, 16GB Memory', sortOrder: 5, maxStorage: 160l * 1024l * 1024l * 1024l, maxMemory: 16l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 2, customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])
		servicePlans << new ServicePlan([code: 'scvmm-24576', editable: true, name: '4 Core, 24GB Memory', description: '4 Core, 24GB Memory', sortOrder: 6, maxStorage: 240l * 1024l * 1024l * 1024l, maxMemory: 24l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 4, customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])
		servicePlans << new ServicePlan([code: 'scvmm-32768', editable: true, name: '4 Core, 32GB Memory', description: '4 Core, 32GB Memory', sortOrder: 7, maxStorage: 320l * 1024l * 1024l * 1024l, maxMemory: 32l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 4, customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])
		servicePlans << new ServicePlan([code: 'scvmm-hypervisor', editable: false, name: 'SCVMM hypervisor', description: 'custom hypervisor plan', sortOrder: 100, hidden: true, maxCores: 1, maxCpu: 1, maxStorage: 20l * 1024l * 1024l * 1024l, maxMemory: (long) (1l * 1024l * 1024l * 1024l), active: true, customCores: true, customMaxStorage: true, customMaxDataStorage: true, customMaxMemory: true])
		servicePlans << new ServicePlan([code: 'internal-custom-scvmm', editable: false, name: 'Custom SCVMM', description: 'Custom SCVMM', sortOrder: 0, customMaxStorage: true, customMaxDataStorage: true, addVolumes: true, customCpu: true, customCores: true, customMaxMemory: true, deletable: false, provisionable: false, maxStorage: 0l, maxMemory: 0l, maxCpu: 0])
		servicePlans
	}

	/**
	 * Validates the provided provisioning options of a workload. A return of success = false will halt the
	 * creation and display errors
	 * @param opts options
	 * @return Response from API. Errors should be returned in the errors Map with the key being the field name and the error
	 * message as the value.
	 */
	@Override
	ServiceResponse validateWorkload(Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * This method is a key entry point in provisioning a workload. This could be a vm, a container, or something else.
	 * Information associated with the passed Workload object is used to kick off the workload provision request
	 * @param workload the Workload object we intend to provision along with some of the associated data needed to determine
	 *                 how best to provision the workload
	 * @param workloadRequest the RunWorkloadRequest object containing the various configurations that may be needed
	 *                        in running the Workload
	 * @param opts additional configuration options that may have been passed during provisioning
	 * @return Response from API
	 */
	@Override
	ServiceResponse<ProvisionResponse> runWorkload(Workload workload, WorkloadRequest workloadRequest, Map opts) {
		log.debug "runWorkload: ${workload} ${workloadRequest} ${opts}"
		log.info ("Ray :: runWorkload: workload?.id: ${workload?.id}")
		log.info ("Ray :: runWorkload: workloadRequest: ${workloadRequest}")
		log.info ("Ray :: runWorkload: opts: ${opts}")
		ProvisionResponse provisionResponse = new ProvisionResponse(success: true)
		def server = workload.server
		log.info ("Ray :: runWorkload: server?.id: ${server?.id}")
		def containerId = workload?.id
		log.info ("Ray :: runWorkload: containerId: ${containerId}")
		def cloud = server.cloud
		log.info ("Ray :: runWorkload: cloud.id: ${cloud.id}")
		try {
			def containerConfig = workload.getConfigMap()
			log.info ("Ray :: runWorkload: containerConfig: ${containerConfig}")
			opts.server = workload.server
			log.info ("Ray :: runWorkload: opts.server : ${opts.server }")
			//opts.zone = zoneService.loadFullZone(container.server.zone)
			opts.noAgent = containerConfig.noAgent
			log.info ("Ray :: runWorkload: opts.noAgent: ${opts.noAgent}")
			//opts.account = server.account
			//opts.server.externalId = opts.server.name

			def controllerNode = pickScvmmController(cloud)
			log.info ("Ray :: runWorkload: controllerNode: ${controllerNode}")
			log.info ("Ray :: runWorkload: controllerNode?.id: ${controllerNode?.id}")
			def scvmmOpts = apiService.getScvmmZoneOpts(context, cloud)
			log.info ("Ray :: runWorkload: scvmmOpts: ${scvmmOpts}")
			scvmmOpts.name = server.name
			log.info ("Ray :: runWorkload: scvmmOpts.name : ${scvmmOpts.name }")
			def imageId
			def virtualImage
			//def applianceServerUrl = applianceService.getApplianceUrl(server.zone)

			//scvmmOpts.scvmmProvisionService = getService('scvmmProvisionService')
			scvmmOpts.controllerServerId = controllerNode.id
			log.info ("Ray :: runWorkload: scvmmOpts.controllerServerId : ${scvmmOpts.controllerServerId}")
			def externalPoolId
			log.info ("Ray :: runWorkload: containerConfig.resourcePool : ${containerConfig.resourcePool}")
			if(containerConfig.resourcePool) {
				try {
					// check: dustin will implement getResourcePoolFromIdString in core
					def resourcePool// = zonePoolService.getResourcePoolFromIdString(containerConfig.resourcePool, opts.server.account?.id, container?.instance?.site?.id, opts.zone?.id)
					externalPoolId = resourcePool?.externalId
				} catch(exN) {
					externalPoolId = containerConfig.resourcePool
				}
			}
			log.info ("Ray :: runWorkload: externalPoolId: ${externalPoolId}")

			// host, datastore configuration
			ComputeServer node
			Datastore datastore
			def volumePath, nodeId, highlyAvailable
			def storageVolumes = server.volumes
			log.info ("Ray :: runWorkload: storageVolumes?.size(): ${storageVolumes?.size()}")
			def rootVolume = storageVolumes.find { it.rootVolume == true }
			log.info ("Ray :: runWorkload: rootVolume: ${rootVolume}")
			log.info ("Ray :: runWorkload: rootVolume?.id: ${rootVolume?.id}")
			def maxStorage = getContainerRootSize(workload)
			log.info ("Ray :: runWorkload: maxStorage: ${maxStorage}")
			def maxMemory = workload.maxMemory ?: workload.instance.plan.maxMemory
			log.info ("Ray :: runWorkload: maxMemory: ${maxMemory}")
			setDynamicMemory(scvmmOpts, workload.instance.plan)
			try {
				log.info ("Ray :: runWorkload: containerConfig.cloneContainerId: ${containerConfig.cloneContainerId}")
				if(containerConfig.cloneContainerId) {
					Workload cloneContainer = context.services.workload.get(containerConfig.cloneContainerId.toLong())
					log.info ("Ray :: runWorkload: cloneContainer: ${cloneContainer}")
					log.info ("Ray :: runWorkload: cloneContainer.server.volumes?.size(): ${cloneContainer.server.volumes?.size()}")
					cloneContainer.server.volumes?.eachWithIndex { vol, i ->
						server.volumes[i].datastore = vol.datastore
						//server.volumes[i].save()
						context.services.storageVolume.save(server.volumes[i])
					}
				}

				scvmmOpts.volumePaths = []

				(node, datastore, volumePath, highlyAvailable) = getHostAndDatastore(cloud, server.account, externalPoolId, containerConfig.hostId, rootVolume?.datastore, rootVolume?.datastoreOption, maxStorage, workload.instance.site?.id, maxMemory)
				nodeId = node?.id
				log.info ("Ray :: runWorkload: node: ${node}")
				log.info ("Ray :: runWorkload: datastore: ${datastore}")
				log.info ("Ray :: runWorkload: volumePath: ${volumePath}")
				log.info ("Ray :: runWorkload: highlyAvailable: ${highlyAvailable}")
				scvmmOpts.datastoreId = datastore?.externalId
				scvmmOpts.hostExternalId = node?.externalId
				scvmmOpts.volumePath = volumePath
				scvmmOpts.volumePaths << volumePath
				scvmmOpts.highlyAvailable = highlyAvailable
				log.info ("Ray :: runWorkload: scvmmOpts1: ${scvmmOpts}")

				if (rootVolume) {
					rootVolume.datastore = datastore
					//rootVolume.save()
					context.services.storageVolume.save(rootVolume)
				}

				storageVolumes?.each { vol ->
					if(!vol.rootVolume) {
						def tmpNode, tmpDatastore, tmpVolumePath, tmpHighlyAvailable
						(tmpNode, tmpDatastore, tmpVolumePath, tmpHighlyAvailable) = getHostAndDatastore(cloud, server.account, externalPoolId, containerConfig.hostId, vol?.datastore, vol?.datastoreOption, maxStorage, workload.instance.site?.id, maxMemory)
						vol.datastore = tmpDatastore
						if(tmpVolumePath) {
							vol.volumePath = tmpVolumePath
							scvmmOpts.volumePaths << tmpVolumePath
						}
						//vol.save()
						context.services.storageVolume.save(vol)
					}
				}
			} catch(e) {
				log.error("runWorkload error: Error in determining host and datastore: {}", e.message, e)
				return new ServiceResponse(success: false, msg: provisionResponse.message ?: 'Error in determining host and datastore', error: provisionResponse.message, data: provisionResponse)
			}

			scvmmOpts += apiService.getScvmmControllerOpts(cloud, controllerNode)
			log.info ("Ray :: runWorkload: scvmmOpts2: ${scvmmOpts}")
			log.info ("Ray :: runWorkload: containerConfig.imageId: ${containerConfig.imageId}")
			log.info ("Ray :: runWorkload: containerConfig.template: ${containerConfig.template}")
			log.info ("Ray :: runWorkload: workload.workloadType.virtualImage?.id: ${workload.workloadType.virtualImage?.id}")
			if(containerConfig.imageId || containerConfig.template || workload.workloadType.virtualImage?.id) {
				def virtualImageId = (containerConfig.imageId?.toLong() ?: containerConfig.template?.toLong() ?: workload.workloadType.virtualImage.id)
				//virtualImage = VirtualImage.get(virtualImageId)
				log.info ("Ray :: runWorkload: virtualImageId: ${virtualImageId}")
				virtualImage = context.async.virtualImage.get(virtualImageId).blockingGet()
				log.info ("Ray :: runWorkload: virtualImage?.id: ${virtualImage?.id}")
				scvmmOpts.scvmmGeneration = virtualImage?.getConfigProperty('generation') ?: 'generation1'
				scvmmOpts.isSyncdImage = virtualImage?.refType == 'ComputeZone'
				scvmmOpts.isTemplate = !(virtualImage?.remotePath != null) && !virtualImage?.systemImage
				scvmmOpts.templateId = virtualImage?.externalId
				log.info ("Ray :: runWorkload: scvmmOpts3: ${scvmmOpts}")
				if(scvmmOpts.isSyncdImage) {
					scvmmOpts.diskExternalIdMappings = getDiskExternalIds(virtualImage, cloud)
					log.info ("Ray :: runWorkload: scvmmOpts.diskExternalIdMappings: ${scvmmOpts.diskExternalIdMappings}")
					imageId = scvmmOpts.diskExternalIdMappings.find { it.rootVolume == true}.externalId
				} else {
					imageId = virtualImage.externalId
				}
				log.info ("Ray :: runWorkload: imageId1: ${imageId}")
				if(!imageId) { //If its userUploaded and still needs uploaded
					def cloudFiles = context.async.virtualImage.getVirtualImageFiles(virtualImage).blockingGet()
					log.info ("Ray :: runWorkload: cloudFiles?.size(): ${cloudFiles?.size()}")
					def imageFile = cloudFiles?.find { cloudFile -> cloudFile.name.toLowerCase().endsWith(".vhd") || cloudFile.name.toLowerCase().endsWith(".vhdx") || cloudFile.name.toLowerCase().endsWith(".vmdk") }
					log.info ("Ray :: runWorkload: imageFile: ${imageFile}")
					log.info ("Ray :: runWorkload: imageFile?.name: ${imageFile?.name}")
					def containerImage = [
							name	: virtualImage.name ?: workload.workloadType.imageCode,
							minDisk	: 5,
							minRam	: 512* ComputeUtility.ONE_MEGABYTE,
							virtualImageId	: virtualImage.id,
							tags	: 'morpheus, ubuntu',
							imageType	: virtualImage.imageType,
							containerType	: 'vhd',
							imageFile	: imageFile,
							cloudFiles	: cloudFiles
					]
					log.info ("Ray :: runWorkload: containerImage: ${containerImage}")
					scvmmOpts.image = containerImage
					//scvmmOpts.applianceServerUrl = applianceServerUrl
					scvmmOpts.userId = workload.instance.createdBy?.id
					log.info ("Ray :: runWorkload: scvmmOpts4: ${scvmmOpts}")
					log.debug "scvmmOpts: ${scvmmOpts}"
					def imageResults = apiService.insertContainerImage(scvmmOpts)
					log.info ("Ray :: runWorkload: imageResults: ${imageResults}")
					if(imageResults.success == true) {
						imageId = imageResults.imageId
						log.info ("Ray :: runWorkload: imageId2: ${imageId}")
						def locationConfig = [
								virtualImage: virtualImage,
								code        : "scvmm.image.${cloud.id}.${virtualImage.externalId}",
								internalId  : virtualImage.externalId,
								externalId  : virtualImage.externalId,
								imageName   : virtualImage.name
						]
						log.info ("Ray :: runWorkload: locationConfig: ${locationConfig}")
						VirtualImageLocation location = new VirtualImageLocation(locationConfig)
						context.services.virtualImage.location.create(location)
					} else {
						log.info ("Ray :: runWorkload: provision response false")
						provisionResponse.success = false
					}
				}

				log.info ("Ray :: runWorkload: scvmmOpts.templateId: ${scvmmOpts.templateId}")
				log.info ("Ray :: runWorkload: scvmmOpts.isSyncdImage: ${scvmmOpts.isSyncdImage}")
				if(scvmmOpts.templateId && scvmmOpts.isSyncdImage) {
					// Determine if any additional data disks were added to the template
					scvmmOpts.additionalTemplateDisks = additionalTemplateDisksConfig(container, scvmmOpts)
					log.debug "scvmmOpts.additionalTemplateDisks ${scvmmOpts.additionalTemplateDisks}"
				}
				log.info ("Ray :: runWorkload: scvmmOpts.additionalTemplateDisks: ${scvmmOpts.additionalTemplateDisks}")
			}



			log.info ("Ray :: runWorkload: imageId4: ${imageId}")
			if(imageId) {
				scvmmOpts.isSysprep = virtualImage?.isSysprep
				log.info ("Ray :: runWorkload: scvmmOpts.isSysprep: ${scvmmOpts.isSysprep}")
				if(scvmmOpts.isSysprep) {
					// Need to lookup the OS name
					scvmmOpts.OSName = apiService.getMapScvmmOsType(virtualImage.osType.code, false)
				}
				log.info ("Ray :: runWorkload: scvmmOpts.OSName: ${scvmmOpts.OSName}")
				opts.installAgent = (virtualImage ? virtualImage.installAgent : true) && !opts.noAgent
				log.info ("Ray :: runWorkload: opts.installAgent: ${opts.installAgent}")
				opts.skipNetworkWait = virtualImage?.imageType == 'iso' || !virtualImage?.vmToolsInstalled ? true : false
				if(virtualImage?.installAgent == false) {
					opts.noAgent = true
				}
				//user config
				//def createdBy = getInstanceCreateUser(container.instance)
				def userGroups = workload.instance.userGroups?.toList() ?: []
				if (workload.instance.userGroup && userGroups.contains(workload.instance.userGroup) == false) {
					userGroups << workload.instance.userGroup
				}
				//opts.userConfig = userGroupService.buildContainerUserGroups(opts.account, virtualImage, userGroups, createdBy, opts + [ isCloudInit: virtualImage?.isCloudInit || scvmmOpts.isSysprep])
				//opts.server.sshUsername = opts.userConfig.sshUsername
				//opts.server.sshPassword = opts.userConfig.sshPassword
				server.sourceImage = virtualImage
				server.externalId = scvmmOpts.name
				server.parentServer = node
				server.serverOs = server.serverOs ?: virtualImage.osType
				server.osType = (virtualImage.osType?.platform == 'windows' ? 'windows' : 'linux') ?: virtualImage.platform
				def newType = this.findVmNodeServerTypeForCloud(cloud.id?.Long(), server.osType, PROVISION_TYPE_CODE)
				if(newType && server.computeServerType != newType)
					server.computeServerType = newType
				//opts.server.save(flush:true)
				server = saveAndGetMorpheusServer(server, true)
				scvmmOpts.imageId = imageId
				scvmmOpts.server = server
				scvmmOpts += getScvmmContainerOpts(workload)
				scvmmOpts.hostname = server.getExternalHostname()
				scvmmOpts.domainName = server.getExternalDomain()
				scvmmOpts.fqdn = scvmmOpts.hostname
				if(scvmmOpts.domainName) {
					scvmmOpts.fqdn += '.' + scvmmOpts.domainName
				}
				scvmmOpts.networkConfig = opts.networkConfig
				if(scvmmOpts.networkConfig?.primaryInterface?.network?.pool) {
					scvmmOpts.networkConfig.primaryInterface.poolType = scvmmOpts.networkConfig.primaryInterface.network.pool.type.code
				}
				// check: if license is required
				//scvmmOpts.licenses = licenseService.applyLicense(opts.server.sourceImage, 'ComputeServer', opts.server.id, opts.server.account)?.data?.licenses
				/*if(scvmmOpts.licenses) {
					def license = scvmmOpts.licenses[0]
					scvmmOpts.license = [fullName: license.fullName, productKey: license.licenseKey, orgName: license.orgName]
				}*/


				if(virtualImage?.isCloudInit || scvmmOpts.isSysprep) {
					def initOptions = constructCloudInitOptions(workload, opts.installAgent, scvmmOpts.platform, opts.userConfig, virtualImage, scvmmOpts.networkConfig, scvmmOpts.licenses, scvmmOpts)
					opts.installAgent = initOptions.installAgent && !opts.noAgent
					scvmmOpts.cloudConfigUser = initOptions.cloudConfigUser
					scvmmOpts.cloudConfigMeta = initOptions.cloudConfigMeta
					scvmmOpts.cloudConfigBytes = initOptions.cloudConfigBytes
					scvmmOpts.cloudConfigNetwork = initOptions.cloudConfigNetwork
					// check: if license is required
					/*if(initOptions.licenseApplied) {
						opts.licenseApplied = true
					}*/
					opts.unattendCustomized = initOptions.unattendCustomized
					server.cloudConfigUser = scvmmOpts.cloudConfigUser
					server.cloudConfigMeta = scvmmOpts.cloudConfigMeta
					server.cloudConfigNetwork = scvmmOpts.cloudConfigNetwork
					//opts.server.save(flush:true)
					server = saveAndGetMorpheusServer(server, true)
				} else {
					// check:
					//opts.createUserList = opts.userConfig.createUsers
				}
				// If cloning.. gotta stop it first
				if(containerConfig.cloneContainerId) {
					Workload cloneContainer = context.services.workload.get(containerConfig.cloneContainerId.toLong())
					scvmmOpts.cloneContainerId = cloneContainer.id
					scvmmOpts.cloneVMId = cloneContainer.server.externalId
					if(cloneContainer.status == Workload.Status.running) {
						//instanceTaskService.runShutdownTasks(cloneContainer.instance, opts.userId)
						stopWorkload(cloneContainer)
						scvmmOpts.startClonedVM = true
					}
					log.debug "Handling startup of the original VM"
					def cloneBaseOpts = [:]
					cloneBaseOpts.cloudInitIsoNeeded = (cloneContainer.server.sourceImage && cloneContainer.server.sourceImage.isCloudInit && cloneContainer.server.serverOs?.platform != 'windows')
					if(cloneBaseOpts.cloudInitIsoNeeded) {
						def initOptions = constructCloudInitOptions(cloneContainer, opts.installAgent, scvmmOpts.platform, opts.userConfig, virtualImage, scvmmOpts.networkConfig, scvmmOpts.licenses, scvmmOpts)
						def clonedScvmmOpts = apiService.getScvmmZoneOpts(context, cloud)
						clonedScvmmOpts += apiService.getScvmmControllerOpts(cloud, controllerNode)
						clonedScvmmOpts += getScvmmContainerOpts(cloneContainer)
						opts.installAgent = initOptions.installAgent && !opts.noAgent
						clonedScvmmOpts.installAgent = opts.installAgent
						cloneBaseOpts.imageFolderName = clonedScvmmOpts.serverFolder
						cloneBaseOpts.diskFolder = "${clonedScvmmOpts.diskRoot}\\${cloneBaseOpts.imageFolderName}"
						cloneBaseOpts.cloudConfigBytes = initOptions.cloudConfigBytes
						cloneBaseOpts.cloudConfigNetwork = initOptions.cloudConfigNetwork
						cloneBaseOpts.clonedScvmmOpts = clonedScvmmOpts
						//cloneBaseOpts.clonedScvmmOpts.scvmmProvisionService = getService('scvmmProvisionService')
						cloneBaseOpts.clonedScvmmOpts.controllerServerId = controllerNode.id
						// check: if license is required?
						/*if(initOptions.licenseApplied) {
							opts.licenseApplied = true
						}*/
						opts.unattendCustomized = initOptions.unattendCustomized
					} else {
						if(scvmmOpts.isSysprep && !opts.installAgent && !opts.noAgent) {
							// Need Morpheus to install the agent.. can't do it over unattend on clone cause SCVMM commands don't seem to support passing AnswerFile
							opts.installAgent = true
						}
					}
					scvmmOpts.cloneBaseOpts = cloneBaseOpts
				}
				// check: for inprogress status
				//rtn.inProgress = true
				log.debug("create server: ${scvmmOpts}")
				log.info ("Ray :: runWorkload: scvmmOpts5: ${scvmmOpts}")
				def createResults = apiService.createServer(scvmmOpts)
				log.info ("Ray :: runWorkload: createResults: ${createResults}")
				scvmmOpts.deleteDvdOnComplete = createResults.deleteDvdOnComplete
				log.info ("Ray :: runWorkload: createResults.deleteDvdOnComplete: ${createResults.deleteDvdOnComplete}")
				log.info ("Ray :: runWorkload: createResults.success : ${createResults.success }")
				if (createResults.success == true) {
					def checkReadyResults = apiService.checkServerReady([waitForIp: opts.skipNetworkWait ? false : true] + scvmmOpts, createResults.server.id)
					log.info ("Ray :: runWorkload: checkReadyResults: ${checkReadyResults}")
					if (checkReadyResults.success) {
						server.externalIp = checkReadyResults.server.ipAddress
						server.powerState = ComputeServer.PowerState.on
						server = saveAndGetMorpheusServer(server, true)
					} else {
						log.error "Failed to obtain ip address for server, ${checkReadyResults}"
						throw new Exception("Failed to obtain ip address for server")
					}
					if(scvmmOpts.deleteDvdOnComplete?.removeIsoFromDvd) {
						apiService.setCdrom(scvmmOpts)
						if(scvmmOpts.deleteDvdOnComplete?.deleteIso) {
							apiService.deleteIso(scvmmOpts, scvmmOpts.deleteDvdOnComplete.deleteIso)
						}
					}

					if (scvmmOpts.cloneVMId && scvmmOpts.cloneContainerId) {
						// Restart the VM being cloned
						if (scvmmOpts.startClonedVM) {
							log.debug "Handling startup of the original VM"
							//task {
							//ComputeServer.withNewSession {
							Workload cloneContainer = context.services.workload.get(containerConfig.cloneContainerId?.toLong())
							if (cloneContainer && cloneContainer.status != Workload.Status.running.toString()) {
								log.debug "stopping/starting original VM: ${scvmmOpts.cloneVMId}"
								apiService.startServer([async: true] + scvmmOpts.cloneBaseOpts.clonedScvmmOpts, scvmmOpts.cloneVMId)
								Workload savedContainer = context.services.workload.find(new DataQuery().withFilter(cloneContainer.server?.id))
								if (savedContainer) {
									savedContainer.userStatus = Workload.Status.running.toString()
									context.services.workload.save(savedContainer)
								}
								ComputeServer savedServer = context.services.computeServer.get(cloneContainer.server?.id)
								if (savedServer) {
									context.async.computeServer.updatePowerState(savedServer.id, ComputeServer.PowerState.on)
								}
							}
						}
					}

					node = context.services.computeServer.get(nodeId)
					log.info ("Ray :: runWorkload: createResults.server: ${createResults.server}")
					if (createResults.server) {
						server.externalId = createResults.server.id
						server.internalId = createResults.server.VMId
						server.parentServer = node
						if(server.cloud.getConfigProperty('enableVnc')) {
							//credentials
							server.consoleHost = server.parentServer?.name
							server.consoleType = 'vmrdp'
							server.sshUsername = server.cloud.credentialData?.username ?: server.cloud.getConfigProperty('username')
							server.consolePassword = server.cloud.credentialData?.password ?: server.cloud.getConfigProperty('password')
							server.consolePort = 2179
						}
						def serverDisks = createResults.server.disks
						if (serverDisks && server.volumes) {
							storageVolumes = server.volumes
							rootVolume = storageVolumes.find { it.rootVolume == true }
							rootVolume.externalId = serverDisks.diskMetaData[serverDisks.osDisk?.externalId]?.VhdID  // Fix up the externalId.. initially set to the VirtualDiskDrive ID.. now setting to VirtualHardDisk ID
							rootVolume.datastore = loadDatastoreForVolume(cloud, serverDisks.diskMetaData[rootVolume.externalId]?.HostVolumeId, serverDisks.diskMetaData[rootVolume.externalId]?.FileShareId, serverDisks.diskMetaData[rootVolume.externalId]?.PartitionUniqueId) ?: rootVolume.datastore
							storageVolumes.each { storageVolume ->
								def dataDisk = serverDisks.dataDisks.find { it.id == storageVolume.id }
								if (dataDisk) {
									def newExternalId = serverDisks.diskMetaData[dataDisk.externalId]?.VhdID
									if(newExternalId) {
										storageVolume.externalId = newExternalId
									}
									// Ensure the datastore is set
									storageVolume.datastore = loadDatastoreForVolume(cloud, serverDisks.diskMetaData[storageVolume.externalId]?.HostVolumeId, serverDisks.diskMetaData[storageVolume.externalId]?.FileShareId , serverDisks.diskMetaData[storageVolume.externalId]?.PartitionUniqueId) ?: storageVolume.datastore
								}
							}
						}

						def serverDetails = apiService.getServerDetails(scvmmOpts, server.externalId)
						if (serverDetails.success == true) {
							log.info("serverDetail: ${serverDetails}")
							applyComputeServerNetworkIp(server, serverDetails.server?.ipAddress, serverDetails.server?.ipAddress, 0, null)
							server.osDevice = '/dev/sda'
							server.dataDevice = '/dev/sda'
							server.lvmEnabled = false
							server.sshHost = server.internalIp
							server.managed = true
							server.capacityInfo = new ComputeCapacityInfo(maxCores: scvmmOpts.maxCores, maxMemory: scvmmOpts.maxMemory, maxStorage: scvmmOpts.maxTotalStorage)
							server.status = 'provisioned'
							context.async.computeServer.save(server).blockingGet()
							provisionResponse.success = true
							log.debug("provisionResponse.success: ${provisionResponse.success}")
						} else {
							server.statusMessage = 'Failed to run server'
							context.async.computeServer.save(server).blockingGet()
							provisionResponse.success = false
						}

					} else {
						if (createResults.server?.externalId) {
							// we did create a vm though so we need to bind it to the server
							server.externalId = createResults.server.externalId
						}
						server.statusMessage = 'Failed to create server'
						context.async.computeServer.save(server).blockingGet()
						provisionResponse.success = false
					}

				}
			} else {
				server.statusMessage = 'Failed to upload image'
				context.async.computeServer.save(server).blockingGet()
			}
			provisionResponse.noAgent = opts.noAgent ?: false
			log.info ("Ray :: runWorkload: provisionResponse.success: ${provisionResponse.success}")
			if (provisionResponse.success != true) {
				return new ServiceResponse(success: false, msg: provisionResponse.message ?: 'vm config error', error: provisionResponse.message, data: provisionResponse)
			} else {
				return new ServiceResponse<ProvisionResponse>(success: true, data: provisionResponse)
			}
		} catch (e) {
			log.error("initializeServer error:${e}", e)
			provisionResponse.setError(e.message)
			return new ServiceResponse(success: false, msg: e.message, error: e.message, data: provisionResponse)
		}
		log.info ("Ray :: runWorkload: provisionResponse.success1: ${provisionResponse.success}")
	}

	private applyComputeServerNetworkIp(ComputeServer server, privateIp, publicIp, index, macAddress) {
		ComputeServerInterface netInterface
		if (privateIp) {
			privateIp = privateIp?.toString().contains("\n") ? privateIp.toString().replace("\n", "") : privateIp.toString()
			def newInterface = false
			server.internalIp = privateIp
			server.sshHost = privateIp
			server.macAddress = macAddress
			log.debug("Setting private ip on server:${server.sshHost}")
			netInterface = server.interfaces?.find { it.ipAddress == privateIp }

			if (netInterface == null) {
				if (index == 0)
					netInterface = server.interfaces?.find { it.primaryInterface == true }
				if (netInterface == null)
					netInterface = server.interfaces?.find { it.displayOrder == index }
				if (netInterface == null)
					netInterface = server.interfaces?.size() > index ? server.interfaces[index] : null
			}
			if (netInterface == null) {
				def interfaceName = server.sourceImage?.interfaceName ?: 'eth0'
				netInterface = new ComputeServerInterface(
						name: interfaceName,
						ipAddress: privateIp,
						primaryInterface: true,
						displayOrder: (server.interfaces?.size() ?: 0) + 1
						//externalId		: networkOpts.externalId
				)
				netInterface.addresses += new NetAddress(type: NetAddress.AddressType.IPV4, address: privateIp)
				newInterface = true
			} else {
				netInterface.ipAddress = privateIp
			}
			if (publicIp) {
				publicIp = publicIp?.toString().contains("\n") ? publicIp.toString().replace("\n", "") : publicIp.toString()
				netInterface.publicIpAddress = publicIp
				server.externalIp = publicIp
			}
			netInterface.macAddress = macAddress
			if (newInterface == true)
				context.async.computeServer.computeServerInterface.create([netInterface], server).blockingGet()
			else
				context.async.computeServer.computeServerInterface.save([netInterface]).blockingGet()
		}
		saveAndGetMorpheusServer(server, true)
		return netInterface
	}

	def loadDatastoreForVolume(Cloud cloud, hostVolumeId=null, fileShareId=null, partitionUniqueId=null) {
		log.debug "loadDatastoreForVolume: ${hostVolumeId}, ${fileShareId}"
		if(hostVolumeId) {
			StorageVolume storageVolume = context.services.storageVolume.find (new DataQuery()
					.withFilter('internalId', hostVolumeId).withFilter('datastore.refType', 'ComputeZone')
					.withFilter('refId', cloud.id))
			def ds = storageVolume?.datastore
			if(!ds && partitionUniqueId) {
				storageVolume = context.services.storageVolume.find (new DataQuery()
						.withFilter('externalId', partitionUniqueId).withFilter('datastore.refType', 'ComputeZone')
						.withFilter('datastore.refId', cloud.id))
				ds = storageVolume?.datastore
			}
			return ds
		} else if(fileShareId) {
			Datastore datastore = context.services.cloud.datastore.find(new DataQuery().withFilter('externalId', fileShareId)
					.withFilter('refType', 'ComputeZone')
					.withFilter('refId', zone.id))
			return datastore
		}
		null
	}

	def getResourcePoolFromIdString(resourcePoolId, Long accountId, Long siteId, Long zoneId, Boolean ignoreUpdate=false) {
		def resourcePool
		if(resourcePoolId instanceof String && resourcePoolId.startsWith('pool-')) {
			resourcePool = context.services.cloud.pool.get (resourcePoolId?.substring(5))
		} else if (resourcePoolId instanceof String && resourcePoolId.startsWith('poolGroup-')) {
			def poolGroup = ComputeZonePoolGroup.get(resourcePoolId.substring(10))// check:
			if(poolGroup) {
				resourcePool = pickPoolGroupResourcePool(poolGroup, accountId, siteId, zoneId, ignoreUpdate) // check:
			}
		} else {
			try {
				resourcePool = context.services.cloud.pool.get(resourcePoolId?.toLong())
			} catch(NumberFormatException nEx) {
				resourcePool = context.services.cloud.pool.find(new DataQuery().withFilter('refType', 'ComputeZone')
						.withFilter('refId', zoneId).withFilter('externalId', resourcePoolId))
			}
		}
		return resourcePool
	}

	private constructCloudInitOptions(Workload container, workloadRequest, installAgent, platform, userConfig, VirtualImage virtualImage, networkConfig, licenses, scvmmOpts) {
		log.debug("constructCloudInitOptions: ${container}, ${installAgent}, ${platform}, ${userConfig}")
		def rtn = [:]
		ComputeServer server = container.server
		Cloud zone = server.cloud
		def containerConfig = container.getConfigMap()
		//def applianceServerUrl = applianceService.getApplianceUrl(zone)
		/*def cloudConfigOpts = buildCloudConfigOpts(zone, container.server, installAgent, [doPing:true, sendIp:true, apiKey:server.apiKey,
																						  applianceIp:MorpheusUtils.getUrlHost(applianceServerUrl), hostname:server.getExternalHostname(), applianceUrl:applianceServerUrl,
																						  hostname:server.getExternalHostname(), hosts:server.getExternalHostname(), disableCloudInit:true, timezone: containerConfig.timezone])*/ //check: cloudConfigOpts
		def cloudConfigOpts = context.services.provision.buildCloudConfigOptions(zone, server, installAgent, scvmmOpts)

		// Special handling for install agent on SCVMM (determine if we are installing via cloud init)
		cloudConfigOpts.installAgent = false
		if(installAgent == true) {
			if(zone.agentMode == 'cloudInit' && (platform != 'windows' || scvmmOpts.isSysprep)) {
				cloudConfigOpts.installAgent = true
			}
		}
		rtn.installAgent = installAgent && (cloudConfigOpts.installAgent != true)  // If cloudConfigOpts.installAgent == true, it means we are installing the agent via cloud config.. so do NOT install is via morpheus
		cloudConfigOpts.licenses = licenses
		context.services.provision.buildCloudNetworkData(PlatformType.valueOf(platform), cloudConfigOpts)
		rtn.cloudConfigUser = workloadRequest?.cloudConfigUser ?: null
		rtn.cloudConfigMeta = workloadRequest?.cloudConfigMeta ?: null
		rtn.cloudConfigNetwork = workloadRequest?.cloudConfigNetwork ?: null
		// check: if license required ?
		/*if(cloudConfigOpts.licenseApplied) {
			rtn.licenseApplied = true
		}*/
		rtn.unattendCustomized = cloudConfigOpts.unattendCustomized
		rtn.cloudConfigUnattend = context.services.provision.buildCloudUserData(PlatformType.valueOf(platform), workloadRequest.usersConfiguration, cloudConfigOpts)
		def isoBuffer = context.services.provision.buildIsoOutputStream(virtualImage.isSysprep, PlatformType.valueOf(platform), rtn.cloudConfigMeta, rtn.cloudConfigUnattend, rtn.cloudConfigNetwork)
		rtn.cloudConfigBytes = isoBuffer
		return rtn
	}

	def additionalTemplateDisksConfig(Workload workload, scvmmOpts) {
		// Determine what additional disks need to be added after provisioning
		def additionalTemplateDisks = []
		def dataDisks = getContainerDataDiskList(workload)
		log.debug "dataDisks: ${dataDisks} ${dataDisks?.size()}"
		def diskExternalIdMappings = scvmmOpts.diskExternalIdMappings
		def additionalDisksRequired =  dataDisks?.size() + 1 > diskExternalIdMappings?.size()
		def busNumber = '0'
		if(additionalDisksRequired) {
			def diskCounter = diskExternalIdMappings.size()
			dataDisks?.eachWithIndex { StorageVolume sv, index ->
				if(index + 2 > diskExternalIdMappings.size()){  // add 1 for the root disk and then 1 for 0 based
					additionalTemplateDisks << [idx: index + 1, diskCounter: diskCounter, diskSize: sv.maxStorage, busNumber: busNumber ]
					diskCounter++
				}
			}
		}
		log.debug "returning additionalTemplateDisks ${additionalTemplateDisks}"
		additionalTemplateDisks
	}

	def getDiskExternalIds(VirtualImage virtualImage, cloud) {
		// The mapping of volumes is off of the VirtualImageLocation
		VirtualImageLocation location = getVirtualImageLocation(virtualImage, cloud)

		def rtn = []
		def rootVolume = location.volumes.find { it.rootVolume }
		rtn << [rootVolume: true, externalId: rootVolume.externalId, idx: 0]
		location.volumes?.eachWithIndex { vol, index ->
			if(!vol.rootVolume) {
				rtn << [rootVolume: false, externalId: vol.externalId, idx: 1 + index]
			}
		}
		rtn
	}

	private getVirtualImageLocation(VirtualImage virtualImage, cloud) {
		def location = context.services.virtualImage.location.find(new DataQuery().withFilters(
				new DataFilter('virtualImage.id', virtualImage.id),
				new DataOrFilter(
						new DataAndFilter(
								new DataFilter('refType','ComputeZone'),
								new DataFilter('refId',cloud.id)
						),
						new DataAndFilter(
								new DataFilter('owner.id', cloud.owner.id),
								new DataFilter('imageRegion', cloud.regionCode)
						)
				)
		))
		return location
	}

	def getHostAndDatastore(cloud, account, clusterId, hostId, Datastore datastore, datastoreOption, size, siteId=null, maxMemory) {
		log.debug "clusterId: ${clusterId}, hostId: ${hostId}, datastore: ${datastore}, datastoreOption: ${datastoreOption}, size: ${size}, siteId: ${siteId}, maxMemory ${maxMemory}"
		ComputeServer node
		def volumePath
		def highlyAvailable = false

		// If clusterId (resourcePool) is not specified AND host not specified AND datastore is 'auto',
		// then we are just deploying to the cloud (so... can not select the datastore, nor host)
		clusterId = clusterId && clusterId != 'null' ? clusterId : null
		hostId = hostId && hostId.toString().trim() != '' ? hostId : null
		def zoneHasCloud = cloud.regionCode != null && cloud.regionCode != ''
		if(zoneHasCloud && !clusterId && !hostId && !datastore && (datastoreOption == 'auto' || !datastoreOption)) {
			return [node, datastore, volumePath, highlyAvailable]
		}
		// If host specified by the user, then use it
		node = hostId ? context.services.computeServer.get(hostId.toLong()) : null
		if(!datastore) {
			def datastoreIds = context.services.resourcePermission.listAccessibleResources(account.id, ResourcePermission.ResourceType.Datastore, siteId?.toLong(), null)
			def hasFilteredDatastores = false
			// If hostId specifed.. gather all the datastoreIds for the host via storagevolumes
			if(hostId) {
				hasFilteredDatastores = true
				def scopedDatastoreIds = context.services.computeServer.list(new DataQuery()
						.withFilter('id', hostId.toLong()).withJoin('volumes.datastore'))
						.collect{it.volumes.collect{it.datastore.id}}.flatten().unique()
				datastoreIds = scopedDatastoreIds
			}
			def dsQuery = new DataQuery().withFilter('refType', 'ComputeZone')
					.withFilter('refId', cloud.id).withFilter('type', 'generic')
					.withFilter('online', true).withFilter('active', true)
					.withFilter('freeSpace', size)

			if(hasFilteredDatastores) {
				dsQuery = dsQuery.withFilter('id', 'in', datastoreIds)
				dsQuery = new DataQuery(dsQuery).withFilters(
						new DataOrFilter(
								new DataFilter('visibility', 'public'),
								new DataFilter('id', account.id)
						)
				)
			} else {
				dsQuery = new DataQuery(dsQuery).withFilters(
						new DataOrFilter(
								new DataFilter('id', 'in', datastoreIds),
								new DataOrFilter(
										new DataFilter('visibility', 'public'),
										new DataFilter('owner.id', account.id)
								)
						)
				)
			}
			if(clusterId) {
				if(clusterId.toString().isNumber()) {
					dsQuery = dsQuery.withFilter('id', clusterId.toLong())
				} else {
					dsQuery = dsQuery.withFilter('externalId', clusterId)
				}
			}
			def dsList = context.services.cloud.datastore.list(dsQuery.withSort('freeSpace', DataQuery.SortOrder.desc))
			// Return the first one
			if(dsList.size() > 0) {
				datastore = dsList[0]
			}
		}

		if(!node && datastore) {
			// We've grabbed a datastore.. now pick a host that has this datastore
			def nodes = context.services.computeServer.list(new DataQuery()
					.withFilter('zone.id', cloud.id)
					.withFilter('enabled', true).withFilter('computeServerType.code','scvmmHypervisor')
					.withFilter('volumes.datastore.id', datastore?.id).withFilter('powerState', ComputeServer.PowerState.on)
			)
			nodes = nodes.findAll { it.capacityInfo?.maxMemory - it.capacityInfo?.usedMemory > maxMemory }?.sort { -(it.capacityInfo?.maxMemory - it.capacityInfo?.usedMemory) }
			node = nodes?.size() > 0 ? nodes.first() : null
		}

		if(!zoneHasCloud && (!node || !datastore)) {
			// Need a node and a datastore for non-cloud scoped zones
			throw new Exception('Unable to obtain datastore and host for options selected')
		}

		// Get the volumePath (used during provisioning to tell SCVMM where to place the disks)
		volumePath = getVolumePathForDatastore(datastore)

		// Highly Available (in the Failover Cluster Manager) if we are in a cluster and the datastore is a shared volume
		if(clusterId && datastore?.zonePool) {  // datastore found above MUST be part of a shared volume because it has a zonepool
			highlyAvailable = true
		}

		return [node, datastore, volumePath, highlyAvailable]
	}

	def getVolumePathForDatastore(Datastore datastore) {
		log.debug "getVolumePathForDatastore: ${datastore}"
		def volumePath
		if(datastore) {
			StorageVolume storageVolume = context.services.storageVolume.find(new DataQuery().withFilter('datastore.id', datastore?.id)
					.withFilter('volumePath', '!=', null))
			volumePath = storageVolume?.volumePath
		}
		log.debug "volumePath: ${volumePath}"
		return volumePath
	}



	private setDynamicMemory(Map targetMap, ServicePlan plan) {
		log.debug "setDynamicMemory: ${plan}"
		if (plan) {
			def ranges = plan.getConfigProperty('ranges') ?: [:]
			targetMap.minDynamicMemory = ranges.minMemory ?: null
			targetMap.maxDynamicMemory = ranges.maxMemory ?: null
		}
	}

	protected ComputeServer saveAndGetMorpheusServer(ComputeServer server, Boolean fullReload = false) {
		def saveResult = context.async.computeServer.bulkSave([server]).blockingGet()
		def updatedServer
		if (saveResult.success == true) {
			if (fullReload) {
				updatedServer = getMorpheusServer(server.id)
			} else {
				updatedServer = saveResult.persistedItems.find { it.id == server.id }
			}
		} else {
			updatedServer = saveResult.failedItems.find { it.id == server.id }
			log.warn("Error saving server: ${server?.id}")
		}
		return updatedServer ?: server
	}

	protected ComputeServer getMorpheusServer(Long id) {
		return context.services.computeServer.find(
				new DataQuery().withFilter("id", id).withJoin("interfaces.network")
		)
	}

	/**
	 * This method is called after successful completion of runWorkload and provides an opportunity to perform some final
	 * actions during the provisioning process. For example, ejected CDs, cleanup actions, etc
	 * @param workload the Workload object that has been provisioned
	 * @return Response from the API
	 */
	@Override
	ServiceResponse finalizeWorkload(Workload workload) {
		return ServiceResponse.success()
	}

	/**
	 * Issues the remote calls necessary top stop a workload element from running.
	 * @param workload the Workload we want to shut down
	 * @return Response from API
	 */
	@Override
	ServiceResponse stopWorkload(Workload workload) {
		def rtn = ServiceResponse.prepare()
		try {
			if (workload.server?.externalId) {
				def scvmmOpts = getAllScvmmOpts(workload)
				def results = apiService.stopServer(scvmmOpts, scvmmOpts.vmId)
				if (results.success == true) {
					rtn.success = true
				}
			} else {
				rtn.success = false
				rtn.msg = 'vm not found'
			}
		} catch (e) {
			log.error("stopWorkload error: ${e}", e)
			rtn.msg = e.message
		}
		return rtn
	}

	def pickScvmmController(cloud) {
		// Could be using a shared controller
		def sharedControllerId = cloud.getConfigProperty('sharedController')
		def sharedController = sharedControllerId ? context.services.computeServer.get(sharedControllerId?.toLong()) : null
		if(sharedController) {
			return sharedController
		}
		def rtn = context.services.computeServer.find(new DataQuery()
				.withFilter('cloud.id', cloud.id)
				.withFilter('computeServerType.code', 'scvmmController')
				.withJoin('computeServerType'))
		if(rtn == null) {
			//old zone with wrong type
			rtn = context.services.computeServer.find(new DataQuery()
					.withFilter('cloud.id', cloud.id)
					.withFilter('computeServerType.code', 'scvmmHypervisor')
					.withJoin('computeServerType'))
			if(rtn == null)
				rtn = context.services.computeServer.find(new DataQuery()
						.withFilter('cloud.id', cloud.id)
						.withFilter('serverType', 'hypervisor'))
			//if we have tye type
			if(rtn) {
				rtn.computeServerType = new ComputeServerType(code: 'scvmmController')
				context.services.computeServer.save(rtn)
			}
		}
		return rtn
	}

	def getContainerRootSize(container) {
		def rtn
		def rootDisk = getContainerRootDisk(container)
		if (rootDisk)
			rtn = rootDisk.maxStorage
		else
			rtn = container.maxStorage ?: container.instance.plan.maxStorage
		return rtn
	}

	def getContainerRootDisk(container) {
		def rtn = container.server?.volumes?.find { it.rootVolume == true }
		return rtn
	}

	def getContainerVolumeSize(container) {
		def rtn = container.maxStorage ?: container.instance.plan.maxStorage
		if (container.server?.volumes?.size() > 0) {
			def newMaxStorage = container.server.volumes.sum { it.maxStorage ?: 0 }
			if (newMaxStorage > rtn)
				rtn = newMaxStorage
		}
		return rtn
	}

	static getContainerDataDiskList(container) {
		def rtn = container.server?.volumes?.findAll { it.rootVolume == false }?.sort { it.id }
		return rtn
	}

	def getScvmmContainerOpts(container) {
		def serverConfig = container.server.getConfigMap()
		def containerConfig = container.getConfigMap()
		def network = context.services.cloud.network.get(containerConfig.networkId?.toLong())
		def serverFolder = "morpheus\\morpheus_server_${container.server.id}"
		def maxMemory = container.maxMemory ?: container.instance.plan.maxMemory
		def maxCpu = container.maxCpu ?: container.instance.plan?.maxCpu ?: 1
		def maxCores = container.maxCores ?: container.instance.plan.maxCores ?: 1
		def maxStorage = getContainerRootSize(container)
		def maxTotalStorage = getContainerVolumeSize(container)
		def dataDisks = getContainerDataDiskList(container)
		def resourcePool = container.server?.resourcePool ? container.server?.resourcePool : null
		def platform = (container.server.serverOs?.platform == 'windows' || container.server.osType == 'windows') ? 'windows' : 'linux'
		return [config:serverConfig, vmId:container.server.externalId, name:container.server.externalId, server:container.server, serverId:container.server?.id,
				memory:maxMemory, maxCpu:maxCpu, maxCores:maxCores, serverFolder:serverFolder, hostname:container.hostname,
				network:network, networkId:network?.id, platform:platform, externalId:container.server.externalId, networkType:containerConfig.networkType,
				containerConfig:containerConfig, resourcePool:resourcePool?.externalId, hostId:containerConfig.hostId,
				osDiskSize:maxStorage, maxTotalStorage:maxTotalStorage, dataDisks:dataDisks,
				scvmmCapabilityProfile:(containerConfig.scvmmCapabilityProfile?.toString() != '-1' ? containerConfig.scvmmCapabilityProfile : null),
				accountId:container.account?.id
		]
	}

	def getAllScvmmOpts(workload) {
		def controllerNode = pickScvmmController(workload.server.cloud)
		def rtn = apiService.getScvmmCloudOpts(context, workload.server.cloud, controllerNode)
		rtn += apiService.getScvmmControllerOpts(workload.server.cloud, controllerNode)
		rtn += getScvmmContainerOpts(workload)
		return rtn
	}

	/**
	 * Issues the remote calls necessary to start a workload element for running.
	 * @param workload the Workload we want to start up.
	 * @return Response from API
	 */
	@Override
	ServiceResponse startWorkload(Workload workload) {
		log.debug ("startWorkload: ${workload?.id}")
		def rtn = ServiceResponse.prepare()
		try {
			if (workload.server?.externalId) {
				def scvmmOpts = getAllScvmmOpts(workload)
				def results = apiService.startServer(scvmmOpts, scvmmOpts.vmId)
				if (results.success == true) {
					rtn.success = true
				}
			} else {
				rtn.success = false
				rtn.msg = 'vm not found'
			}
		} catch (e) {
			log.error("startWorkload error: ${e}", e)
			rtn.msg = e.message
		}
		return rtn
	}

	/**
	 * Issues the remote calls to restart a workload element. In some cases this is just a simple alias call to do a stop/start,
	 * however, in some cases cloud providers provide a direct restart call which may be preferred for speed.
	 * @param workload the Workload we want to restart.
	 * @return Response from API
	 */
	@Override
	ServiceResponse restartWorkload(Workload workload) {
		// Generally a call to stopWorkLoad() and then startWorkload()
		return ServiceResponse.success()
	}

	/**
	 * This is the key method called to destroy / remove a workload. This should make the remote calls necessary to remove any assets
	 * associated with the workload.
	 * @param workload to remove
	 * @param opts map of options
	 * @return Response from API
	 */
	@Override
	ServiceResponse removeWorkload(Workload workload, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Method called after a successful call to runWorkload to obtain the details of the ComputeServer. Implementations
	 * should not return until the server is successfully created in the underlying cloud or the server fails to
	 * create.
	 * @param server to check status
	 * @return Response from API. The publicIp and privateIp set on the WorkloadResponse will be utilized to update the ComputeServer
	 */
	@Override
	ServiceResponse<ProvisionResponse> getServerDetails(ComputeServer server) {
		return new ServiceResponse<ProvisionResponse>(true, null, null, new ProvisionResponse(success:true))
	}

	/**
	 * Method called before runWorkload to allow implementers to create resources required before runWorkload is called
	 * @param workload that will be provisioned
	 * @param opts additional options
	 * @return Response from API
	 */
	@Override
	ServiceResponse createWorkloadResources(Workload workload, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Stop the server
	 * @param computeServer to stop
	 * @return Response from API
	 */
	@Override
	ServiceResponse stopServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	/**
	 * Start the server
	 * @param computeServer to start
	 * @return Response from API
	 */
	@Override
	ServiceResponse startServer(ComputeServer computeServer) {
		return ServiceResponse.success()
	}

	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 *
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
		return PROVIDER_CODE
	}

	/**
	 * Provides the provider name for reference when adding to the Morpheus Orchestrator
	 * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
	 *
	 * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
	 */
	@Override
	String getName() {
		return 'SCVMM Provisioning'
	}

	/**
	 * Determines if this provision type has datastores that can be selected or not.
	 * @return Boolean representation of whether or not this provision type has datastores
	 */
	@Override
	Boolean hasDatastores() {
		return true
	}

	/**
	 * Determines if this provision type has networks that can be selected or not.
	 * @return Boolean representation of whether or not this provision type has networks
	 */
	@Override
	Boolean hasNetworks() {
		return true
	}

	/**
	 * Determines if this provision type has ComputeZonePools that can be selected or not.
	 * @return Boolean representation of whether or not this provision type has ComputeZonePools
	 */
	@Override
	Boolean hasComputeZonePools() {
		return true
	}

	/**
	 * Indicates if volumes may be added during provisioning
	 * @return Boolean
	 */
	@Override
	Boolean canAddVolumes() {
		return true
	}

	/**
	 * Indicates if the root volume may be customized during provisioning. For example, the size changed
	 * @return Boolean
	 */
	@Override
	Boolean canCustomizeRootVolume() {
		return true
	}

	/**
	 * Indicates if this provider supports node types
	 * @return Boolean
	 */
	@Override
	Boolean hasNodeTypes() {
		return true
	}

	/**
	 * The node format for this provider
	 * valid options are: vm, server, container
	 * @return String
	 */
	@Override
	String getNodeFormat() {
		return 'vm'
	}

	/**
	 * Indicates if this provider supports LVM instances
	 * @return Boolean
	 */
	@Override
	Boolean lvmSupported() {
		return true
	}

	/**
	 * Indicates if this provider should set a server type different from its code, e.g. "service" or "vm"
	 * @return String
	 */
	@Override
	String serverType() {
		return 'vm'
	}

	/**
	 * Returns the host type that is to be provisioned
	 * @return HostType
	 */
	@Override
	HostType getHostType() {
		return HostType.vm
	}

	/**
	 * Custom service plans can be created for this provider
	 * @return Boolean
	 */
	@Override
	Boolean supportsCustomServicePlans() {
		return true
	}

	/**
	 * Does this provision type allow more than one instance on a box
	 * @return
	 */
	@Override
	Boolean multiTenant() {
		return false
	}

	/**
	 * Can control firewall rules on the instance
	 * @return
	 */
	@Override
	Boolean aclEnabled() {
		return false
	}
}
