package com.morpheusdata.scvmm

import com.morpheusdata.PrepareHostResponse
import com.morpheusdata.core.AbstractProvisionProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataAndFilter
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.HostProvisionProvider
import com.morpheusdata.core.providers.ProvisionProvider
import com.morpheusdata.core.providers.WorkloadProvisionProvider
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.model.*
import com.morpheusdata.model.provisioning.HostRequest
import com.morpheusdata.model.provisioning.WorkloadRequest
import com.morpheusdata.request.ResizeRequest
import com.morpheusdata.response.InitializeHypervisorResponse
import com.morpheusdata.response.PrepareWorkloadResponse
import com.morpheusdata.response.ProvisionResponse
import com.morpheusdata.response.ServiceResponse
import com.morpheusdata.scvmm.helper.morpheus.types.StorageVolumeTypeHelper
import groovy.util.logging.Slf4j

@Slf4j
class ScvmmProvisionProvider extends AbstractProvisionProvider implements WorkloadProvisionProvider, HostProvisionProvider, ProvisionProvider.HypervisorProvisionFacet, HostProvisionProvider.ResizeFacet, WorkloadProvisionProvider.ResizeFacet, ProvisionProvider.BlockDeviceNameFacet {
    public static final String PROVIDER_CODE = 'scvmm.provision'
    public static final String PROVISION_TYPE_CODE = 'scvmm'
    public static final diskNames = ['sda', 'sdb', 'sdc', 'sdd', 'sde', 'sdf', 'sdg', 'sdh', 'sdi', 'sdj', 'sdk', 'sdl']

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
            if (sharedController) {
                // No controller needed.. we are sharing another cloud's controller
                rtn.success = true
            } else {
                def opts = apiService.getScvmmZoneOpts(context, cloud)
                opts += apiService.getScvmmControllerOpts(cloud, server)
                def serverInfo = apiService.getScvmmServerInfo(opts)
				String versionCode
				versionCode = apiService.extractWindowsServerVersion(serverInfo.osName)
                log.debug("serverInfo: ${serverInfo}")
                if (serverInfo.success == true && serverInfo.hostname) {
                    server.hostname = serverInfo.hostname
                }
                def maxStorage = serverInfo?.disks ? serverInfo?.disks.toLong() : 0
                def maxMemory = serverInfo?.memory ? serverInfo?.memory.toLong() : 0
                def maxCores = 1

				// Create proper OS code format
				rtn.data.serverOs = new OsType(code: "windows.server.${versionCode}")
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
        log.debug("prepare workload scvmm")
        ServiceResponse<PrepareWorkloadResponse> resp = new ServiceResponse<PrepareWorkloadResponse>(
                true, // successful
                '', // no message
                null, // no errors
                new PrepareWorkloadResponse(workload: workload) // adding the workload to the response for convenience
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
        return new Icon(path: 'provision-circular.svg', darkPath: 'provision-circular-dark.svg')
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
                fieldGroup: 'Advanced Options',
                displayOrder: 4,
                required: false,
                enabled: true,
                editable: false,
                global: false,
                placeHolder: null,
                helpBlock: 'Skipping Agent installation will result in a lack of logging and guest operating system statistics. Automation scripts may also be adversely affected.',
                defaultValue: null,
                custom: false,
                fieldClass: null
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
                fieldGroup: 'Options',
                displayOrder: 11,
                required: true,
                enabled: true,
                editable: true,
                global: false,
                placeHolder: null,
                helpBlock: '',
                defaultValue: null,
                custom: false,
                fieldClass: null,
                optionSource: 'scvmmCapabilityProfile',
                optionSourceType: 'scvmm'
        )
        options << new OptionType(
                code: 'provisionType.scvmm.host',
                inputType: OptionType.InputType.SELECT,
                name: 'host',
                category: 'provisionType.scvmm',
                optionSourceType: 'scvmm',
                fieldName: 'hostId',
                fieldCode: 'gomorpheus.optiontype.Host',
                fieldLabel: 'Host',
                fieldContext: 'config',
                fieldGroup: 'Options',
                required: false,
                enabled: true,
                optionSource: 'scvmmHost',
                editable: false,
                global: false,
                placeHolder: null,
                helpBlock: '',
                defaultValue: null,
                custom: false,
                displayOrder: 102,
                fieldClass: null
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
		nodeOptions << new OptionType(
			name: 'virtual image type',
			category:'provisionType.scvmm.custom',
			code: 'provisionType.scvmm.custom.containerType.virtualImageType',
			fieldContext: null,
			fieldName: 'virtualImageSelect',
			fieldCode: null,
			fieldLabel: null,
			fieldGroup: null,
			inputType: OptionType.InputType.RADIO,
			displayOrder:10,
			fieldClass:'inline',
			required: false,
			editable: true,
			optionSource: 'virtualImageTypeList'
		)

		nodeOptions << new OptionType(
			code: 'provisionType.scvmm.custom.containerType.virtualImageId',
			inputType: OptionType.InputType.SELECT,
			name: 'virtual image',
			category: 'provisionType.scvmm.custom',
			optionSourceType: 'scvmm',
			optionSource: 'scvmmVirtualImages',
			fieldName: 'virtualImage.id',
			fieldCode: 'gomorpheus.optiontype.VirtualImage',
			fieldLabel: 'Virtual Image',
			fieldContext: 'domain',
			fieldGroup: null,
			noSelection: 'Select',
			required: false,
			enabled: true,
			editable: true,
			global: false,
			placeHolder: null,
			helpBlock: '',
			defaultValue: null,
			custom: false,
			displayOrder: 12,
			fieldClass: null,
			visibleOnCode: 'virtualImageSelect:vi',
		)

		nodeOptions << new OptionType(
			name: 'osType',
			category:'provisionType.scvmm.custom',
			code: 'provisionType.scvmm.custom.containerType.osTypeId',
			fieldContext: 'domain',
			fieldName: 'osType.id',
			fieldCode: 'gomorpheus.label.osType',
			fieldLabel: 'OsType',
			fieldGroup: null,
			inputType: OptionType.InputType.SELECT,
			displayOrder:15,
			fieldClass:null,
			required: false,
			editable: true,
			noSelection: 'Select',
			optionSource: 'osTypes',
			visibleOnCode: 'virtualImageSelect:os'
		)

		nodeOptions << new OptionType(
			name: 'mount logs',
			category: "provisionType.scvmm.custom",
			code: 'provisionType.scvmm.custom.containerType.mountLogs',
			fieldContext: 'domain',
			fieldName: 'mountLogs',
			fieldCode: 'gomorpheus.optiontype.LogFolder',
			fieldLabel: 'Log Folder',
			fieldGroup: null,
			inputType: OptionType.InputType.TEXT,
			displayOrder: 20,
			required: false,
			enabled:true,
			editable: true,
			global:false,
			placeHolder:null,
			defaultValue:null,
			custom:false,
			fieldClass:null
		)
		nodeOptions << new OptionType(
			name: 'mount config',
			category: "provisionType.scvmm.custom",
			code: 'provisionType.scvmm.custom.containerType.mountConfig',
			fieldContext: 'domain',
			fieldName: 'mountConfig',
			fieldCode: 'gomorpheus.optiontype.ConfigFolder',
			fieldLabel: 'Config Folder',
			fieldGroup: null,
			inputType: OptionType.InputType.TEXT,
			displayOrder: 30,
			required: false,
			enabled:true,
			editable: true,
			global:false,
			placeHolder:null,
			defaultValue:null,
			custom:false,
			fieldClass:null,
		)
		nodeOptions << new OptionType(
			name: 'mount data',
			category: "provisionType.scvmm.custom",
			code: 'provisionType.scvmm.custom.containerType.mountData',
			fieldContext: 'domain',
			fieldName: 'mountData',
			fieldCode: 'gomorpheus.optiontype.DeployFolder',
			fieldLabel: 'Deploy Folder',
			fieldGroup: null,
			inputType: OptionType.InputType.TEXT,
			displayOrder: 40,
			required: false,
			enabled:true,
			editable: true,
			global:false,
			placeHolder:null,
			helpTextI18nCode: "gomorpheus.help.deployFolder",
			defaultValue:null,
			custom:false,
			fieldClass:null
		)
		nodeOptions << new OptionType(
			code:'provisionType.scvmm.custom.containerType.statTypeCode',
			inputType: OptionType.InputType.HIDDEN,
			name:'stat type code',
			category:'provisionType.scvmm.custom',
			fieldName:'statTypeCode',
			fieldCode: 'gomorpheus.optiontype.StatTypeCode',
			fieldLabel:'Stat Type Code',
			fieldContext:'containerType',
			fieldGroup: null,
			required:false,
			enabled:true,
			editable:false,
			global:false,
			placeHolder:null,
			helpBlock:'',
			defaultValue:'scvmm',
			custom:false,
			displayOrder:101,
			fieldClass:null
		)
		nodeOptions << new OptionType(
			code:'provisionType.scvmm.custom.containerType.logTypeCode',
			inputType: OptionType.InputType.HIDDEN,
			name:'log type code',
			category:'provisionType.scvmm.custom',
			fieldName:'logTypeCode',
			fieldCode: 'gomorpheus.optiontype.LogTypeCode',
			fieldLabel:'Log Type Code',
			fieldContext:'containerType',
			fieldGroup:	null,
			required:false,
			enabled:true,
			editable:false,
			global:false,
			placeHolder:null,
			helpBlock:'',
			defaultValue:'scvmm',
			custom:false,
			displayOrder:102,
			fieldClass:null
		)

		nodeOptions << new OptionType(
			code: 'provisionType.scvmm.custom.instanceTypeLayout.description',
			inputType: OptionType.InputType.HIDDEN,
			name: 'layout description',
			category: 'provisionType.scvmm.custom',
			fieldName: 'description',
			fieldCode: 'gomorpheus.optiontype.LayoutDescription',
			fieldLabel: 'Layout Description',
			fieldContext: 'instanceTypeLayout',
			fieldGroup: null,
			required: false,
			enabled: true,
			editable: false,
			global: false,
			placeHolder: null,
			helpBlock: '',
			defaultValue: 'This will provision a single vm container',
			custom: false,
			displayOrder: 103,
			fieldClass: null
		)

		nodeOptions << new OptionType(
			code: 'provisionType.scvmm.custom.instanceType.backupType',
			inputType: OptionType.InputType.HIDDEN,
			name: 'backup type',
			category: 'provisionType.scvmm.custom',
			fieldName: 'backupType',
			fieldCode: 'gomorpheus.optiontype.BackupType',
			fieldLabel: 'Backup Type',
			fieldContext: 'instanceType',
			fieldGroup: null,
			required: false,
			enabled: true,
			editable: false,
			global: false,
			placeHolder: null,
			helpBlock: '',
			defaultValue: 'scvmmSnapshot',
			custom: false,
			displayOrder: 100,
			fieldClass: null
		)

        return nodeOptions
    }

    /**
     * Provides a Collection of StorageVolumeTypes that are available for root StorageVolumes
     * @return Collection of StorageVolumeTypes
     */
    @Override
    Collection<StorageVolumeType> getRootVolumeStorageTypes() {
        context.async.storageVolume.storageVolumeType.list(
                new DataQuery().withFilter("code", "standard")).toList().blockingGet()
    }

    /**
     * Provides a Collection of StorageVolumeTypes that are available for data StorageVolumes
     * @return Collection of StorageVolumeTypes
     */
    @Override
    Collection<StorageVolumeType> getDataVolumeStorageTypes() {
        context.async.storageVolume.storageVolumeType.list(
				new DataQuery().withFilter("code", "standard")).toList().blockingGet()
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

        servicePlans << new ServicePlan([code            : 'scvmm-1024', editable: true, name: '1 Core, 1GB Memory', description: '1 Core, 1GB Memory', sortOrder: 1,
                                         maxStorage      : 10l * 1024l * 1024l * 1024l, maxMemory: 1l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 1,
                                         customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])

        servicePlans << new ServicePlan([code            : 'scvmm-2048', editable: true, name: '1 Core, 2GB Memory', description: '1 Core, 2GB Memory', sortOrder: 2,
                                         maxStorage      : 20l * 1024l * 1024l * 1024l, maxMemory: 2l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 1,
                                         customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])

        servicePlans << new ServicePlan([code            : 'scvmm-4096', editable: true, name: '1 Core, 4GB Memory', description: '1 Core, 4GB Memory', sortOrder: 3,
                                         maxStorage      : 40l * 1024l * 1024l * 1024l, maxMemory: 4l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 1,
                                         customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])

        servicePlans << new ServicePlan([code            : 'scvmm-8192', editable: true, name: '2 Core, 8GB Memory', description: '2 Core, 8GB Memory', sortOrder: 4,
                                         maxStorage      : 80l * 1024l * 1024l * 1024l, maxMemory: 8l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 2,
                                         customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])

        servicePlans << new ServicePlan([code            : 'scvmm-16384', editable: true, name: '2 Core, 16GB Memory', description: '2 Core, 16GB Memory', sortOrder: 5,
                                         maxStorage      : 160l * 1024l * 1024l * 1024l, maxMemory: 16l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 2,
                                         customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])

        servicePlans << new ServicePlan([code            : 'scvmm-24576', editable: true, name: '4 Core, 24GB Memory', description: '4 Core, 24GB Memory', sortOrder: 6,
                                         maxStorage      : 240l * 1024l * 1024l * 1024l, maxMemory: 24l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 4,
                                         customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])

        servicePlans << new ServicePlan([code            : 'scvmm-32768', editable: true, name: '4 Core, 32GB Memory', description: '4 Core, 32GB Memory', sortOrder: 7,
                                         maxStorage      : 320l * 1024l * 1024l * 1024l, maxMemory: 32l * 1024l * 1024l * 1024l, maxCpu: 0, maxCores: 4,
                                         customMaxStorage: true, customMaxDataStorage: true, addVolumes: true])

        servicePlans << new ServicePlan([code       : 'scvmm-hypervisor', editable: false, name: 'SCVMM hypervisor', description: 'custom hypervisor plan', sortOrder: 100, hidden: true,
                                         maxCores   : 1, maxCpu: 1, maxStorage: 20l * 1024l * 1024l * 1024l, maxMemory: (long) (1l * 1024l * 1024l * 1024l), active: true,
                                         customCores: true, customMaxStorage: true, customMaxDataStorage: true, customMaxMemory: true])

        servicePlans << new ServicePlan([code            : 'internal-custom-scvmm', editable: false, name: 'Custom SCVMM', description: 'Custom SCVMM', sortOrder: 0,
                                         customMaxStorage: true, customMaxDataStorage: true, addVolumes: true, customCpu: true, customCores: true, customMaxMemory: true, deletable: false, provisionable: false,
                                         maxStorage      : 0l, maxMemory: 0l, maxCpu: 0])

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
		ProvisionResponse provisionResponse = new ProvisionResponse(
				success: true,
				installAgent: !opts?.noAgent,
				noAgent: opts?.noAgent
		)
        def server = workload.server
        def containerId = workload?.id
        Cloud cloud = server.cloud
        try {
            def containerConfig = workload.getConfigMap()
			WorkloadType workloadType = context.services.workloadType.get(workload.workloadType.id)
            opts.server = workload.server

            def controllerNode = pickScvmmController(cloud)
            def scvmmOpts = apiService.getScvmmZoneOpts(context, cloud)
            scvmmOpts.name = server.name
            def imageId
            def virtualImage = server.sourceImage

            scvmmOpts.controllerServerId = controllerNode.id
            def externalPoolId
            if (containerConfig.resourcePool) {
                try {
                    def resourcePool = server.resourcePool
                    externalPoolId = resourcePool?.externalId
                } catch (exN) {
                    externalPoolId = containerConfig.resourcePool
                }
            }
            log.debug("externalPoolId: ${externalPoolId}")

            // host, datastore configuration
            ComputeServer node
            Datastore datastore
            def volumePath, nodeId, highlyAvailable
            def storageVolumes = server.volumes
            def rootVolume = storageVolumes.find { it.rootVolume == true }
            def maxStorage = getContainerRootSize(workload)
            def maxMemory = workload.maxMemory ?: workload.instance.plan.maxMemory
            setDynamicMemory(scvmmOpts, workload.instance.plan)
            try {
                if (containerConfig.cloneContainerId) {
                    Workload cloneContainer = context.services.workload.get(containerConfig.cloneContainerId.toLong())
                    cloneContainer.server.volumes?.eachWithIndex { vol, i ->
                        server.volumes[i].datastore = vol.datastore
                        context.services.storageVolume.save(server.volumes[i])
                    }
                }

                scvmmOpts.volumePaths = []

                (node, datastore, volumePath, highlyAvailable) = getHostAndDatastore(cloud, server.account, externalPoolId, containerConfig.hostId, rootVolume?.datastore, rootVolume?.datastoreOption, maxStorage, workload.instance.site?.id, maxMemory)
                nodeId = node?.id
                scvmmOpts.datastoreId = datastore?.externalId
                scvmmOpts.hostExternalId = node?.externalId
                scvmmOpts.volumePath = volumePath
                scvmmOpts.volumePaths << volumePath
                scvmmOpts.highlyAvailable = highlyAvailable
                log.debug("scvmmOpts: ${scvmmOpts}")

                if (rootVolume) {
                    rootVolume.datastore = datastore
                    context.services.storageVolume.save(rootVolume)
                }

                storageVolumes?.each { vol ->
                    if (!vol.rootVolume) {
                        def tmpNode, tmpDatastore, tmpVolumePath, tmpHighlyAvailable
                        (tmpNode, tmpDatastore, tmpVolumePath, tmpHighlyAvailable) = getHostAndDatastore(cloud, server.account, externalPoolId, containerConfig.hostId, vol?.datastore, vol?.datastoreOption, maxStorage, workload.instance.site?.id, maxMemory)
                        vol.datastore = tmpDatastore
                        if (tmpVolumePath) {
                            vol.volumePath = tmpVolumePath
                            scvmmOpts.volumePaths << tmpVolumePath
                        }
                        context.services.storageVolume.save(vol)
                    }
                }
            } catch (e) {
                log.error("Error in determining host and datastore: {}", e.message, e)
                return new ServiceResponse(success: false, msg: provisionResponse.message ?: 'Error in determining host and datastore', error: provisionResponse.message, data: provisionResponse)
            }

            scvmmOpts += apiService.getScvmmControllerOpts(cloud, controllerNode)
            if (containerConfig.template || virtualImage?.id) {
				if(containerConfig.template) {
					virtualImage = context.services.virtualImage.get(containerConfig.template?.toLong())
				}

                scvmmOpts.scvmmGeneration = virtualImage?.getConfigProperty('generation') ?: 'generation1'
                scvmmOpts.isSyncdImage = virtualImage?.refType == 'ComputeZone'
                scvmmOpts.isTemplate = !(virtualImage?.remotePath != null) && !virtualImage?.systemImage
                scvmmOpts.templateId = virtualImage?.externalId
                if (scvmmOpts.isSyncdImage) {
                    scvmmOpts.diskExternalIdMappings = getDiskExternalIds(virtualImage, cloud)
                    imageId = scvmmOpts.diskExternalIdMappings.find { it.rootVolume == true }.externalId
                } else {
                    imageId = virtualImage.externalId
                }
                log.debug("imageId: ${imageId}")
                if (!imageId) { //If its userUploaded and still needs uploaded
                    def cloudFiles = context.async.virtualImage.getVirtualImageFiles(virtualImage).blockingGet()
                    log.debug("cloudFiles?.size(): ${cloudFiles?.size()}")
                    if (cloudFiles?.size() == 0) {
                        server.statusMessage = 'Failed to find cloud files'
                        provisionResponse.setError("Cloud files could not be found for ${virtualImage}")
                        provisionResponse.success = false
                    }
                    def containerImage = [
                            name          : virtualImage.name ?: workload.workloadType.imageCode,
                            minDisk       : 5,
                            minRam        : 512 * ComputeUtility.ONE_MEGABYTE,
                            virtualImageId: virtualImage.id,
                            tags          : 'morpheus, ubuntu',
                            imageType     : virtualImage.imageType,
                            containerType : 'vhd',
                            cloudFiles    : cloudFiles
                    ]
                    scvmmOpts.image = containerImage
                    scvmmOpts.userId = workload.instance.createdBy?.id
                    log.debug "scvmmOpts: ${scvmmOpts}"
                    def imageResults = apiService.insertContainerImage(scvmmOpts)
                    log.debug("imageResults: ${imageResults}")
                    if (imageResults.success == true) {
                        imageId = imageResults.imageId
                        def locationConfig = [
                                virtualImage: virtualImage,
                                code        : "scvmm.image.${cloud.id}.${virtualImage.externalId}",
                                internalId  : virtualImage.externalId,
                                externalId  : virtualImage.externalId,
                                imageName   : virtualImage.name
                        ]
                        VirtualImageLocation location = new VirtualImageLocation(locationConfig)
                        context.services.virtualImage.location.create(location)
                    } else {
                        provisionResponse.success = false
                    }
                }
                if (scvmmOpts.templateId && scvmmOpts.isSyncdImage) {
                    // Determine if any additional data disks were added to the template
                    scvmmOpts.additionalTemplateDisks = additionalTemplateDisksConfig(workload, scvmmOpts)
                    log.debug "scvmmOpts.additionalTemplateDisks ${scvmmOpts.additionalTemplateDisks}"
                }
            }
            log.debug("imageId2: ${imageId}")
            if (imageId) {
                scvmmOpts.isSysprep = virtualImage?.isSysprep
                if (scvmmOpts.isSysprep) {
                    // Need to lookup the OS name
                    scvmmOpts.OSName = apiService.getMapScvmmOsType(virtualImage.osType.code, false)
                }
				opts.installAgent = (virtualImage ? virtualImage.installAgent : true) && !workloadRequest.cloudConfigOpts?.noAgent
				// If the image is an ISO or VMTools not installed, we need to skip network wait
				opts.skipNetworkWait = virtualImage?.imageType == 'iso' || !virtualImage?.vmToolsInstalled ? true : false
                //user config
                def userGroups = workload.instance.userGroups?.toList() ?: []
                if (workload.instance.userGroup && userGroups.contains(workload.instance.userGroup) == false) {
                    userGroups << workload.instance.userGroup
                }
                server.sourceImage = virtualImage
                server.externalId = scvmmOpts.name
                server.parentServer = node
                server.serverOs = server.serverOs ?: virtualImage.osType
                server.osType = (virtualImage.osType?.platform == 'windows' ? 'windows' : 'linux') ?: virtualImage.platform
                def newType = this.findVmNodeServerTypeForCloud(cloud.id, server.osType, PROVISION_TYPE_CODE)
                if (newType && server.computeServerType != newType)
                    server.computeServerType = newType
                server = saveAndGetMorpheusServer(server, true)
                scvmmOpts.imageId = imageId
                scvmmOpts.server = server
                scvmmOpts += getScvmmContainerOpts(workload)
                scvmmOpts.hostname = server.getExternalHostname()
                scvmmOpts.domainName = server.getExternalDomain()
                scvmmOpts.fqdn = scvmmOpts.hostname
                if (scvmmOpts.domainName) {
                    scvmmOpts.fqdn += '.' + scvmmOpts.domainName
                }
                scvmmOpts.networkConfig = opts.networkConfig
                if (scvmmOpts.networkConfig?.primaryInterface?.network?.pool) {
                    scvmmOpts.networkConfig.primaryInterface.poolType = scvmmOpts.networkConfig.primaryInterface.network.pool.type.code
                }
                workloadRequest.cloudConfigOpts.licenses
                scvmmOpts.licenses = workloadRequest.cloudConfigOpts.licenses
                log.debug("scvmmOpts.licenses: ${scvmmOpts.licenses}")
                if (scvmmOpts.licenses) {
                    def license = scvmmOpts.licenses[0]
                    scvmmOpts.license = [fullName: license.fullName, productKey: license.licenseKey, orgName: license.orgName]
                }


                if (virtualImage?.isCloudInit || scvmmOpts.isSysprep) {
                    def initOptions = constructCloudInitOptions(workload, workloadRequest, opts.installAgent, scvmmOpts.platform, virtualImage, scvmmOpts.networkConfig, scvmmOpts.licenses, scvmmOpts)
                    scvmmOpts.cloudConfigUser = initOptions.cloudConfigUser
                    scvmmOpts.cloudConfigMeta = initOptions.cloudConfigMeta
                    scvmmOpts.cloudConfigBytes = initOptions.cloudConfigBytes
                    scvmmOpts.cloudConfigNetwork = initOptions.cloudConfigNetwork
                    if (initOptions.licenseApplied) {
                        opts.licenseApplied = true
                    }
                    opts.unattendCustomized = initOptions.unattendCustomized
                }
                // If cloning.. gotta stop it first
                if (containerConfig.cloneContainerId) {
                    Workload cloneContainer = context.services.workload.get(containerConfig.cloneContainerId.toLong())
                    scvmmOpts.cloneContainerId = cloneContainer.id
                    scvmmOpts.cloneVMId = cloneContainer.server.externalId
                    if (cloneContainer.status == Workload.Status.running) {
                        stopWorkload(cloneContainer)
                        scvmmOpts.startClonedVM = true
                    }
                    log.debug "Handling startup of the original VM"
                    def cloneBaseOpts = [:]
                    cloneBaseOpts.cloudInitIsoNeeded = (cloneContainer.server.sourceImage && cloneContainer.server.sourceImage.isCloudInit && cloneContainer.server.serverOs?.platform != 'windows')
                    if (cloneBaseOpts.cloudInitIsoNeeded) {
                        def initOptions = constructCloudInitOptions(cloneContainer, workloadRequest, opts.installAgent, scvmmOpts.platform, virtualImage, scvmmOpts.networkConfig, scvmmOpts.licenses, scvmmOpts)
                        def clonedScvmmOpts = apiService.getScvmmZoneOpts(context, cloud)
                        clonedScvmmOpts += apiService.getScvmmControllerOpts(cloud, controllerNode)
                        clonedScvmmOpts += getScvmmContainerOpts(cloneContainer)
                        cloneBaseOpts.imageFolderName = clonedScvmmOpts.serverFolder
                        cloneBaseOpts.diskFolder = "${clonedScvmmOpts.diskRoot}\\${cloneBaseOpts.imageFolderName}"
                        cloneBaseOpts.cloudConfigBytes = initOptions.cloudConfigBytes
                        cloneBaseOpts.cloudConfigNetwork = initOptions.cloudConfigNetwork
                        cloneBaseOpts.clonedScvmmOpts = clonedScvmmOpts
                        cloneBaseOpts.clonedScvmmOpts.controllerServerId = controllerNode.id
                        if (initOptions.licenseApplied) {
                            opts.licenseApplied = true
                        }
                        opts.unattendCustomized = initOptions.unattendCustomized
                    }
                    scvmmOpts.cloneBaseOpts = cloneBaseOpts
                }
                log.debug("create server: ${scvmmOpts}")
                def createResults = apiService.createServer(scvmmOpts)
                log.debug("createResults: ${createResults}")
                scvmmOpts.deleteDvdOnComplete = createResults.deleteDvdOnComplete
                if (createResults.success == true) {
                    def checkReadyResults = apiService.checkServerReady([waitForIp: opts.skipNetworkWait ? false : true] + scvmmOpts, createResults.server.id)
                    if (checkReadyResults.success) {
                        server.externalIp = checkReadyResults.server.ipAddress
                        server.powerState = ComputeServer.PowerState.on
                        server = saveAndGetMorpheusServer(server, true)
                    } else {
                        log.error "Failed to obtain ip address for server, ${checkReadyResults}"
                        throw new Exception("Failed to obtain ip address for server")
                    }
                    if (scvmmOpts.deleteDvdOnComplete?.removeIsoFromDvd) {
                        apiService.setCdrom(scvmmOpts)
                        if (scvmmOpts.deleteDvdOnComplete?.deleteIso) {
                            apiService.deleteIso(scvmmOpts, scvmmOpts.deleteDvdOnComplete.deleteIso)
                        }
                    }

                    if (scvmmOpts.cloneVMId && scvmmOpts.cloneContainerId) {
                        // Restart the VM being cloned
                        if (scvmmOpts.startClonedVM) {
                            log.debug "Handling startup of the original VM"
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
                    if (createResults.server) {
                        server.externalId = createResults.server.id
                        server.internalId = createResults.server.VMId
                        server.parentServer = node
                        if (server.cloud.getConfigProperty('enableVnc')) {
                            //credentials
                            server.consoleHost = server.parentServer?.name
                            server.consoleType = 'vmrdp'
                            server.sshUsername = server.cloud.accountCredentialData?.username ?: server.cloud.getConfigProperty('username')
                            server.consolePassword = server.cloud.accountCredentialData?.password ?: server.cloud.getConfigProperty('password')
                            server.consolePort = 2179
                        }
                        def serverDisks = createResults.server.disks
                        if (serverDisks && server.volumes) {
                            storageVolumes = server.volumes
                            rootVolume = storageVolumes.find { it.rootVolume == true }
                            rootVolume.externalId = serverDisks.diskMetaData[serverDisks.osDisk?.externalId]?.VhdID
                            // Fix up the externalId.. initially set to the VirtualDiskDrive ID.. now setting to VirtualHardDisk ID
                            rootVolume.datastore = loadDatastoreForVolume(cloud, serverDisks.diskMetaData[rootVolume.externalId]?.HostVolumeId, serverDisks.diskMetaData[rootVolume.externalId]?.FileShareId, serverDisks.diskMetaData[rootVolume.externalId]?.PartitionUniqueId) ?: rootVolume.datastore
                            storageVolumes.each { storageVolume ->
                                def dataDisk = serverDisks.dataDisks.find { it.id == storageVolume.id }
                                if (dataDisk) {
                                    def newExternalId = serverDisks.diskMetaData[dataDisk.externalId]?.VhdID
                                    if (newExternalId) {
                                        storageVolume.externalId = newExternalId
                                    }
                                    // Ensure the datastore is set
                                    storageVolume.datastore = loadDatastoreForVolume(cloud, serverDisks.diskMetaData[storageVolume.externalId]?.HostVolumeId, serverDisks.diskMetaData[storageVolume.externalId]?.FileShareId, serverDisks.diskMetaData[storageVolume.externalId]?.PartitionUniqueId) ?: storageVolume.datastore
                                }
                            }
                        }

                        def serverDetails = apiService.getServerDetails(scvmmOpts, server.externalId)
                        if (serverDetails.success == true) {
                            log.info("serverDetail: ${serverDetails}")
                            opts.network = applyComputeServerNetworkIp(server, serverDetails.server?.ipAddress, serverDetails.server?.ipAddress, 0, null)
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

			if (provisionResponse.success != true) {
                return new ServiceResponse(success: false, msg: provisionResponse.message ?: 'vm config error', error: provisionResponse.message, data: provisionResponse)
            } else {
                return new ServiceResponse<ProvisionResponse>(success: true, data: provisionResponse)
            }
        } catch (e) {
            log.error("runWorkload error:${e}", e)
            provisionResponse.setError(e.message)
            return new ServiceResponse(success: false, msg: e.message, error: e.message, data: provisionResponse)
        }
    }

    def additionalTemplateDisksConfig(Workload workload, scvmmOpts) {
        // Determine what additional disks need to be added after provisioning
        def additionalTemplateDisks = []
        def dataDisks = getContainerDataDiskList(workload)
        log.debug "dataDisks: ${dataDisks} ${dataDisks?.size()}"
		// scvmmOpts.diskExternalIdMappings will usually contain the virtualImage disk externalId
        def diskExternalIdMappings = scvmmOpts.diskExternalIdMappings
        def additionalDisksRequired = dataDisks?.size() + 1 > diskExternalIdMappings?.size()
        def busNumber = '0'
        if (additionalDisksRequired) {
            def diskCounter = diskExternalIdMappings.size()
            dataDisks?.eachWithIndex { StorageVolume sv, index ->
                if (index + 2 > diskExternalIdMappings.size()) {  // add 1 for the root disk and then 1 for 0 based
                    additionalTemplateDisks << [idx: index + 1, diskCounter: diskCounter, diskSize: sv.maxStorage, busNumber: busNumber]
                    diskCounter++
                }
            }
        }

        log.debug "returning additionalTemplateDisks ${additionalTemplateDisks}"
        additionalTemplateDisks
    }

    private constructCloudInitOptions(Workload container, WorkloadRequest workloadRequest, installAgent, platform, VirtualImage virtualImage, networkConfig, licenses, scvmmOpts) {
        log.debug("constructCloudInitOptions: ${container}, ${installAgent}, ${platform}")
        def rtn = [:]
        ComputeServer server = container.server
        Cloud zone = server.cloud
        def cloudConfigOpts = context.services.provision.buildCloudConfigOptions(zone, server, installAgent, scvmmOpts)

        // Special handling for install agent on SCVMM (determine if we are installing via cloud init)
        cloudConfigOpts.installAgent = false
        if (installAgent == true) {
            if (zone.agentMode == 'cloudInit' && (platform != 'windows' || scvmmOpts.isSysprep)) {
                cloudConfigOpts.installAgent = true
            }
        }
        rtn.installAgent = installAgent && (cloudConfigOpts.installAgent != true)
        // If cloudConfigOpts.installAgent == true, it means we are installing the agent via cloud config.. so do NOT install is via morpheus
        cloudConfigOpts.licenses = licenses
        rtn.cloudConfigUser = workloadRequest?.cloudConfigUser ?: null
        rtn.cloudConfigMeta = workloadRequest?.cloudConfigMeta ?: null
        rtn.cloudConfigNetwork = workloadRequest?.cloudConfigNetwork ?: null
        if (cloudConfigOpts.licenseApplied) {
            rtn.licenseApplied = true
        }
        rtn.unattendCustomized = cloudConfigOpts.unattendCustomized
        rtn.cloudConfigUnattend = workloadRequest.cloudConfigUser
        def isoBuffer = context.services.provision.buildIsoOutputStream(virtualImage.isSysprep, PlatformType.valueOf(platform), rtn.cloudConfigMeta, rtn.cloudConfigUnattend, rtn.cloudConfigNetwork)
        rtn.cloudConfigBytes = isoBuffer
        return rtn
    }

    def getDiskExternalIds(VirtualImage virtualImage, cloud) {
        // The mapping of volumes is off of the VirtualImageLocation
        VirtualImageLocation location = getVirtualImageLocation(virtualImage, cloud)

        def rtn = []
        def rootVolume = location.volumes.find { it.rootVolume }
        rtn << [rootVolume: true, externalId: rootVolume.externalId, idx: 0]
        location.volumes?.eachWithIndex { vol, index ->
            if (!vol.rootVolume) {
                rtn << [rootVolume: false, externalId: vol.externalId, idx: 1 + index]
            }
        }
        rtn
    }

    private setDynamicMemory(Map targetMap, ServicePlan plan) {
        log.debug "setDynamicMemory: ${plan}"
        if (plan) {
            def ranges = plan.getConfigProperty('ranges') ?: [:]
            targetMap.minDynamicMemory = ranges.minMemory ?: null
            targetMap.maxDynamicMemory = ranges.maxMemory ?: null
        }
    }

    private getVirtualImageLocation(VirtualImage virtualImage, Cloud cloud) {
        def location = context.services.virtualImage.location.find(new DataQuery().withFilters(
                new DataFilter('virtualImage.id', virtualImage.id),
                new DataOrFilter(
                        new DataAndFilter(
                                new DataFilter('refType', 'ComputeZone'),
                                new DataFilter('refId', cloud.id)
                        ),
                        new DataAndFilter(
                                new DataFilter('virtualImage.owner.id', cloud.owner.id),
                                new DataFilter('imageRegion', cloud.regionCode)
                        )
                )
        ))
        return location
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
        if (sharedController) {
            return sharedController
        }
        def rtn = context.services.computeServer.find(new DataQuery()
                .withFilter('cloud.id', cloud.id)
                .withFilter('computeServerType.code', 'scvmmController')
                .withJoin('computeServerType'))
        if (rtn == null) {
            //old zone with wrong type
            rtn = context.services.computeServer.find(new DataQuery()
                    .withFilter('cloud.id', cloud.id)
                    .withFilter('computeServerType.code', 'scvmmHypervisor')
                    .withJoin('computeServerType'))
            if (rtn == null)
                rtn = context.services.computeServer.find(new DataQuery()
                        .withFilter('cloud.id', cloud.id)
                        .withFilter('serverType', 'hypervisor'))
            //if we have tye type
            if (rtn) {
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
        return [config                : serverConfig, vmId: container.server.externalId, name: container.server.externalId, server: container.server, serverId: container.server?.id,
                memory                : maxMemory, maxCpu: maxCpu, maxCores: maxCores, serverFolder: serverFolder, hostname: container.hostname,
                network               : network, networkId: network?.id, platform: platform, externalId: container.server.externalId, networkType: containerConfig.networkType,
                containerConfig       : containerConfig, resourcePool: resourcePool?.externalId, hostId: containerConfig.hostId,
                osDiskSize            : maxStorage, maxTotalStorage: maxTotalStorage, dataDisks: dataDisks,
                scvmmCapabilityProfile: (containerConfig.scvmmCapabilityProfile?.toString() != '-1' ? containerConfig.scvmmCapabilityProfile : null),
                accountId             : container.account?.id
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
        log.debug("startWorkload: ${workload?.id}")
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
        log.debug("removeWorkload: opts: ${opts}")
        ServiceResponse response = ServiceResponse.prepare()
        try {
            log.debug("Removing container: ${workload?.dump()}")
            if (workload.server?.externalId) {
                def scvmmOpts = getAllScvmmOpts(workload)
                def deleteResults = apiService.deleteServer(scvmmOpts, scvmmOpts.externalId)
                log.debug "deleteResults: ${deleteResults?.dump()}"
                if (deleteResults.success == true) {
                    response.success = true
                } else {
                    response.msg = 'Failed to remove vm'
                }
            } else {
                response.msg = 'vm not found'
            }
        } catch (e) {
            log.error("removeWorkload error: ${e}", e)
            response.error = e.message
        }
        return response
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
        return new ServiceResponse<ProvisionResponse>(true, null, null, new ProvisionResponse(success: true))
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
        def rtn = [success: false, msg: null]
        try {
            if (computeServer?.externalId) {
                def scvmmOpts = getAllScvmmServerOpts(computeServer)
                def stopResults = apiService.stopServer(scvmmOpts, scvmmOpts.externalId)
                if (stopResults.success == true) {
                    rtn.success = true
                }
            } else {
                rtn.msg = 'vm not found'
            }
        } catch (e) {
            log.error("stopServer error: ${e}", e)
            rtn.msg = e.message
        }
        return ServiceResponse.create(rtn)
    }

    def getServerRootSize(server) {
        def rtn
        def rootDisk = getServerRootDisk(server)
        if (rootDisk)
            rtn = rootDisk.maxStorage
        else
            rtn = server.maxStorage ?: server.plan.maxStorage
        return rtn
    }

    def getServerRootDisk(server) {
        def rtn = server?.volumes?.find { it.rootVolume == true }
        return rtn
    }

    def getServerVolumeSize(server) {
        def rtn = server.maxStorage ?: server.plan.maxStorage
        if (server?.volumes?.size() > 0) {
            def newMaxStorage = server.volumes.sum { it.maxStorage ?: 0 }
            if (newMaxStorage > rtn)
                rtn = newMaxStorage
        }
        return rtn
    }

    def getServerDataDiskList(server) {
        def rtn = server?.volumes?.findAll { it.rootVolume == false }?.sort { it.id }
        return rtn
    }

    def getScvmmServerOpts(server) {
        def serverName = server.name //cleanName(server.name)
        def serverConfig = server.getConfigMap()
        def maxMemory = server.maxMemory ?: server.plan.maxMemory
        def maxCpu = server.maxCpu ?: server.plan?.maxCpu ?: 1
        def maxCores = server.maxCores ?: server.plan.maxCores ?: 1
        def maxStorage = getServerRootSize(server)
        def maxTotalStorage = getServerVolumeSize(server)
        def dataDisks = getServerDataDiskList(server)
        def network = context.services.cloud.network.get(serverConfig.networkId?.toLong())
        def serverFolder = "morpheus\\morpheus_server_${server.id}"
        return [name     : serverName, vmId: server.externalId, config: serverConfig, server: server, serverId: server.id, memory: maxMemory, osDiskSize: maxStorage, externalId: server.externalId, maxCpu: maxCpu,
                maxCores : maxCores, serverFolder: serverFolder, hostname: server.getExternalHostname(), network: network, networkId: network?.id, maxTotalStorage: maxTotalStorage,
                dataDisks: dataDisks, scvmmCapabilityProfile: serverConfig.scvmmCapabilityProfile?.toString() != '-1' ? serverConfig.scvmmCapabilityProfile : null,
                accountId: server.account?.id]
    }

    def getAllScvmmServerOpts(server) {
        def controllerNode = pickScvmmController(server.cloud)
        def rtn = apiService.getScvmmCloudOpts(context, server.cloud, controllerNode)
        rtn += apiService.getScvmmControllerOpts(server.cloud, controllerNode)
        rtn += getScvmmServerOpts(server)
        return rtn
    }

    /**
     * Start the server
     * @param computeServer to start
     * @return Response from API
     */
    @Override
    ServiceResponse startServer(ComputeServer computeServer) {
        log.debug("startServer: computeServer.id: ${computeServer?.id}")
        def rtn = ServiceResponse.prepare()
        try {
            if (computeServer?.externalId) {
                def scvmmOpts = getAllScvmmServerOpts(computeServer)
                def results = apiService.startServer(scvmmOpts, scvmmOpts.externalId)
                if (results.success == true) {
                    rtn.success = true
                }
            } else {
                rtn.msg = 'externalId not found'
            }
        } catch (e) {
            log.error("startServer error:${e}", e)
        }
        return rtn
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
        return 'SCVMM'
    }

    @Override
    Boolean hasNetworks() {
        return true
    }

	@Override
	Integer getMaxNetworks() {
		return 1
	}

    @Override
    Boolean canAddVolumes() {
        return true
    }

    @Override
    Boolean canCustomizeRootVolume() {
        return true
    }

	@Override
	Boolean canResizeRootVolume() {
		return true
	}

	@Override
	Boolean canCustomizeDataVolumes() {
		return true
	}

	@Override
	Boolean hasDatastores() {
		return true
	}

    @Override
    HostType getHostType() {
        return HostType.vm
    }

    @Override
    String serverType() {
        return "vm"
    }

    @Override
    Boolean supportsCustomServicePlans() {
        return true;
    }

    @Override
    Boolean multiTenant() {
        return false
    }

    @Override
    Boolean aclEnabled() {
        return false
    }

    @Override
    Boolean customSupported() {
        return true;
    }

    @Override
    Boolean lvmSupported() {
        return true
    }

    @Override
    String getDeployTargetService() {
        return "vmDeployTargetService"
    }

    @Override
    String getNodeFormat() {
        return "vm"
    }

    @Override
    Boolean hasSecurityGroups() {
        return false
    }

    @Override
    Boolean hasNodeTypes() {
        return true;
    }

    @Override
    String getHostDiskMode() {
        return 'lvm'
    }

    /**
     * For most provision types, a default instance type is created upon plugin registration.  Override this method if
     * you do NOT want to create a default instance type for your provision provider
     * @return defaults to true
     */
    @Override
    Boolean createDefaultInstanceType() {
        return false
    }

    /**
     * Determines if this provision type has ComputeZonePools that can be selected or not.
     * @return Boolean representation of whether or not this provision type has ComputeZonePools
     */
    @Override
    Boolean hasComputeZonePools() {
        return true
    }

    @Override
    ServiceResponse validateHost(ComputeServer server, Map opts = [:]) {
        log.debug("validateHostConfiguration:$opts")
        def rtn = ServiceResponse.success()
        try {
            if (server.computeServerType?.vmHypervisor == true) {
                rtn = ServiceResponse.success()
            } else {
                def validationOpts = [
                        networkId             : opts?.networkInterface?.network?.id ?: opts?.config?.networkInterface?.network?.id ?: opts.networkInterfaces.getAt(0)?.network?.id,
                        scvmmCapabilityProfile: opts?.config?.scvmmCapabilityProfile ?: opts?.scvmmCapabilityProfile,
                        nodeCount             : opts?.config?.nodeCount
                ]
                def validationResults = apiService.validateServerConfig(validationOpts)
                if (!validationResults.success) {
                    rtn.success = false
                    rtn.errors += validationResults.errors
                }
            }
        } catch (e) {
            log.error("error in validateHost:${e.message}", e)
        }
        return rtn
    }

    protected ComputeServer saveAndGet(ComputeServer server) {
        def saveResult = context.async.computeServer.bulkSave([server]).blockingGet()
        def updatedServer
        if (saveResult.success == true) {
            updatedServer = saveResult.persistedItems.find { it.id == server.id }
        } else {
            updatedServer = saveResult.failedItems.find { it.id == server.id }
            log.warn("Error saving server: ${server?.id}")
        }
        return updatedServer ?: server
    }

    @Override
    ServiceResponse<PrepareHostResponse> prepareHost(ComputeServer server, HostRequest hostRequest, Map opts) {
        log.debug "prepareHost: ${server} ${hostRequest} ${opts}"

        def prepareResponse = new PrepareHostResponse(computeServer: server, disableCloudInit: false, options: [sendIp: true])
        ServiceResponse<PrepareHostResponse> rtn = ServiceResponse.prepare(prepareResponse)
        if(server.sourceImage){
            rtn.success = true
            return rtn
        }

        try {
            VirtualImage virtualImage
            Long computeTypeSetId = server.typeSet?.id
            if (computeTypeSetId) {
                ComputeTypeSet computeTypeSet = morpheus.async.computeTypeSet.get(computeTypeSetId).blockingGet()
                if (computeTypeSet.workloadType) {
                    WorkloadType workloadType = morpheus.async.workloadType.get(computeTypeSet.workloadType.id).blockingGet()
                    virtualImage = workloadType.virtualImage
                }
            }
            if (!virtualImage) {
                rtn.msg = "No virtual image selected"
            } else {
                server.sourceImage = virtualImage
                saveAndGet(server)
                rtn.success = true
            }
        } catch (e) {
            rtn.msg = "Error in prepareHost: ${e}"
            log.error("${rtn.msg}, ${e}", e)

        }
        return rtn
    }

    protected ComputeServer getMorpheusServer(Long id) {
        return context.services.computeServer.find(
                new DataQuery().withFilter("id", id).withJoin("interfaces.network")
        )
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

    def getVolumePathForDatastore(Datastore datastore) {
        log.debug "getVolumePathForDatastore: ${datastore}"
        def volumePath
        if (datastore) {
            StorageVolume storageVolume = context.services.storageVolume.find(new DataQuery()
                    .withFilter('datastore.id', datastore.id)
                    .withFilter('volumePath', '!=', null))
            volumePath = storageVolume?.volumePath
        }
        log.debug "volumePath: ${volumePath}"
        return volumePath
    }


    def getHostAndDatastore(Cloud cloud, account, clusterId, hostId, Datastore datastore, datastoreOption, size, siteId = null, maxMemory) {
        log.debug "clusterId: ${clusterId}, hostId: ${hostId}, datastore: ${datastore}, datastoreOption: ${datastoreOption}, size: ${size}, siteId: ${siteId}, maxMemory ${maxMemory}"
        ComputeServer node
        def volumePath
        def highlyAvailable = false

        // If clusterId (resourcePool) is not specified AND host not specified AND datastore is 'auto',
        // then we are just deploying to the cloud (so... can not select the datastore, nor host)
        clusterId = clusterId && clusterId != 'null' ? clusterId : null
        hostId = hostId && hostId.toString().trim() != '' ? hostId : null
        def zoneHasCloud = cloud.regionCode != null && cloud.regionCode != ''
        if (zoneHasCloud && !clusterId && !hostId && !datastore && (datastoreOption == 'auto' || !datastoreOption)) {
            return [node, datastore, volumePath, highlyAvailable]
        }
        // If host specified by the user, then use it
        node = hostId ? context.services.computeServer.get(hostId.toLong()) : null
        if (!datastore) {
            def datastoreIds = context.services.resourcePermission.listAccessibleResources(account.id, ResourcePermission.ResourceType.Datastore, siteId, null)
            def hasFilteredDatastores = false
            // If hostId specifed.. gather all the datastoreIds for the host via storagevolumes
            if (hostId) {
                hasFilteredDatastores = true
                def scopedDatastoreIds = context.services.computeServer.list(new DataQuery()
                        .withFilter('hostId', hostId.toLong())
                        .withJoin('volumes.datastore')).collect { it.volumes.collect { it.datastore?.id } }.flatten().unique()
                datastoreIds = scopedDatastoreIds
            }

            def query = new DataQuery()
                    .withFilter('refType', 'ComputeZone')
                    .withFilter('refId', cloud.id)
                    .withFilter('type', 'generic')
                    .withFilter('online', true)
                    .withFilter('active', true)
                    .withFilter('freeSpace', '>', size)
            def dsList
            def dsQuery
            if (hasFilteredDatastores) {
                dsQuery = query.withFilters(
                        new DataFilter('id', 'in', datastoreIds),
                        new DataOrFilter(
                                new DataFilter('visibility', 'public'),
                                new DataFilter('owner.id', account.id)
                        )
                )
            } else {
                dsQuery = query.withFilters(
                        new DataOrFilter(
                                new DataFilter('id', 'in', datastoreIds),
                                new DataOrFilter(
                                        new DataFilter('visibility', 'public'),
                                        new DataFilter('owner.id', account.id)
                                )
                        )
                )
            }
            if (clusterId) {
                if (clusterId.toString().isNumber()) {
                    dsQuery = dsQuery.withFilter('zonePool.id', clusterId.toLong())
                } else {
                    dsQuery = dsQuery.withFilter('zonePool.externalId', clusterId)
                }
            }
            dsList = context.services.cloud.datastore.list(dsQuery.withSort('freeSpace', DataQuery.SortOrder.desc))

            // Return the first one
            if (dsList.size() > 0) {
                datastore = dsList[0]
            }
        }

        if (!node && datastore) {
            // We've grabbed a datastore.. now pick a host that has this datastore
            def nodes = context.services.computeServer.list(new DataQuery()
                    .withFilter('cloud.id', cloud.id)
                    .withFilter('enabled', true)
                    .withFilter('computeServerType.code', 'scvmmHypervisor')
                    .withFilter('volumes.datastore.id', datastore.id)
                    .withFilter('powerState', ComputeServer.PowerState.on))
            nodes = nodes.findAll { it.capacityInfo?.maxMemory - it.capacityInfo?.usedMemory > maxMemory }?.sort { -(it.capacityInfo?.maxMemory - it.capacityInfo?.usedMemory) }
            node = nodes?.size() > 0 ? nodes.first() : null
        }

        if (!zoneHasCloud && (!node || !datastore)) {
            // Need a node and a datastore for non-cloud scoped zones
            throw new Exception('Unable to obtain datastore and host for options selected')
        }

        // Get the volumePath (used during provisioning to tell SCVMM where to place the disks)
        volumePath = getVolumePathForDatastore(datastore)

        // Highly Available (in the Failover Cluster Manager) if we are in a cluster and the datastore is a shared volume
        if (clusterId && datastore?.zonePool) {
            // datastore found above MUST be part of a shared volume because it has a zonepool
            highlyAvailable = true
        }

        return [node, datastore, volumePath, highlyAvailable]
    }

    def loadDatastoreForVolume(Cloud cloud, hostVolumeId = null, fileShareId = null, partitionUniqueId = null) {
        log.debug "loadDatastoreForVolume: ${hostVolumeId}, ${fileShareId}"
        if (hostVolumeId) {

            StorageVolume storageVolume = context.services.storageVolume.find(new DataQuery().withFilter('internalId', hostVolumeId)
                    .withFilter('datastore.refType', 'ComputeZone').withFilter('datastore.refId', cloud.id))
            def ds = storageVolume?.datastore
            if (!ds && partitionUniqueId) {

                storageVolume = context.services.storageVolume.find(new DataQuery().withFilter('externalId', partitionUniqueId)
                        .withFilter('datastore.refType', 'ComputeZone').withFilter('datastore.refId', cloud.id))
                ds = storageVolume?.datastore
            }
            return ds
        } else if (fileShareId) {

            Datastore datastore = context.services.cloud.datastore.find(new DataQuery()
                    .withFilter('externalId', fileShareId)
                    .withFilter('refType', 'ComputeZone')
                    .withFilter('refId', cloud.id))
            return datastore
        }
        null
    }

    @Override
    ServiceResponse<ProvisionResponse> runHost(ComputeServer server, HostRequest hostRequest, Map opts) {
        log.debug("runHost: ${server} ${hostRequest} ${opts}")
        ProvisionResponse provisionResponse = new ProvisionResponse()
        try {
            def config = server.getConfigMap()
            config.resourcePool = ""   // keep it blank else server creation failed
            Cloud cloud = server.cloud
            def account = server.account
            def layout = server.layout
            def typeSet = server.typeSet
            def controllerNode = pickScvmmController(cloud)
            def scvmmOpts = apiService.getScvmmCloudOpts(context, cloud, controllerNode)
            scvmmOpts.controllerServerId = controllerNode.id
            scvmmOpts.creatingDockerHost = true
            scvmmOpts.name = server.name

            def imageType = config.templateTypeSelect ?: 'default'
            def virtualImage
            def clusterId
            if (config.resourcePool) {
                def pool = server.resourcePool
                if (pool) {
                    clusterId = pool.externalId
                }
            }

            // host, datastore configuration
            ComputeServer node
            Datastore datastore
            def volumePath, nodeId, highlyAvailable
            def storageVolumes = server.volumes
            def rootVolume = getServerRootDisk(server)
            def maxStorage = getServerRootSize(server)
            def maxMemory = server.maxMemory ?: server.plan.maxMemory
            (node, datastore, volumePath, highlyAvailable) = getHostAndDatastore(cloud, account, clusterId, config.hostId, rootVolume?.datastore, rootVolume?.datastoreOption, maxStorage, server.provisionSiteId, maxMemory)
            nodeId = node?.id
            scvmmOpts.datastoreId = datastore?.externalId
            scvmmOpts.hostExternalId = node?.externalId
            scvmmOpts.volumePath = volumePath
            scvmmOpts.highlyAvailable = highlyAvailable

            if (rootVolume) {
                rootVolume.datastore = datastore
                context.services.storageVolume.save(rootVolume)
            }

            storageVolumes?.each { vol ->
                if (!vol.rootVolume) {
                    def tmpNode, tmpDatastore, tmpVolumePath, tmpHighlyAvailable
                    (tmpNode, tmpDatastore, tmpVolumePath, tmpHighlyAvailable) = getHostAndDatastore(cloud, account, clusterId, config.hostId, vol?.datastore, vol?.datastoreOption, maxStorage, server.provisionSiteId, maxMemory)
                    vol.datastore = tmpDatastore
                    context.services.storageVolume.save(vol)
                }
            }

            scvmmOpts += apiService.getScvmmControllerOpts(cloud, controllerNode)
            def imageId
            if (layout && typeSet) {
                virtualImage = typeSet.workloadType.virtualImage
                imageId = virtualImage.externalId
            } else if (imageType == 'custom' && config.template) {
                def virtualImageId = config.template?.toLong()
                virtualImage = context.services.virtualImage.get(virtualImageId)
                imageId = virtualImage.externalId
            } else {
                virtualImage = new VirtualImage(code: 'scvmm.image.morpheus.ubuntu.16.04.3-v1.ubuntu.16.04.3.amd64')
                //better this later
            }

            if (!imageId) { //If its userUploaded and still needs uploaded
                def cloudFiles = context.async.virtualImage.getVirtualImageFiles(virtualImage).blockingGet()
                def imageFile = cloudFiles?.find { cloudFile -> cloudFile.name.toLowerCase().endsWith(".vhd") || cloudFile.name.toLowerCase().endsWith(".vhdx") || cloudFile.name.toLowerCase().endsWith(".vmdk") }

                def containerImage = [
                        name          : virtualImage.name,
                        minDisk       : 5,
                        minRam        : 512l * ComputeUtility.ONE_MEGABYTE,
                        virtualImageId: virtualImage.id,
                        tags          : 'morpheus, ubuntu',
                        imageType     : virtualImage.imageType,
                        containerType : 'vhd',
                        imageFile     : imageFile,
                        cloudFiles    : cloudFiles,
                ]

                scvmmOpts.image = containerImage
                scvmmOpts.userId = server.createdBy?.id
                log.debug("scvmmOpts: {}", scvmmOpts)

                def imageResults = apiService.insertContainerImage(scvmmOpts)
                if (imageResults.success == true) {
                    imageId = imageResults.imageId
                }
            }

            if (imageId) {
                server.sourceImage = virtualImage
                server.externalId = scvmmOpts.name
                server.serverOs = server.serverOs ?: virtualImage.osType
                server.osType = (virtualImage.osType?.platform == 'windows' ? 'windows' : 'linux') ?: virtualImage.platform
                server.parentServer = node
                scvmmOpts.secureBoot = virtualImage?.uefi ?: false
                scvmmOpts.imageId = imageId
                scvmmOpts.scvmmGeneration = virtualImage?.getConfigProperty('generation') ?: 'generation1'
                scvmmOpts.diskMap = context.services.virtualImage.getImageDiskMap(virtualImage)
                server = saveAndGetMorpheusServer(server, true)
                scvmmOpts += getScvmmServerOpts(server)

                scvmmOpts.networkConfig = hostRequest.networkConfiguration
                scvmmOpts.cloudConfigUser = hostRequest.cloudConfigUser
                scvmmOpts.cloudConfigMeta = hostRequest.cloudConfigMeta
                scvmmOpts.cloudConfigNetwork = hostRequest.cloudConfigNetwork
                scvmmOpts.isSysprep = virtualImage?.isSysprep

                def isoBuffer = context.services.provision.buildIsoOutputStream(
                        scvmmOpts.isSysprep, PlatformType.valueOf(server.osType), scvmmOpts.cloudConfigMeta, scvmmOpts.cloudConfigUser, scvmmOpts.cloudConfigNetwork)

                scvmmOpts.cloudConfigBytes = isoBuffer
                server.cloudConfigUser = scvmmOpts.cloudConfigUser
                server.cloudConfigMeta = scvmmOpts.cloudConfigMeta
                server.cloudConfigNetwork = scvmmOpts.cloudConfigNetwork

                //save the server
                server = saveAndGetMorpheusServer(server, true)
                log.debug("create server:${scvmmOpts}")

                //create it in scvmm
                def createResults = apiService.createServer(scvmmOpts)
                log.debug "create server results:${createResults}"
                if (createResults.success == true) {
                    def instance = createResults.server
                    if (instance) {
                        node = context.services.computeServer.get(nodeId)
                        server.externalId = instance.id
                        server.parentServer = node
                        def serverDisks = createResults.server.disks
                        if (serverDisks) {
                            storageVolumes = server.volumes
                            rootVolume = storageVolumes.find { it.rootVolume == true }
                            rootVolume.externalId = serverDisks.diskMetaData[serverDisks.osDisk?.externalId]?.VhdID
                            // Fix up the externalId.. initially set to the VirtualDiskDrive ID.. now setting to VirtualHardDisk ID
                            rootVolume.datastore = loadDatastoreForVolume(cloud, serverDisks.diskMetaData[rootVolume.externalId]?.HostVolumeId, serverDisks.diskMetaData[rootVolume.externalId]?.FileShareId, serverDisks.diskMetaData[rootVolume.externalId]?.PartitionUniqueId) ?: rootVolume.datastore
                            storageVolumes.each { storageVolume ->
                                def dataDisk = serverDisks.dataDisks.find { it.id == storageVolume.id }
                                if (dataDisk) {
                                    def newExternalId = serverDisks.diskMetaData[dataDisk.externalId]?.VhdID
                                    if (newExternalId) {
                                        storageVolume.externalId = newExternalId
                                    }

                                    // Ensure the datastore is set
                                    storageVolume.datastore = loadDatastoreForVolume(cloud, serverDisks.diskMetaData[storageVolume.externalId]?.HostVolumeId, serverDisks.diskMetaData[storageVolume.externalId]?.FileShareId, serverDisks.diskMetaData[storageVolume.externalId]?.PartitionUniqueId) ?: storageVolume.datastore
                                }
                            }
                        }

                        def serverDetails = apiService.getServerDetails(scvmmOpts, server.externalId)
                        if (serverDetails.success == true) {
                            //fill in ip address.
                            def newIpAddress = serverDetails.server?.ipAddress ?: createResults.server?.ipAddress
                            def macAddress = serverDetails.server?.macAddress
                            applyComputeServerNetworkIp(server, newIpAddress, newIpAddress, 0, macAddress)
                            server.osDevice = '/dev/sda'
                            server.dataDevice = '/dev/sda'
                            server.sshHost = server.internalIp
                            server.managed = true
                            server.capacityInfo = new ComputeCapacityInfo(maxCores: scvmmOpts.maxCores, maxMemory: scvmmOpts.memory, maxStorage: scvmmOpts.maxTotalStorage)
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
                        //no reservation
                        server.statusMessage = 'Error loading created server'
                    }
                } else {
                    if (createResults.server?.id) {
                        // we did create a vm though so we need to bind it to the server
                        server.externalId = createResults.server.id
                        context.async.computeServer.save(server).blockingGet()
                    }
                    server.statusMessage = 'Error creating server'
                    //tell someone :)
                }
            } else {
                server.statusMessage = 'Error creating server'
            }
            if (provisionResponse.success != true) {
                return new ServiceResponse(success: false, msg: provisionResponse.message ?: 'vm config error', error: provisionResponse.message, data: provisionResponse)
            } else {
                return new ServiceResponse<ProvisionResponse>(success: true, data: provisionResponse)
            }

        } catch (Exception e) {
            log.error("Error in runHost method: ${e.message}", e)
            provisionResponse.setError(e.message)
            return new ServiceResponse(success: false, msg: e.message, error: e.message, data: provisionResponse)
        }
    }

    private applyComputeServerNetworkIp(ComputeServer server, privateIp, publicIp, index, macAddress) {
        log.debug("applyComputeServerNetworkIp: ${privateIp}")
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

    @Override
    ServiceResponse<ProvisionResponse> waitForHost(ComputeServer server) {
        log.debug("waitForHost: ${server}")
        def provisionResponse = new ProvisionResponse()
        ServiceResponse<ProvisionResponse> rtn = ServiceResponse.prepare(provisionResponse)
        try {
            def config = server.getConfigMap()
            def node = config.hostId ? context.services.computeServer.get(config.hostId.toLong()) : pickScvmmController(server.cloud)
            def scvmmOpts = apiService.getScvmmCloudOpts(context, server.cloud, node)
            scvmmOpts += apiService.getScvmmControllerOpts(server.cloud, node)
            scvmmOpts += getScvmmServerOpts(server)
            def serverDetail = apiService.checkServerReady(scvmmOpts, server.externalId)
            if (serverDetail.success == true) {
                provisionResponse.privateIp = serverDetail.server.ipAddress
                provisionResponse.publicIp = serverDetail.server.ipAddress
                provisionResponse.externalId = server.externalId
                def finalizeResults = finalizeHost(server)
                if (finalizeResults.success == true) {
                    provisionResponse.success = true
                    rtn.success = true
                }
            }
        } catch (e) {
            log.error("Error waitForHost: ${e.message}", e)
            rtn.success = false
            rtn.msg = "Error in waiting for Host: ${e}"
        }
        return rtn
    }

    @Override
    ServiceResponse finalizeHost(ComputeServer server) {
        ServiceResponse rtn = ServiceResponse.prepare()
        log.debug("finalizeHost: ${server?.id}")
        try {
            def config = server.getConfigMap()
            def node = config.hostId ? context.services.computeServer.get(config.hostId.toLong()) : pickScvmmController(server.cloud)
            def scvmmOpts = apiService.getScvmmCloudOpts(context, server.cloud, node)
            scvmmOpts += apiService.getScvmmControllerOpts(server.cloud, node)
            scvmmOpts += getScvmmServerOpts(server)
            def serverDetail = apiService.checkServerReady(scvmmOpts, server.externalId)
            if (serverDetail.success == true) {
				def newIpAddress = serverDetail.server?.ipAddress
				def macAddress = serverDetail.server?.macAddress
				applyComputeServerNetworkIp(server, newIpAddress, newIpAddress, 0, macAddress)
                context.async.computeServer.save(server).blockingGet()
                rtn.success = true
            }
        } catch (e) {
            rtn.success = false
            rtn.msg = "Error in finalizing server: ${e.message}"
            log.error("Error in finalizeHost: ${e.message}", e)
        }
        return rtn
    }

    /**
     * Request to scale the size of the Workload. Most likely, the implementation will follow that of resizeServer
     * as the Workload usually references a ComputeServer. It is up to implementations to create the volumes, set the memory, etc
     * on the underlying ComputeServer in the cloud environment. In addition, implementations of this method should
     * add, remove, and update the StorageVolumes, StorageControllers, ComputeServerInterface in the cloud environment with the requested attributes
     * and then save these attributes on the models in Morpheus. This requires adding, removing, and saving the various
     * models to the ComputeServer using the appropriate contexts. The ServicePlan, memory, cores, coresPerSocket, maxStorage values
     * defined on ResizeRequest will be set on the Workload and ComputeServer upon return of a successful ServiceResponse
     * @param instance to resize
     * @param workload to resize
     * @param resizeRequest the resize requested parameters
     * @param opts additional options
     * @return Response from API
     */
    @Override
    ServiceResponse resizeWorkload(Instance instance, Workload workload, ResizeRequest resizeRequest, Map opts) {
        log.info("resizeWorkload calling resizeWorkloadAndServer")
        return resizeWorkloadAndServer(workload, null, resizeRequest, opts, true)
    }

    @Override
    ServiceResponse resizeServer(ComputeServer server, ResizeRequest resizeRequest, Map opts) {
        log.info("resizeServer calling resizeWorkloadAndServer")
        return resizeWorkloadAndServer(null, server, resizeRequest, opts, false)
    }

    private ServiceResponse resizeWorkloadAndServer(Workload workload, ComputeServer server, ResizeRequest resizeRequest, Map opts, Boolean isWorkload) {
        log.debug("resizeWorkloadAndServer workload.id: ${workload?.id} - opts: ${opts}")

        ServiceResponse rtn = ServiceResponse.success()
        ComputeServer computeServer = isWorkload ? getMorpheusServer(workload.server?.id) : getMorpheusServer(server.id)
        try {
            computeServer.status = 'resizing'
            computeServer = saveAndGet(computeServer)
            def vmId = computeServer.externalId
            def scvmmOpts = isWorkload ? getAllScvmmOpts(workload) : getAllScvmmServerOpts(computeServer)

            // Memory and core changes
            def resizeConfig = isWorkload ?
                    getResizeConfig(workload, null, workload.instance.plan, opts, resizeRequest) :
                    getResizeConfig(null, computeServer, computeServer.plan, opts, resizeRequest)
            log.debug("resizeConfig: ${resizeConfig}")
            def requestedMemory = resizeConfig.requestedMemory
            def requestedCores = resizeConfig.requestedCores
            def neededMemory = resizeConfig.neededMemory
            def neededCores = resizeConfig.neededCores
            def minDynamicMemory = resizeConfig.minDynamicMemory
            def maxDynamicMemory = resizeConfig.maxDynamicMemory
            def stopRequired = !resizeConfig.hotResize

            // Only stop if needed
            def stopResults
            if (stopRequired) {
                stopResults = isWorkload ? stopWorkload(workload) : stopServer(computeServer)
            }
            log.debug("stopResults?.success: ${stopResults?.success}")
            if (!stopRequired || stopResults?.success == true) {
                if (neededMemory != 0 || neededCores != 0 || minDynamicMemory || maxDynamicMemory) {
                    def resizeResults = apiService.updateServer(scvmmOpts, vmId, [maxMemory: requestedMemory, maxCores: requestedCores, minDynamicMemory: minDynamicMemory, maxDynamicMemory: maxDynamicMemory])
                    log.debug("resize results: ${resizeResults}")
                    if (resizeResults.success == true) {
                        computeServer.setConfigProperty('maxMemory', requestedMemory)
                        computeServer.setConfigProperty('maxCores', (requestedCores ?: 1))
                        computeServer.maxCores = (requestedCores ?: 1).toLong()
                        computeServer.maxMemory = requestedMemory.toLong()
                        computeServer = saveAndGet(computeServer)
                        if(isWorkload) {
                            workload.setConfigProperty('maxMemory', requestedMemory)
                            workload.maxMemory = requestedMemory.toLong()
                            workload.setConfigProperty('maxCores', (requestedCores ?: 1))
                            workload.maxCores = (requestedCores ?: 1).toLong()
                            workload = context.services.workload.save(workload)
							workload.server = computeServer
                        }
                    } else {
                        rtn.error = resizeResults.error ?: 'Failed to resize container'
                    }
                }
                // Handle all the volumes
                if (opts.volumes && !rtn.error) {
                    def diskCounter = computeServer.volumes?.size()
                    resizeRequest.volumesUpdate?.each { volumeUpdate ->
                        log.debug("resizing vm storage: count: ${diskCounter} ${volumeUpdate}")
                        StorageVolume existing = volumeUpdate.existingModel
                        Map updateProps = volumeUpdate.updateProps
                        //existing disk - resize it
                        if (updateProps.maxStorage > existing.maxStorage) {
                            def volumeId = existing.externalId
							def diskSize = ComputeUtility.parseGigabytesToBytes(updateProps.size?.toLong())
                            def resizeResults = apiService.resizeDisk(scvmmOpts, volumeId, diskSize)
                            if (resizeResults.success == true) {
                                def existingVolume = context.services.storageVolume.get(existing.id)
                                existingVolume.maxStorage = diskSize
                                context.services.storageVolume.save(existingVolume)
                            } else {
                                log.error "Error in resizing volume: ${resizeResults}"
                                rtn.error = resizeResults.error ?: "Error in resizing volume"
                            }
                        }
                    }
                    // new disk add it
                    resizeRequest.volumesAdd.each { volumeAdd ->
                        def diskSize = ComputeUtility.parseGigabytesToBytes(volumeAdd.size?.toLong())  / ComputeUtility.ONE_MEGABYTE
                        def busNumber = '0'
                        def volumePath = getVolumePathForDatastore(volumeAdd.datastore)
						//Create the new diskSpec
						def diskSpec = [
								vhdName: "data-${UUID.randomUUID().toString()}",
								vhdType: null,  //Use Default as determined from existing VM
								vhdFormat: null, //Use Default  as determined from existing VM
								vhdPath: null, // Place with the VM?? or should this be volumePath?
								sizeMb: diskSize
						]
						log.info("resizeContainer - volumePath: ${volumePath} - diskSpec: ${diskSpec}")
						def diskResults = apiService.createAndAttachDisk(scvmmOpts, diskSpec, true)
						log.info("create disk: ${diskResults.success}")
                        if (diskResults.success == true) {
                            def newVolume = buildStorageVolume(computeServer, volumeAdd, diskCounter)
                            if (volumePath) {
                                newVolume.volumePath = volumePath
                            }
							//internalId can now be set to the location of the VirtualHardDisk (VhdLocation)
							newVolume.internalId = diskResults.disk.VhdLocation
                            newVolume.maxStorage = volumeAdd.size.toInteger() * ComputeUtility.ONE_GIGABYTE
                            newVolume.externalId = diskResults.disk.VhdID

                            def updatedDatastore = loadDatastoreForVolume(computeServer.cloud, diskResults.disk.HostVolumeId, diskResults.disk.FileShareId, diskResults.disk.PartitionUniqueId) ?: null
                            if (updatedDatastore && newVolume.datastore != updatedDatastore) {
                                newVolume.datastore = updatedDatastore
                            }
                            context.async.storageVolume.create([newVolume], computeServer).blockingGet()
                            computeServer = getMorpheusServer(computeServer.id)
                            diskCounter++
                        } else {
                            log.error "Error in creating the volume: ${diskResults}"
                            rtn.error = "Error in creating the volume"
                        }
                    }
                    // Delete any removed volumes
                    resizeRequest.volumesDelete.each { volume ->
                        log.debug "Deleting volume : ${volume.externalId}"
                        def detachResults = apiService.removeDisk(scvmmOpts, volume.externalId)
                        log.debug("detachResults.success: ${detachResults.data}")
                        if (detachResults.success == true) {
                            context.async.storageVolume.remove([volume], computeServer, true).blockingGet()
                            computeServer = getMorpheusServer(computeServer.id)
                        }
                    }
                }
                computeServer = getMorpheusServer(computeServer.id)
                rtn.success = true
            } else {
                rtn.success = false
                rtn.error = 'Server never stopped so resize could not be performed'
            }
            computeServer.status = 'provisioned'
            computeServer = saveAndGet(computeServer)
            if (stopRequired) {
                def startResults = isWorkload ? startWorkload(workload) : startServer(computeServer)
            }
            rtn.success = true
        } catch (e) {
            def resizeError = isWorkload ? "Unable to resize workload: ${e.message}" : "Unable to resize server: ${e.message}"
            log.error(resizeError, e)
            computeServer.status = 'provisioned'
            computeServer.statusMessage = resizeError
            computeServer = saveAndGet(computeServer)
            rtn.success = false
            rtn.setError("${e}")
        }
        return rtn
    }

    private getResizeConfig(Workload workload = null, ComputeServer server = null, ServicePlan plan, Map opts = [:], ResizeRequest resizeRequest) {
        log.debug "getResizeConfig: ${resizeRequest}"
        def rtn = [
                success       : true, allowed: true, hotResize: true, volumeSyncLists: null, requestedMemory: null,
                requestedCores: null, neededMemory: null, neededCores: null, minDynamicMemory: null, maxDynamicMemory: null
        ]
        try {
            // Memory and core changes
            rtn.requestedMemory = resizeRequest.maxStorage
            rtn.requestedCores = resizeRequest?.maxCores
            def currentMemory = server?.maxMemory ?: workload?.server?.maxMemory ?: workload?.maxMemory ?: workload?.getConfigProperty('maxMemory')?.toLong()
            def currentCores = server?.maxCores ?: workload?.maxCores ?: 1
            rtn.neededMemory = rtn.requestedMemory - currentMemory
            rtn.neededCores = (rtn.requestedCores ?: 1) - (currentCores ?: 1)
            setDynamicMemory(rtn, plan)

            rtn.hotResize = false

            // Disk changes.. see if stop is required
            if (opts.volumes) {
                resizeRequest.volumesUpdate?.each { volumeUpdate ->
                    if (volumeUpdate.existingModel) {
                        //existing disk - resize it
						def volumeCode = volumeUpdate.existingModel.type?.code ?: "standard"
                        if (volumeUpdate.updateProps.maxStorage > volumeUpdate.existingModel.maxStorage) {
							if (volumeCode.contains("differencing")) {
								log.warn("getResizeConfig - Resize is not supported on Differencing Disks  - volume type ${volumeCode}")
								rtn.allowed = false
							} else {
								log.info("getResizeConfig - volumeCode: ${volumeCode}. Volume Resize requested. Current: ${volumeUpdate.existingModel.maxStorage} - requested : ${volumeUpdate.updateProps.maxStorage}")
								rtn.allowed = true
							}
                            if (volumeUpdate.existingModel.rootVolume) {
                                rtn.hotResize = false
                            }
                        }
                    } else {
						// new disk - add it
						log.info("getResizeConfig - Adding new volume ${volumeUpdate.volume}")
						rtn.allowed = true
					}
                }
            }
        } catch (e) {
            log.error("getResizeConfig error - ${e}", e)
        }
        return rtn
    }

    def buildStorageVolume(computeServer, volumeAdd, newCounter) {
        def newVolume = new StorageVolume(
                refType: 'ComputeZone',
                refId: computeServer.cloud.id,
                regionCode: computeServer.region?.regionCode,
                account: computeServer.account,
                maxStorage: volumeAdd.maxStorage?.toLong(),
                maxIOPS: volumeAdd.maxIOPS?.toInteger(),
                name: volumeAdd.name,
                displayOrder: newCounter,
                status: 'provisioned',
                deviceDisplayName: getDiskDisplayName(newCounter)
        )
        return newVolume
    }

    /**
     * Returns a String array of block device names i.e. (['vda','vdb','vdc']) in the order
     * of the disk index.
     * @return the String array
     */
    @Override
    String[] getDiskNameList() {
        return diskNames
    }
}
