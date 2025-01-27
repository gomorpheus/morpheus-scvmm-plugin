package com.morpheusdata.scvmm

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.backup.BackupExecutionProvider
import com.morpheusdata.core.backup.response.BackupExecutionResponse
import com.morpheusdata.core.backup.util.BackupResultUtility
import com.morpheusdata.core.util.DateUtility
import com.morpheusdata.model.Backup
import com.morpheusdata.model.BackupResult
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

@Slf4j
class ScvmmBackupExecutionProvider implements BackupExecutionProvider {

	ScvmmPlugin plugin
	MorpheusContext morpheusContext
	ScvmmProvisionProvider provisionProvider
	ScvmmApiService apiService

	ScvmmBackupExecutionProvider(ScvmmPlugin plugin, MorpheusContext morpheusContext) {
		this.plugin = plugin
		this.morpheusContext = morpheusContext
		this.provisionProvider = new ScvmmProvisionProvider(plugin, morpheusContext)
		this.apiService = new ScvmmApiService(morpheusContext)
	}
	
	/**
	 * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
	 * @return an implementation of the MorpheusContext for running Future based rxJava queries
	 */
	MorpheusContext getMorpheus() {
		return morpheusContext
	}

	/**
	 * Add additional configurations to a backup. Morpheus will handle all basic configuration details, this is a
	 * convenient way to add additional configuration details specific to this backup provider.
	 * @param backupModel the current backup the configurations are applied to.
	 * @param config the configuration supplied by external inputs.
	 * @param opts optional parameters used for configuration.
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a false success will indicate a failed
	 * configuration and will halt the backup creation process.
	 */
	@Override
	ServiceResponse configureBackup(Backup backup, Map config, Map opts) {
		return ServiceResponse.success(backup)
	}

	/**
	 * Validate the configuration of the backup. Morpheus will validate the backup based on the supplied option type
	 * configurations such as required fields. Use this to either override the validation results supplied by the
	 * default validation or to create additional validations beyond the capabilities of option type validation.
	 * @param backupModel the backup to validate
	 * @param config the original configuration supplied by external inputs.
	 * @param opts optional parameters used for
	 * @return a {@link ServiceResponse} object. The errors field of the ServiceResponse is used to send validation
	 * results back to the interface in the format of {@code errors['fieldName'] = 'validation message' }. The msg
	 * property can be used to send generic validation text that is not related to a specific field on the model.
	 * A ServiceResponse with any items in the errors list or a success value of 'false' will halt the backup creation
	 * process.
	 */
	@Override
	ServiceResponse validateBackup(Backup backup, Map config, Map opts) {
		return ServiceResponse.success(backup)
	}

	/**
	 * Create the backup resources on the external provider system.
	 * @param backupModel the fully configured and validated backup
	 * @param opts additional options used during backup creation
	 * @return a {@link ServiceResponse} object. A ServiceResponse with a success value of 'false' will indicate the
	 * creation on the external system failed and will halt any further backup creation processes in morpheus.
	 */
	@Override
	ServiceResponse createBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Delete the backup resources on the external provider system.
	 * @param backupModel the backup details
	 * @param opts additional options used during the backup deletion process
	 * @return a {@link ServiceResponse} indicating the results of the deletion on the external provider system.
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local refernce
	 * will be retained.
	 */
	@Override
	ServiceResponse deleteBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Delete the results of a backup execution on the external provider system.
	 * @param backupResultModel the backup results details
	 * @param opts additional options used during the backup result deletion process
	 * @return a {@link ServiceResponse} indicating the results of the deletion on the external provider system.
	 * A ServiceResponse object with a success value of 'false' will halt the deletion process and the local refernce
	 * will be retained.
	 */
	@Override
	ServiceResponse deleteBackupResult(BackupResult backupResult, Map opts) {
		log.debug("Delete backup result {}", backupResult.id)

		def rtn = [success: true]
		try {
			def config = backupResult.getConfigMap()
			def cloudId = config.cloudId
			def cloud
			if (cloudId) {
				cloud = morpheusContext.services.cloud.get(cloudId)
			}
			def serverId = backupResult.serverId
			def server
			if (serverId) {
				server = morpheusContext.services.computeServer.get(serverId)
			} else {
				def container = morpheusContext.services.workload.get(backupResult.containerId)
				server = container ? morpheusContext.services.computeServer.get(container.serverId) : null
			}
			def snapshotId = config.snapshotId
			if (!cloud) {
				cloud = server ? morpheusContext.services.cloud.get(server.cloud.id) : null
			}
			if (snapshotId && server && cloud) {
				def node = provisionProvider.pickScvmmController(cloud)
				opts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, node)
				def result = apiService.deleteSnapshot(opts, server.externalId, snapshotId)
				if (!result.success) {
					log.debug "An error occurred removing the snapshot, server may already be deleted?... ${result}"
					rtn.success = false
				} else {
					result = [success: true]
				}
			}
		} catch (e) {
			log.error("error in deleteBackupResult: {}", e, e)
			rtn.success = false
		}
		return ServiceResponse.create(rtn)
	}

	/**
	 * A hook into the execution process. This method is called before the backup execution occurs.
	 * @param backupModel the backup details associated with the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the execution preperation. A success value
	 * of 'false' will halt the execution process.
	 */
	@Override
	ServiceResponse prepareExecuteBackup(Backup backup, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Provide additional configuration on the backup result. The backup result is a representation of the output of
	 * the backup execution including the status and a reference to the output that can be used in any future operations.
	 * @param backupResultModel
	 * @param opts
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.
	 */
	@Override
	ServiceResponse prepareBackupResult(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Initiate the backup process on the external provider system.
	 * @param backup the backup details associated with the backup execution.
	 * @param backupResult the details associated with the results of the backup execution.
	 * @param executionConfig original configuration supplied for the backup execution.
	 * @param cloud cloud context of the target of the backup execution
	 * @param computeServer the target of the backup execution
	 * @param opts additional options used during the backup execution process
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution. A success value
	 * of 'false' will halt the execution process.
	 */
	@Override
	ServiceResponse<BackupExecutionResponse> executeBackup(Backup backup, BackupResult backupResult, Map executionConfig, Cloud cloud, ComputeServer server, Map opts) {
//		return ServiceResponse.success(new BackupExecutionResponse(backupResult))
		log.debug("executeBackup: executionConfig: {}, opts: {}", executionConfig, opts)
		ServiceResponse<BackupExecutionResponse> rtn = ServiceResponse.prepare(new BackupExecutionResponse(backupResult))
		try {
			log.info("backupConfig container: ${rtn}")
			//config
//			def backupResult = backupConfig.backupResult
//			def container = Container.read(rtn.containerId)
			log.info("RAZI :: executionConfig.containerId: ${executionConfig.containerId}")
			def container = morpheusContext.services.workload.get(executionConfig.containerId)
//			def server = ComputeServer.read(container.serverId)
//			def zone = ComputeZone.read(server.zoneId)
			def snapshotName = "${server.externalId}.${System.currentTimeMillis()}".toString()
			log.info("RAZI :: snapshotName: ${snapshotName}")
			def outputPath = executionConfig.workingPath
			log.info("RAZI :: outputPath: ${outputPath}")
//			log.debug("outputPath: ${outputPath}")
			//set process id
//			setBackupProcessId(backupResult, '1000', 'executeSnapshot')
			//update status
//			updateBackupStatus(backupResult.id, 'IN_PROGRESS', [:])
			rtn.data.backupResult.status = BackupResult.Status.IN_PROGRESS
			//create snapshot
			def node = provisionProvider.pickScvmmController(cloud)
			def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(morpheusContext, cloud, node)
			log.info("RAZI :: scvmmOpts: ${scvmmOpts}")
			scvmmOpts.snapshotId = snapshotName
			def vmId = server.externalId
			log.info("RAZI :: vmId: ${vmId}")
			def snapshotResults = apiService.snapshotServer(scvmmOpts, vmId)
			log.info("RAZI :: snapshotResults: ${snapshotResults}")
			log.info("RAZI :: snapshotResults.success: ${snapshotResults.success}")
//			log.info("backup complete: {}", snapshotResults)
			if(snapshotResults.success) {
				/*def statusMap = [backupResultId:rtn.backupResultId, executorIP:rtn.ipAddress, destinationPath:outputPath,
								 providerType:'scvmm', providerBasePath:'scvmm', targetBucket:snapshotResults.snapshotId, targetDirectory:snapshotResults.snapshotId,
								 targetArchive:snapshotResults.snapshotId, backupSizeInMb:0, success:true]
				statusMap.config = [snapshotId: snapshotResults.snapshotId, vmId:vmId]
				updateBackupStatus(backupResult.id, statusMap)*/
				log.info("RAZI :: opts.backupSetId: ${opts.backupSetId}")
				rtn.data.backupResult.backupSetId = opts.backupSetId
				log.info("RAZI :: executionConfig.ipAddress: ${executionConfig.ipAddress}")
				rtn.data.backupResult.executorIpAddress = executionConfig.ipAddress
				rtn.data.backupResult.resultBase = 'scvmm'
				log.info("RAZI :: snapshotResults.snapshotId: ${snapshotResults.snapshotId}")
				rtn.data.backupResult.resultBucket = snapshotResults.snapshotId
//				rtn.data.backupResult.resultPath = snapshotResults.snapshotId
				rtn.data.backupResult.resultPath = outputPath
				rtn.data.backupResult.resultArchive = snapshotResults.snapshotId
				rtn.data.backupResult.sizeInMb = 0l
				rtn.data.backupResult.snapshotId = snapshotResults.snapshotId
				rtn.data.backupResult.setConfigProperty("snapshotId", snapshotResults.snapshotId)
				rtn.data.backupResult.setConfigProperty("vmId", vmId)
				rtn.data.updates = true
				rtn.data.backupResult.status = BackupResult.Status.SUCCEEDED
				if (!backupResult.endDate) {
					rtn.data.backupResult.endDate = new Date()
					def startDate = backupResult.startDate
					if (startDate) {
						def start = DateUtility.parseDate(startDate)
						def end = rtn.data.backupResult.endDate
						rtn.data.backupResult.durationMillis = end.time - start.time
					}
				}
			} else {
				//error
				/*def statusMap = [backupResultId:rtn.backupResultId, executorIP:rtn.ipAddress, destinationPath:outputPath,
								 backupSizeInMb:0, success:false, errorOutput:snapshotResults.error?.toString()?.encodeAsBase64()]
				updateBackupStatus(backupResult.id, statusMap)*/
				//error
				rtn.data.backupResult.backupSetId = opts.backupSetId
				rtn.data.backupResult.executorIpAddress = executionConfig.ipAddress
				rtn.data.backupResult.resultPath = outputPath
				rtn.data.backupResult.sizeInMb = 0l
				rtn.data.backupResult.status = BackupResult.Status.FAILED
				rtn.data.backupResult.errorOutput = snapshotResults.error?.toString().encodeAsBase64()
				rtn.data.updates = true
			}
			rtn.success = true
		} catch(e) {
			log.error("executeBackup: ${e}", e)
			rtn.msg = e.getMessage()
			def error = "Failed to execute backup"
			rtn.data.backupResult.backupSetId = executionConfig.backupResultId ?: BackupResultUtility.generateBackupResultSetId()
			rtn.data.backupResult.executorIpAddress = executionConfig.ipAddress
			rtn.data.backupResult.sizeInMb = 0l
			rtn.data.backupResult.status = BackupResult.Status.FAILED
			rtn.data.backupResult.errorOutput = error.encodeAsBase64()
			rtn.data.updates = true
		}
		log.info("RAZI :: backup executed successfully")
		return rtn
	}

	/**
	 * Periodically call until the backup execution has successfully completed. The default refresh interval is 60 seconds.
	 * @param backupResult the reference to the results of the backup execution including the last known status. Set the
	 *                     status to a canceled/succeeded/failed value from one of the {@link BackupStatusUtility} values
	 *                     to end the execution process.
	 * @return a {@link ServiceResponse} indicating the success or failure of the method. A success value
	 * of 'false' will halt the further execution process.n
	 */
	@Override
	ServiceResponse<BackupExecutionResponse> refreshBackupResult(BackupResult backupResult) {
		return ServiceResponse.success(new BackupExecutionResponse(backupResult))
	}
	
	/**
	 * Cancel the backup execution process without waiting for a result.
	 * @param backupResultModel the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup execution cancellation.
	 */
	@Override
	ServiceResponse cancelBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

	/**
	 * Extract the results of a backup. This is generally used for packaging up a full backup for the purposes of
	 * a download or full archive of the backup.
	 * @param backupResultModel the details associated with the results of the backup execution.
	 * @param opts additional options.
	 * @return a {@link ServiceResponse} indicating the success or failure of the backup extraction.
	 */
	@Override
	ServiceResponse extractBackup(BackupResult backupResultModel, Map opts) {
		return ServiceResponse.success()
	}

}		
