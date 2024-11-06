package com.morpheus.scvmm

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.model.Cloud
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.KeyPair
import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import com.bertramlabs.plugins.karman.CloudFile

@Slf4j
class ScvmmApiService {
    MorpheusContext morpheusContext

    ScvmmApiService(MorpheusContext morpheusContext) {
        this.morpheusContext = morpheusContext
    }
    static defaultRoot = 'C:\\morpheus'

    def executeCommand(command, opts) {
        def output = morpheusContext.executeWindowsCommand(opts.sshHost, opts.sshPort?.toInteger(), opts.sshUsername, opts.sshPassword, command, null, false).blockingGet()
        return output
    }

    def prepareNode(opts) {
        def zoneRoot = opts.zoneRoot ?: defaultRoot
        def command = "mkdir \"${zoneRoot}\\images\""
        def out = executeCommand(command, opts)
        command = "mkdir \"${zoneRoot}\\export\""
        out = executeCommand(command, opts)
        command = "mkdir \"${opts.diskRoot}\""
        out = executeCommand(command, opts)
    }

    def generateCommandString(command) {
        // FormatEnumeration causes lists to show ALL items
        // width value prevents wrapping
        "\$FormatEnumerationLimit =-1; ${command} | ConvertTo-Json -Depth 3"
    }

    def insertContainerImage(opts) {
        log.debug "insertContainerImage: ${opts}"
        def rtn = [success: false, imageExists: false]
//		rtn.success = true
//		rtn.imageId = '41266f2e-062c-4356-9eb0-b3b0e64c1e15'
//		return rtn
        def image = opts.image
        def imageName = image.name
        def imageType = image.imageType
        def imageFolderName = formatImageFolder(imageName)
        // First... see if it is already uploaded to the share
        def rootSharePath = opts.rootSharePath ?: getRootSharePath(opts)
        def tgtFolder = "${rootSharePath}\\images\\$imageFolderName"
        def tgtFullPath = "${tgtFolder}\\$imageName.$imageType"
        def out = wrapExecuteCommand(generateCommandString("Get-SCVirtualHardDisk -VMMServer localhost | where {\$_.SharePath -like \"${tgtFolder}\\*\"} | Select ID"), opts)
        if (!out.success) {
            throw new Exception("Error in getting Get-SCVirtualHardDisk")
        }
        def vhdBlocks = out.data
        if (vhdBlocks.size() == 0) {
            // Upload it (if needed)
            def match = findImage(opts, imageName)
            log.info("findImage: ${match}")
            if (match.imageExists == false) {
                //transfer it to host
                def transferResults = transferImage(opts, image.cloudFiles, imageName)
                log.debug "transferImage: ${transferResults}"
                if (transferResults.success == true) {
                    rtn.success = true
                } else {
                    rtn.msg = 'Error transferring image'
                }
            } else {
                rtn.success = true
            }

            // Import it as a physical resource
            if (rtn.success) {
                def sourcePath = findImage(opts, imageName)?.imageName

                def commands = []
                commands << "\$ignore = Import-SCLibraryPhysicalResource -SourcePath \"$sourcePath\" -SharePath \"$tgtFolder\" -OverwriteExistingFiles -VMMServer localhost"
                commands << "Get-SCVirtualHardDisk | where {\$_.SharePath -like \"${tgtFolder}\\*\"} | Select ID"
                out = wrapExecuteCommand(generateCommandString(commands.join(";")), opts)
                if (!out.success) {
                    throw new Exception("Error in importing physical resource")
                } else {
                    // Delete it from the temp directory
                    deleteImage(opts, imageName)
                }
                rtn.imageId = out.data[0].ID
            }
        } else {
            rtn.success = true
            rtn.imageId = vhdBlocks.first().ID
        }

        return rtn
    }

    def createServer(opts) {
        log.debug("createServer: ${opts}")

        def rtn = [success: false]
        try {
            def createCommands
            def launchCommand
            def createData
            def cloudInitIsoPath
            def removeTemplateCommands = []

            //these classes are not supposed to know our domain model or touch gorm - this needs to be out in the calling service
            ComputeServer server
            ComputeServer.withNewSession {
                opts.network = Network.get(opts.networkId)
                opts.zone = ComputeZone.get(opts.zoneId)
                loadControllerServer(opts)

                def diskRoot = opts.diskRoot
                def imageFolderName = opts.serverFolder
                def diskFolder = "${diskRoot}\\${imageFolderName}"
                if (opts.isSysprep) {
                    loadControllerServer(opts)
                    opts.unattendPath = importScript(opts.cloudConfigUser, diskFolder, imageFolderName, [fileName: 'Unattend.xml'] + opts)
                }

                createCommands = buildCreateServerCommands(opts)

                if (createCommands.hardwareProfileName) {
                    removeTemplateCommands << "\$HWProfile = Get-SCHardwareProfile -VMMServer localhost | where { \$_.Name -eq \"${createCommands.hardwareProfileName}\"} ; \$ignore = Remove-SCHardwareProfile -HardwareProfile \$HWProfile;"
                }
                if (createCommands.templateName) {
                    removeTemplateCommands << "\$template = Get-SCVMTemplate -VMMServer localhost -Name \"${createCommands.templateName}\";  \$ignore = Remove-SCVMTemplate -VMTemplate \$template -RunAsynchronously;"
                }

                launchCommand = createCommands.launchCommand
                log.info("launchCommand: ${launchCommand}")
                // throw new Exception('blah')
                createData = wrapExecuteCommand(generateCommandString(launchCommand), opts)
                log.debug "run server: ${createData}"

                if (removeTemplateCommands) {
                    def command = removeTemplateCommands.join(';')
                    command += "@()"
                    wrapExecuteCommand(generateCommandString(command), opts)
                }

                if (createData.success != true) {
                    if (createData.errorData?.contains('which includes generation 2')) {
                        rtn.errorMsg = 'The virtual hard disk selected is not compatible with the template which include generation 2 virtual machine functionality.'
                    } else if (createData.errorData?.contains('which includes generation 1')) {
                        rtn.errorMsg = 'The virtual hard disk selected is not compatible with the template which include generation 1 virtual machine functionality.'
                    }
                    throw new Exception("Error in launching VM: ${createData}")
                }
            }

            ComputeServer.withNewSession {
                opts.network = Network.get(opts.networkId)
                opts.zone = ComputeZone.get(opts.zoneId)
                server = ComputeServer.get(opts.serverId)
                log.info "Create results: ${createData}"

                def newServerExternalId = createData.data && createData.data.size() == 1 && createData.data[0].ObjectType?.toString() == '1' ? createData.data[0].ID : null
                if (!newServerExternalId) {
                    throw new Exception("Failed to create VM with command: ${launchCommand}: ${createData.errorData}")
                }
                opts.externalId = newServerExternalId
                // Make sure we save the externalId ASAP
                server.externalId = newServerExternalId
                server.save(flush: true)
            }
            // Find the newly assigned VM information
            def serverCreated = checkServerCreated(opts, opts.externalId)
            log.debug "Servercreated: ${serverCreated}"

            if (serverCreated.success == true) {
                ComputeServer.withNewSession {
                    opts.network = Network.get(opts.networkId)
                    opts.zone = ComputeZone.get(opts.zoneId)
                    server = ComputeServer.get(opts.serverId)
                    loadControllerServer(opts)

                    log.debug "opts.additionalTemplateDisks: ${opts.additionalTemplateDisks}"
                    opts.additionalTemplateDisks?.each { diskConfig ->
                        // Create the additional disks the user requests on the template
                        createAndAttachDisk(opts, diskConfig.diskCounter, diskConfig.diskSize, '0', null, false)
                    }
                    log.debug "finished with adding additionalDisks: ${opts.additionalTemplateDisks}"

                    // Special stuff for cloned VMs
                    if (opts.cloneVMId) {
                        // Update the VolumeType for the root disk (SCVMM doesn't preserve the VolumeType :( )
                        changeVolumeTypeForClonedBootDisk(opts, opts.cloneVMId, opts.externalId)
                        // Need to re-create the ISO for the original cloned box and mount the ISO
                        if (opts.cloneBaseOpts && opts.cloneBaseOpts.cloudInitIsoNeeded) {
                            rtn.cloneBaseResults = [cloudInitIsoPath: importAndMountIso(opts.cloneBaseOpts.cloudConfigBytes, opts.cloneBaseOpts.diskFolder, opts.cloneBaseOpts.imageFolderName, opts.cloneBaseOpts.clonedScvmmOpts)]
                        }
                    }
                    // Fetch the disks to create a mapping
                    def disks = [osDisk: [externalId: ''], dataDisks: opts.dataDisks?.collect { [id: it.id] }, diskMetaData: [:]]
                    def diskDrives = listVirtualDiskDrives(opts, opts.externalId)
                    def bookDiskIndex = findBootDiskIndex(diskDrives)
                    diskDrives.disks?.eachWithIndex { disk, diskIndex ->
                        if (diskIndex == bookDiskIndex) {
                            disks.osDisk.externalId = disk.ID
                            disks.diskMetaData[disk.ID] = [HostVolumeId: disk.HostVolumeId, FileShareId: disk.FileShareId, VhdID: disk.VhdID, PartitionUniqueId: disk.PartitionUniqueId]
                        } else {
                            disks.dataDisks[diskIndex - 1].externalId = disk.ID

                            disks.diskMetaData[disk.ID] = [HostVolumeId: disk.HostVolumeId, FileShareId: disk.FileShareId, dataDisk: true, VhdID: disk.VhdID, PartitionUniqueId: disk.PartitionUniqueId]
                        }
                    }
                    //resize disk
                    log.debug ".. about to resize disk ${opts.osDiskSize}"
                    def diskRoot = opts.diskRoot
                    def imageFolderName = opts.serverFolder
                    def diskFolder = "${diskRoot}\\${imageFolderName}"
                    if (opts.osDiskSize) {
                        def osDiskVhdID = disks.diskMetaData[disks.osDisk?.externalId]?.VhdID
                        resizeDisk(opts, osDiskVhdID, opts.osDiskSize)
                    }

                    // Resize the data disks if template
                    if (opts.isTemplate && opts.templateId && opts.dataDisks) {
                        disks.diskMetaData?.each { externalId, map ->
                            def storageVolume = opts.dataDisks.find { it.externalId == externalId }
                            if (storageVolume) {
                                def diskVhdID = disks.diskMetaData[externalId]?.VhdID
                                resizeDisk(opts, diskVhdID, storageVolume.maxStorage)
                            }
                        }
                    }

                    //cloud init
                    if (opts.cloudConfigBytes && !opts.isSysprep) {
                        createDVD(opts)
                        cloudInitIsoPath = importAndMountIso(opts.cloudConfigBytes, diskFolder, imageFolderName, opts)
                    }

                    //start it
                    log.info("Starting Server  ${opts.name}")
                    startServer(opts, opts.externalId)
                    //get details
                    log.info("SCVMM Check for Server Ready ${opts.name}")
                    def serverDetail = checkServerReady(opts, opts.externalId)
                    if (serverDetail.success == true) {
                        rtn.server = [name: opts.name, id: opts.externalId, VMId: serverDetail.server?.VMId, ipAddress: serverDetail.server?.ipAddress, disks: disks]
                        rtn.success = true
                    } else {
                        rtn.server = [name: opts.name, id: opts.externalId, VMId: serverDetail.server?.VMId, ipAddress: serverDetail.server?.ipAddress, disks: disks]
                    }
                }
            }

            if (cloudInitIsoPath) {
                // Unset the DVD
                rtn.deleteDvdOnComplete = [removeIsoFromDvd: true, deleteIso: cloudInitIsoPath]
            }
            if (opts.unattendPath) {
                deleteUnattend(opts, opts.unattendPath)
            }

            // Perform the remove again... in case they were locked above
            if (removeTemplateCommands) {
                def command = removeTemplateCommands.join(';')
                command += "@()"
                wrapExecuteCommand(generateCommandString(command), opts)
            }
        } catch (e) {
            log.error("createServer error: ${e}", e)
        }
        return rtn
    }

    def getServerDetails(opts, externalId) {
        log.debug "getServerDetails: ${externalId}"
        def rtn = [success: false, server: null, networkAdapters: [], error: null]
        try {
            def out = wrapExecuteCommand(generateCommandString("""\$vm = Get-SCVirtualMachine -VMMServer localhost -ID \"${externalId}\";
\$report = @()
if(\$vm) { 
	\$networkAdapters = Get-SCVirtualNetworkAdapter -VMMServer localhost -VM \$vm | where { \$_.Enabled -eq \$true }; 
	
	\$data = New-Object PSObject -property @{
		ID=\$vm.ID
		VMId=\$vm.VMId
		Name=\$vm.Name
		Status=([Microsoft.VirtualManager.Utils.VMComputerSystemState]\$vm.Status).toString()
		VirtualMachineState=([Microsoft.VirtualManager.Utils.VMComputerSystemState]\$vm.VirtualMachineState).toString()
		VirtualHardDiskDrives=@(\$vm.VirtualDiskDrives.VirtualHardDisk.ID)
		VirtualDiskDrives=@(\$vm.VirtualDiskDrives.ID)
		ipAddress=''
		internalIp=''
	}
	foreach (\$na in \$networkAdapters) {
		foreach (\$ip in \$na.IPv4Addresses) {
			if([string]::IsNullOrEmpty(\$data.ipAddress)) {
				\$data.ipAddress = \$ip
				\$data.internalIp = \$ip
			}
		}
	}
	\$report += \$data
} else {
	\$data = New-Object PSObject -property @{
		Error='VM_NOT_FOUND'
	}
	\$report += \$data
}
\$report """), opts)
            if (out.success) {
                def serverData = out.data?.size() > 0 ? out.data.first() : null
                if (!serverData?.Error) {
                    rtn.server = serverData
                    rtn.success = out.success
                } else {
                    rtn.error = serverData.Error
                }
            }
        } catch (e) {
            log.error("getServerDetails error: ${e}", e)
        }
        return rtn
    }

    def refreshVM(opts, externalId) {
        def rtn = [success: false]
        try {
            def out = wrapExecuteCommand(generateCommandString("""\$vm = Get-SCVirtualMachine -VMMServer localhost -ID \"${externalId}\"; 
\$ignore = Read-SCVirtualMachine -VM \$vm """), opts)
            rtn.success = out.success
        } catch (e) {
            log.error("refreshVM error: ${e}", e)
        }
        return rtn
    }

    def discardSavedState(opts, externalId) {
        log.debug "discardSavedState: ${opts}, ${externalId}"
        def rtn = [success: false, server: null, networkAdapters: []]
        try {
            executeCommand("\$vm = Get-SCVirtualMachine -VMMServer localhost -ID \"${externalId}\"; Use-SCDiscardSavedStateVM -VM \$vm;", opts)
        } catch (e) {
            log.error("discardSavedState error: ${e}", e)
        }
        return rtn
    }

    def getScvmmServerInfo(opts) {
        def rtn = [success: false]
        def command = 'hostname'
        def out = executeCommand(command, opts)
        log.debug("out: ${out.data}")
        rtn.hostname = cleanData(out.data)
        command = 'wmic computersystem get TotalPhysicalMemory'
        out = executeCommand(command, opts)
        log.debug("out: ${out.data}")
        rtn.memory = cleanData(out.data, 'TotalPhysicalMemory')
        command = 'wmic diskdrive get size'
        out = executeCommand(command, opts)
        log.debug("out: ${out.data}")
        rtn.disks = cleanData(out.data, 'Size')
        rtn.success = true
        return rtn
    }

    def getCloud(opts) {
        def rtn = [success: false, cloud: null]
        def command = generateCommandString("""\$cloud = Get-SCCloud -VMMServer localhost | where { \$_.ID -eq \'${opts.zone.regionCode}\' }
\$report = @()
if(\$cloud) {
	\$data = New-Object PSObject -property @{
		ID=\$cloud.ID
		Name=\$cloud.Name
		CapabilityProfiles=@(\$cloud.CapabilityProfiles.Name)
	}
	\$report += \$data
}
\$report """)
        def out = wrapExecuteCommand(command, opts)
        log.debug("out: ${out.data}")
        if (out.success) {
            def cloudBlocks = out.data
            if (cloudBlocks) {
                rtn.cloud = cloudBlocks.first()
            }
            rtn.success = true
        }
        return rtn
    }

    def getCapabilityProfiles(opts) {
        def rtn = [success: false, capabilityProfiles: null]
        def command = generateCommandString("Get-SCCapabilityProfile -VMMServer localhost | Select ID,Name")
        def out = wrapExecuteCommand(command, opts)
        log.debug("out: ${out.data}")
        if (out.success) {
            def cloudBlocks = out.data
            if (cloudBlocks) {
                rtn.capabilityProfiles = cloudBlocks
            }
            rtn.success = true
        }
        return rtn
    }

    def listClouds(opts) {
        def rtn = [success: false, clouds: []]
        def command = generateCommandString('Get-SCCloud -VMMServer localhost | Select ID, Name')
        def out = wrapExecuteCommand(command, opts)
        if (out.success) {
            rtn.clouds = out.data
            rtn.success = true
        }
        return rtn
    }

    def listVirtualMachines(opts) {
        def rtn = [success: false, virtualMachines: []]

        def hostGroup = opts.zoneConfig?.hostGroup

        def hasMore = true
        def pageSize = 50
        def fetch = { offset ->

            def commandStr = """\$report = @()"""
            if (!hostGroup) {
                if (opts.zone.regionCode) {
                    commandStr += """
\$cloud = Get-SCCloud -VMMServer localhost | where { \$_.ID -eq \'${opts.zone.regionCode}\' } 
"""
                    commandStr += """
	\$VMs = Get-SCVirtualMachine -VMMServer localhost -Cloud \$cloud | where { \$_.Status -ne 'Missing' } | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
	"""
                } else {
                    commandStr += """
	\$VMs = Get-SCVirtualMachine -VMMServer localhost -All | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
	"""
                }
            } else {
                if (opts.zone.regionCode) {
                    commandStr += """
\$cloud = Get-SCCloud -VMMServer localhost | where { \$_.ID -eq \'${opts.zone.regionCode}\' } 
"""
                    commandStr += """
	\$VMs = Get-SCVirtualMachine -VMMServer localhost -Cloud \$cloud | where { \$_.Status -ne 'Missing' -and \$_.HostGroupPath -like '${
                        hostGroup
                    }*' } | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
	"""
                } else {
                    commandStr += """
	\$VMs = Get-SCVirtualMachine -VMMServer localhost | where { \$_.Status -ne 'Missing' -and \$_.HostGroupPath -like '${
                        hostGroup
                    }*' } | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
	"""
                }
            }
            commandStr += """
				foreach (\$VM in \$VMs) {
					\$data = New-Object PSObject -property @{
						ID=\$VM.ID
						VMId=\$VM.VMId
						Name=\$VM.Name
						CPUCount=\$VM.CPUCount
						Memory=\$VM.Memory
						VirtualMachineState=([Microsoft.VirtualManager.Utils.VMComputerSystemState]\$VM.VirtualMachineState).toString()
						MemoryAvailablePercentage=\$VM.MemoryAvailablePercentage
						CPUUtilization=\$VM.CPUUtilization
						TotalSize=0
						UsedSize=0
						HostId=\$VM.HostId
						Disks=@()
						IpAddress=''
						InternalIp=''
						OperatingSystem=\$VM.OperatingSystem.Name
						OperatingSystemWindows=\$VM.OperatingSystem.IsWindows
						DynamicMemoryEnabled=\$VM.DynamicMemoryEnabled
						MemoryAssignedMB=\$VM.MemoryAssignedMB
						DynamicMemoryMinimumMB=\$VM.DynamicMemoryMinimumMB
						DynamicMemoryMaximumMB=\$VM.DynamicMemoryMaximumMB

					}
		
					\$VHDs = \$VM | Get-SCVirtualDiskDrive
					foreach (\$VHDconf in \$VHDs){
						\$VHD = \$VHDconf.VirtualHardDisk
						\$disk = New-Object PSObject -property @{
							ID=\$VHD.ID
							Name=\$VHD.Name
							Location=\$VHD.Location
							TotalSize=\$VHD.MaximumSize
							UsedSize=\$VHD.Size
							HostId=\$VHD.HostId
							HostVolumeId=\$VHD.HostVolumeId
							PartitionUniqueId=\$VHD.HostVolume.PartitionUniqueId
							VolumeType=([Microsoft.VirtualManager.Remoting.VolumeType]\$VHDconf.VolumeType).toString()
						}
						\$data.Disks += \$disk
						\$data.TotalSize += \$VHD.MaximumSize
						\$data.UsedSize += \$VHD.Size
					}

					\$VNAs = \$VM | Get-SCVirtualNetworkAdapter
					foreach (\$VNA in \$VNAs) {
						foreach (\$ip in \$VNA.IPv4Addresses) {
							if([string]::IsNullOrEmpty(\$data.IpAddress)) {
								\$data.IpAddress = \$ip
								\$data.InternalIp = \$ip
							}
						}
					}

					\$report +=\$data
				}
				\$report """


            def command = generateCommandString(commandStr)
            def out = wrapExecuteCommand(command, opts)
            log.debug("out: ${out.data}")
            if (out.success) {
                hasMore = out.data != ''
                if (out.data) {
                    rtn.virtualMachines += out.data
                }
                rtn.success = true
            } else {
                hasMore = false
            }
        }

        def currentOffset = 0
        while (hasMore) {
            fetch(currentOffset)
            currentOffset += pageSize
        }

        return rtn
    }

    def listTemplates(Map opts) {
        def rtn = [success: false, templates: []]
        def commandStr = """\$report = @()
\$VMTemplates = Get-SCVMTemplate -VMMServer localhost -All | where { \$_.ID -ne \$_.Name -and \$_.Status -eq 'Normal'}
foreach (\$Template in \$VMTemplates) {
	\$data = New-Object PSObject -property @{
		ID=\$Template.ID
		Name=\$Template.Name
		CPUCount=\$Template.CPUCount
		Memory=\$Template.Memory
		OperatingSystem=\$Template.OperatingSystem.Name
		TotalSize=0
		UsedSize=0
		Generation=\$Template.Generation
		Disks=@()
	}

	foreach (\$VHDconf in \$Template.VirtualDiskDrives){
		\$VHD = \$VHDconf.VirtualHardDisk
		\$disk = New-Object PSObject -property @{
			ID=\$VHD.ID
			Name=\$VHD.Name
			Location=\$VHD.Location
			TotalSize=\$VHD.MaximumSize
			UsedSize=\$VHD.Size
			HostId=\$VHD.HostId
			HostVolumeId=\$VHD.HostVolumeId
			VolumeType=([Microsoft.VirtualManager.Remoting.VolumeType]\$VHDconf.VolumeType).toString()
		}
		\$data.Disks += \$disk
		\$data.TotalSize += \$VHD.MaximumSize
		\$data.UsedSize += \$VHD.Size
	}
	\$report += \$data
}

\$Disks = Get-SCVirtualHardDisk -VMMServer localhost
foreach (\$VHDconf in \$Disks) {
	\$data = New-Object PSObject -property @{
		ID=\$VHDconf.ID
		Name=\$VHDconf.Name
		Location=\$VHDconf.Location
		OperatingSystem=\$VHDconf.OperatingSystem.Name
		TotalSize=\$VHDconf.MaximumSize
		VHDFormatType= ([Microsoft.VirtualManager.Remoting.VHDFormatType]\$VHDconf.VHDFormatType).toString()
		UsedSize=0
		Disks=@()
	}
	\$disk = New-Object PSObject -property @{
		ID=\$VHDconf.ID
		Name=\$VHDconf.Name
		Location=\$VHDconf.Location
		TotalSize=\$VHDconf.MaximumSize
		UsedSize=\$VHDconf.Size
		HostId=\$VHDconf.HostId
		HostVolumeId=\$VHDconf.HostVolumeId
	}
	\$data.Disks += \$disk
	\$report += \$data
}
\$report """
        def command = generateCommandString(commandStr)
        def out = wrapExecuteCommand(command, opts)
        log.debug("out: ${out.data}")
        if (out.success) {
            rtn.templates = out.data
            rtn.success = true
        }
        return rtn
    }

    def listClusters(Map opts) {
        def rtn = [success: false, clusters: []]
        def commandStr = """\$report = @()
		\$Clusters = Get-SCVMHostCluster -VMMServer localhost
		foreach (\$Cluster in \$Clusters) {
			\$data = New-Object PSObject -property @{
				id=\$Cluster.ID
				name=\$Cluster.Name
				hostGroup=\$Cluster.HostGroup.Path
				sharedVolumes=@(\$Cluster.SharedVolumes.Name)
				description=\$Cluster.Description
			}
			\$report +=\$data
		}
		\$report """
        def command = generateCommandString(commandStr)
        def out = wrapExecuteCommand(command, opts)
        if (out.success) {
            rtn.clusters = out.data

            // Scope it down to the HostGroup for the zone (or ALL)
            def clusterScope = opts.zone.getConfigProperty('hostGroup')
            if (clusterScope) {
                rtn.clusters = rtn.clusters?.findAll { it.hostGroup?.startsWith(clusterScope) }
            } else {
                rtn.clusters = rtn.clusters
            }

            rtn.success = true
        }

        return rtn
    }

    def internalListHostGroups(Map opts) {
        def rtn = [success: false, hostGroups: []]
        def commandStr = """Get-SCVMHostGroup -VMMServer localhost | Select-Object @{Name="id";Expression={\$_.ID.Guid}}, @{Name="name";Expression={\$_.Name}}, @{Name="path";Expression={\$_.Path}}, @{Name="parent";Expression={\$_.ParentHostGroup.Name}}, @{Name="root";Expression={\$_.IsRoot}}"""

        def command = generateCommandString(commandStr)
        def out = wrapExecuteCommand(command, opts)
        if (out.success) {
            rtn.hostGroups = out.data
            rtn.success = true
        }
        return rtn
    }

    def listLibraryShares(Map opts) {
        def rtn = [success: false, libraryShares: []]
        def command = """\$report = @()
\$shares = Get-SCLibraryShare -VMMServer localhost 
foreach(\$share in \$shares) {
  \$data = New-Object PSObject -property @{
    ID=\$share.ID
    Name=\$share.Name
    Path=\$share.Path
}
\$report += \$data
}
\$report"""

        def out = wrapExecuteCommand(generateCommandString(command), opts)
        if (out.success) {
            rtn.libraryShares = out.data
            rtn.success = true
        }
        return rtn
    }

    def listHostGroups(opts) {
        def rtn = [success: false, hostGroups: []]
        if (opts.zone.regionCode) {
            def commandStr = """\$report = @()
\$clouds = Get-SCCloud -VMMServer localhost
foreach (\$cloud in \$clouds) {
  \$data = New-Object PSObject -property @{
    ID=\$cloud.ID
    HostGroup=@(\$cloud.HostGroup.Path)
  }
  \$report += \$data
}
\$report"""
            def command = generateCommandString(commandStr)
            def out = wrapExecuteCommand(command, opts)
            log.debug("out: ${out.data}")
            if (out.success) {
                def clouds = out.data
                def cloud = clouds?.find { it.ID == opts.zone?.regionCode }
                def cloudHostGroupPaths = cloud?.HostGroup
                def hostGroups = internalListHostGroups(opts)?.hostGroups
                rtn.hostGroups = hostGroups?.findAll { hg ->
                    def foundMatch = false
                    def currentPath = hg.path
                    cloudHostGroupPaths?.each { cloudHostGroupPath ->
                        if (!foundMatch) {
                            foundMatch = isHostInHostGroup(currentPath, cloudHostGroupPath)
                        }
                    }
                    return foundMatch
                }
                rtn.success = true
            }
        } else {
            def hostGroupsResult = internalListHostGroups(opts)
            rtn.hostGroups = hostGroupsResult?.hostGroups
            rtn.success = hostGroupsResult.success
        }

        return rtn
    }

    def listHosts(Map opts) {
        def rtn = [success: false, hosts: []]

        def hasMore = true
        def pageSize = 10
        def fetch = { offset ->
            def commandStr = """\$report = @()
			\$HostNodes = Get-SCVMHost -VMMServer localhost | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
			foreach (\$HostNode in \$HostNodes) {
				\$data = New-Object PSObject -property @{
					id=\$HostNode.ID
					name=\$HostNode.Name
					status=\$HostNode.OverallStateString
					computerName=\$HostNode.ComputerName
					description=\$HostNode.Description
					cpuReserve=\$HostNode.CPUPercentReserve
					cpuUtilization=\$HostNode.CpuUtilization
					hostGroup=\$HostNode.VMHostGroup.Path
					cluster=\$HostNode.HostCluster.Name
					vmPaths=\$HostNode.VMPaths
					enabled=\$HostNode.AvailableForPlacement
					cpuCount=\$HostNode.PhysicalCPUCount
					coresPerCpu=\$HostNode.CoresPerCPU
					diskReserve=\$HostNode.DiskSpaceReserveMB
					totalStorage=\$HostNode.TotalStorageCapacity
					availableStorage=\$HostNode.AvailableStorageCapacity
					usedStorage=\$HostNode.UsedStorageCapacity
					memoryReserve=\$HostNode.MemoryReserveMB
					diskPaths=\$HostNode.BaseDiskPaths
					totalMemory=\$HostNode.TotalMemory
					availableMemory=\$HostNode.AvailableMemory  
					os=\$HostNode.OperatingSystem.Name
					liveMigration=\$HostNode.SupportsLiveMigration
					remoteEnabled=\$HostNode.RemoteConnectEnabled
					reportPort=\$HostNode.RemoveConnectPort
					migrationSubnets=\$HostNode.MigrationSubne			
					tz=\$HostNode.TimeZone
					hypervVersion=\$HostNode.HyperVVersion
					maxMemoryPerVm=\$HostNode.MaximumMemoryPerVM
					gpus=\$HostNode.GPUs
					hyperVState=([Microsoft.VirtualManager.Remoting.ServiceState]\$HostNode.HyperVState).toString()
				}
				\$report +=\$data
			}
			\$report """

            // availableMemory is in MB
            // totalMemory is in bytes
            // totalStorage in bytes
            // availableStorage in bytes
            // usedStorage in bytes
            // cpuUtilization is percent

            def command = generateCommandString(commandStr)
            def out = wrapExecuteCommand(command, opts)
            if (out.success) {
                hasMore = out.data != ''
                if (out.data) {
                    rtn.hosts += out.data
                }
                rtn.success = true
            } else {
                hasMore = false
            }
        }

        def currentOffset = 0
        while (hasMore) {
            fetch(currentOffset)
            currentOffset += pageSize
        }

        return rtn
    }

    def listDatastores(Map opts) {
        def rtn = [success: false, datastores: []]

        def hasMore = true
        def pageSize = 50
        def fetchStorageVolumes = { offset ->
            def commandStr = """\$report = @()
				\$cloud = Get-SCCloud -VMMServer localhost | where { \$_.ID -eq \'${opts.zone.regionCode ?: ''}\' } 
				\$StorageVolumes = Get-SCStorageVolume -VMMServer localhost | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
				If (-not ([string]::IsNullOrEmpty(\$cloud))) {
					\$AllowedClassifications = \$cloud.StorageClassifications.Name
					\$StorageVolumes = \$StorageVolumes | where { \$_.Classification -in \$AllowedClassifications }
				}
				foreach (\$StorageVolume in \$StorageVolumes) {
					\$data = New-Object PSObject -property @{
						id=\$StorageVolume.ID
						name=\$StorageVolume.Name
						storageVolumeID=\$StorageVolume.StorageVolumeID
						partitionUniqueID=\$StorageVolume.PartitionUniqueID
						capacity=\$StorageVolume.Capacity
						freeSpace=\$StorageVolume.FreeSpace
						isClusteredSharedVolume=\$StorageVolume.IsClusterSharedVolume
						vmHost=\$StorageVolume.VMHost.Name
						isAvailableForPlacement=\$StorageVolume.IsAvailableForPlacement
						hostDisk=\$StorageVolume.HostDisk.Name
						size=\$StorageVolume.Size
						mountPoints=\$StorageVolume.MountPoints
					}
					\$report +=\$data
				}
				\$report """

            def command = generateCommandString(commandStr)
            def out = wrapExecuteCommand(command, opts)
            log.debug "listDatastores results: ${out}"
            if (out.success) {
                hasMore = out.data != ''
                if (out.data) {
                    rtn.datastores += out.data
                }
                rtn.success = true
            } else {
                log.debug "Return not successful: ${out}"
                hasMore = false
            }
        }

        def currentOffset = 0
        while (hasMore) {
            fetchStorageVolumes(currentOffset)
            currentOffset += pageSize
        }

        return rtn
    }

    def listRegisteredFileShares(Map opts) {
        def rtn = [success: false, datastores: []]

        def hasMore = true
        def pageSize = 50
        def fetchFileShares = { offset ->
            def commandStr = """\$report = @()
\$FileShares = Get-SCStorageFileShare -VMMServer localhost | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
foreach (\$FileShare in \$FileShares){
    \$fileShareDisk = New-Object PSObject -property @{
        ID=\$FileShare.ID
        Name=\$FileShare.Name
        Capacity=\$FileShare.Capacity
        FreeSpace=\$FileShare.FreeSpace
        IsAvailableForPlacement=\$FileShare.IsAvailableForPlacement
        MountPoints=\$FileShare.MountPoints
        ClusterAssociations=@()
        HostAssociations=@()
    }
    
    foreach (\$CA in \$FileShare.ClusterAssociations) {
        \$tmpCluster = New-Object PSObject -property @{
            ClusterID=\$CA.Cluster.ID
            ClusterName=\$CA.Cluster.Name
            HostID=\$CA.Host.ID
            HostName=\$CA.Host.Name
        }
        \$fileShareDisk.ClusterAssociations += \$tmpCluster
    }

    foreach (\$HA in \$FileShare.HostAssociations) {
        \$tmpHost = New-Object PSObject -property @{
            HostID=\$HA.Host.ID
            HostName=\$HA.Host.Name
        }
        \$fileShareDisk.HostAssociations += \$tmpHost
    }

    \$report += \$fileShareDisk
}
\$report """
            def command = generateCommandString(commandStr)
            def out = wrapExecuteCommand(command, opts)
            log.debug "listDatastores results: ${out}"
            if (out.success) {
                hasMore = out.data != ''
                if (out.data) {
                    rtn.datastores += out.data
                }
                rtn.success = true
            } else {
                log.debug "Return not successful: ${out}"
                hasMore = false
            }
        }

        def currentOffset = 0
        while (hasMore) {
            fetchFileShares(currentOffset)
            currentOffset += pageSize
        }

        return rtn
    }

    def listAllNetworks(opts) {
        def rtn = [success: true, networks: []]
        try {
            def command = generateCommandString("Get-SCLogicalNetwork -VMMServer localhost | Select ID,Name")
            def out = wrapExecuteCommand(command, opts)
            log.debug("listNetworks: ${out}")
            if (out.success && out.exitValue == 0 && out.data?.size() > 0) {
                def logicalNetworks = out.data
                command = generateCommandString("""\$report = @()
\$networks = Get-SCVMNetwork -VMMServer localhost | Select ID,Name,LogicalNetwork | Sort-Object -Property ID | Select-Object -First 1
foreach (\$network in \$networks) {
	\$data = New-Object PSObject -property @{
		ID=\$network.ID
		Name=\$network.Name
		LogicalNetwork=\$network.LogicalNetwork.Name
	}
	\$report += \$data
}
\$report """)
                out = wrapExecuteCommand(command, opts)
                log.debug("get of networks: ${out}")
                if (out.success && out.exitValue == 0) {
                    if (out.data) {
                        log.debug("list logical networks: ${out}")
                        def networks = out.data
                        logicalNetworks?.each { logicalNetwork ->
                            rtn.networks += networks.findAll { it.LogicalNetwork == logicalNetwork.Name }
                        }
                    }
                } else {
                    if (out.exitValue != 0) {
                        log.info "Fetch of networks resulted in non-zero exit value: ${out}"
                    }
                }
            } else {
                log.info "Error in fetching network info: ${out}"
                rtn.success = false
            }
        } catch (ex) {
            rtn.success = false
            rtn.msg = "Error fetching all networks list from SCVMM Host"
            log.error("An error occurred attempting to list all networks on SCVMM Host: ${ex.message}", ex)
        }
        return rtn
    }

    def removeOrphanedResourceLibraryItems(opts) {
        log.debug "removeOrphanedResourceLibraryItems: ${opts}"

        def command = """
\$ISOs = Get-SCISO -VMMServer localhost | where { (\$_.State -match "Missing") -and (\$_.Directory.ToString() -like "*morpheus_server_*") }
\$ignore = \$ISOs | Remove-SCISO -RunAsynchronously

\$Scripts = Get-SCScript -VMMServer localhost | where { (\$_.State -match "Missing") -and (\$_.Directory.ToString() -like "*morpheus_server_*") }
\$ignore = \$Scripts | Remove-SCScript -RunAsynchronously"""
        def out = wrapExecuteCommand(generateCommandString(command), opts)
        if (!out.success) {
            log.warn "Error in removeOrphanedResourceLibraryItems: ${out}"
        }
    }

    def listNetworks(opts) {
        def rtn = [success: true, networks: []]
        try {
            def hasMore = true
            def pageSize = 50
            def fetch = { offset ->
                // Must grab the logical networks for the cloud.. then fetch the VMNetworks for each logical network
                def command
                if (opts.zone.regionCode) {
                    def commandStr = """
\$cloud = Get-SCCloud -VMMServer localhost | where { \$_.ID -eq \'${opts.zone.regionCode}\' }
Get-SCLogicalNetwork -VMMServer localhost -Cloud \$cloud | Select ID,Name"""
                    command = generateCommandString(commandStr)
                } else {
                    command = generateCommandString("Get-SCLogicalNetwork -VMMServer localhost | Select ID,Name")
                }

                def out = wrapExecuteCommand(command, opts)
                log.debug("listNetworks: ${out}")
                if (out.success && out.exitValue == 0 && out.data?.size() > 0) {
                    def logicalNetworks = out.data
                    command = generateCommandString("""\$report = @()
\$networks = Get-SCVMNetwork -VMMServer localhost | where {\$_.IsolationType -ne "NoIsolation"} | Select ID,Name,LogicalNetwork,VMSubnet | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
foreach (\$network in \$networks) {
	\$logicalNetwork = \$network.LogicalNetwork
	\$data = New-Object PSObject -property @{
		ID=\$network.ID
		Name=\$network.Name
		LogicalNetwork=\$logicalNetwork.Name
		Subnets=@()
	}
	\$subnets = \$network.VMSubnet.SubnetVLans | where {\$_.IsVlanEnabled -eq "True" }
	foreach (\$vlan in \$subnets) {	
		\$subnetData = New-Object PSObject -property @{
			ID=\$network.VMSubnet.ID.toString() + "-" + \$vlan.VLanId
			Name=\$network.VMSubnet.Name
			NetworkName=\$logicalNetwork.Name
			LogicalNetworkID=\$logicalNetwork.ID
			Subnet=\$vlan.Subnet
			VLanID=\$vlan.VLanId
		}
		\$data.Subnets += \$subnetData
	}
	\$report += \$data
}
\$report""")
                    out = wrapExecuteCommand(command, opts)
                    log.debug("get of networks: ${out}")
                    if (out.success && out.exitValue == 0) {
                        hasMore = out.data != ''
                        if (out.data) {
                            log.debug("list logical networks: ${out}")
                            def networks = out.data

                            logicalNetworks?.each { logicalNetwork ->
                                rtn.networks += networks.findAll { it.LogicalNetwork == logicalNetwork.Name }
                            }
                        }
                    } else {
                        if (out.exitValue != 0) {
                            log.info "Fetch of networks resulted in non-zero exit value: ${out}"
                        }
                        hasMore = false
                    }
                } else {
                    log.info "Error in fetching network info: ${out}"
                    hasMore = false
                    rtn.success = false
                    hasMore = false
                }
            }
            def currentOffset = 0
            while (hasMore) {
                fetch(currentOffset)
                currentOffset += pageSize
            }
        } catch (ex) {
            rtn.success = false
            rtn.msg = "Error syncing networks list from SCVMM Host"
            log.error("An error occurred attempting to list networks on SCVMM Host: ${ex.message}", ex)
        }
        return rtn
    }

    def listNoIsolationVLans(opts) {
        def rtn = [success: true, networks: []]
        try {
            def hasMore = true
            def pageSize = 50
            def fetch = { offset ->
                def command
                if (opts.zone.regionCode) {
                    def commandStr = """
\$cloud = Get-SCCloud -VMMServer localhost | where { \$_.ID -eq \'${opts.zone.regionCode}\' }
Get-SCLogicalNetwork -VMMServer localhost -Cloud \$cloud | Select ID,Name"""
                    command = generateCommandString(commandStr)
                } else {
                    command = generateCommandString("Get-SCLogicalNetwork -VMMServer localhost | Select ID,Name")
                }
                def out = wrapExecuteCommand(command, opts)
                log.debug("listNetworks: ${out}")
                if (out.success && out.exitValue == 0 && out.data?.size() > 0) {
                    def logicalNetworks = out.data
                    command = generateCommandString("""\$report = @()
\$logicalNetworks = Get-SCLogicalNetworkDefinition -VMMServer localhost | where {\$_.IsolationType -eq "None"} | Sort-Object -Property ID | Select-Object -Skip $offset -First $pageSize
foreach (\$logicalNetwork in \$logicalNetworks) {
	\$network = Get-SCVMNetwork -VMMServer localhost -LogicalNetwork \$logicalNetwork.LogicalNetwork 
	\$subnets = \$logicalNetwork.SubnetVLans | where {\$_.IsVlanEnabled -eq "True" }
	foreach (\$vlan in \$subnets) {	
		\$data = New-Object PSObject -property @{
			ID=\$network.ID.toString() + "-" + \$vlan.VLanId
			Name=\$vlan.VLanId.toString() + "-" + \$logicalNetwork.LogicalNetwork.Name
			NetworkName=\$logicalNetwork.Name
			LogicalNetworkID=\$logicalNetwork.LogicalNetwork.ID
			Subnet=\$vlan.Subnet
			VLanID=\$vlan.VLanId
		}
		\$report += \$data
	}
}
\$report""")
                    out = wrapExecuteCommand(command, opts)
                    log.debug("get of networks: ${out}")
                    if (out.success && out.exitValue == 0) {
                        hasMore = out.data != ''
                        if (out.data) {
                            log.debug("list logical networks: ${out}")
                            def networks = out.data

                            logicalNetworks?.each { logicalNetwork ->
                                rtn.networks += networks.findAll { it.LogicalNetworkID == logicalNetwork.ID }
                            }
                        }
                    } else {
                        if (out.exitValue != 0) {
                            log.info "Fetch of networks resulted in non-zero exit value: ${out}"
                        }
                        hasMore = false
                    }
                } else {
                    log.info "Error in fetching network info: ${out}"
                    hasMore = false
                    rtn.success = false
                    hasMore = false
                }
            }
            def currentOffset = 0
            while (hasMore) {
                fetch(currentOffset)
                currentOffset += pageSize
            }
        } catch (ex) {
            rtn.success = false
            rtn.msg = "Error syncing isolation networks list from SCVMM Host"
            log.error("An error occurred attempting to list isolation networks on SCVMM Host: ${ex.message}", ex)
        }
        return rtn
    }

    //
    def listNetworkIPPools(opts) {
        def rtn = [success: true, ipPools: [], networkMapping: []]
        try {
            // Fetch all the Static IP Address pools
            def command = generateCommandString("""\$report = @()   
\$staticPools = Get-SCStaticIPAddressPool -VMMServer localhost
foreach (\$staticPool in \$staticPools) {	
	\$data = New-Object PSObject -property @{
		ID=\$staticPool.ID
		Name=\$staticPool.Name
		NetworkID=\$staticPool.VMSubnet.VMNetwork.ID
		LogicalNetworkID=\$staticPool.LogicalNetworkDefinition.LogicalNetwork.ID
		Subnet=\$staticPool.Subnet
		SubnetID=\$staticPool.VMSubnet.ID
		DefaultGateways=@(\$staticPool.DefaultGateways.IPAddress)
		TotalAddresses=\$staticPool.TotalAddresses
		AvailableAddresses=\$staticPool.AvailableAddresses
		DNSSearchSuffixes=\$staticPool.DNSSearchSuffixes
		DNSServers=\$staticPool.DNSServers
		IPAddressRangeStart=\$staticPool.IPAddressRangeStart
		IPAddressRangeEnd=\$staticPool.IPAddressRangeEnd
	}
	\$report += \$data
}
\$report """)

            def out = wrapExecuteCommand(command, opts)
            log.debug("listNetworkIPPools: ${out}")
            if (out.success && out.exitValue == 0) {
                rtn.ipPools += out.data ?: []
            } else {
                rtn.success = false
            }

            if (rtn.success) {
                // Also fetch the mapping of networks to logical networks which is needed when mapping ip pools to networks
                command = generateCommandString("""\$report = @()   
\$networks = Get-SCVMNetwork -VMMServer localhost | Select ID,Name,LogicalNetwork
foreach (\$network in \$networks) {
	\$data = New-Object PSObject -property @{
		ID=\$network.ID
		Name=\$network.Name
		LogicalNetwork=\$network.LogicalNetwork.Name
		LogicalNetworkId=\$network.LogicalNetwork.ID
	}
	\$report += \$data
}
\$report """)
                out = wrapExecuteCommand(command, opts)
                log.debug("fetch network mapping: ${out}")
                if (out.success && out.exitValue == 0) {
                    rtn.networkMapping += out.data ?: []
                } else {
                    rtn.success = false
                }
            }
        } catch (ex) {
            rtn.success = false
            rtn.msg = "Error syncing ip pools list from SCVMM Host"
            log.error("An error occurred attempting to list ip pools on SCVMM Host: ${ex.message}", ex)
        }
        return rtn
    }

    def reserveIPAddress(opts, poolId) {
        def rtn = [success: true, ipAddress: []]
        try {
            def command = generateCommandString("""\$ippool = Get-SCStaticIPAddressPool -VMMServer localhost -ID \"$poolId\"; Grant-SCIPAddress -GrantToObjectType \"VirtualMachine\" -StaticIPAddressPool \$ippool | Select-Object ID,Address""")
            def out = wrapExecuteCommand(command, opts)
            log.debug("reserveIPAddress: ${out}")
            if (out.success && out.exitValue == 0) {
                def ipAddressBlock = out.data
                if (ipAddressBlock) {
                    rtn.ipAddress = ipAddressBlock.first()
                }
            } else {
                rtn.success = false
            }
        } catch (ex) {
            rtn.success = false
            rtn.msg = "Error reserving an IP address from SCVMM"
            log.error("Error reserving an IP address from SCVMM: ${ex.message}", ex)
        }
        return rtn
    }

    def releaseIPAddress(opts, poolId, ipId) {
        def rtn = [success: true]
        try {
            def command = generateCommandString("\$ippool = Get-SCStaticIPAddressPool -VMMServer localhost -ID \"$poolId\"; \$ipaddress = Get-SCIPAddress -ID \"$ipId\"; \$ignore = Revoke-SCIPAddress \$ipaddress")
            def out = wrapExecuteCommand(command, opts)
            log.info("releaseIPAddress: ${out}")
            if (out.success && out.exitValue == 0) {
                // Do nothing
            } else {
                if (out.errorData?.contains("Unable to find the specified allocated IP address")) {
                    // It has already been deleted somehow
                    rtn.success = true
                } else {
                    rtn.success = false
                }
            }
        } catch (ex) {
            rtn.success = false
            rtn.msg = "Error revoking an IP address from SCVMM"
            log.error("Error revoking an IP address from SCVMM: ${ex.message}", ex)
        }

        return rtn
    }

    def listVirtualDiskDrives(opts, externalId, name = null) {
        def rtn = [success: false, disks: []]
        def commands = []
        commands << "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${externalId}\""
        def getCmd = "\$disks = Get-SCVirtualDiskDrive -VM \$VM"
        if (name) {
            getCmd += " | where {\$_.VirtualHardDisk -like \"${name}\"}"
        }
        commands << getCmd
        commands << """
\$report = @()
foreach (\$disk in \$disks) {
  \$data = New-Object PSObject -property @{
    ID=\$disk.ID
    VhdID=\$disk.VirtualHardDisk.ID
    Name=\$disk.Name
    HostVolumeId=\$disk.VirtualHardDisk.HostVolumeId
	FileShareId=\$disk.VirtualHardDisk.FileShare.ID
	PartitionUniqueId=\$disk.VirtualHardDisk.HostVolume.PartitionUniqueId
  }
  \$report += \$data
}
\$report """
        def command = generateCommandString(commands.join(';'))
        def out = wrapExecuteCommand(command, opts)
        if (out.success) {
            rtn.disks = out.data
            rtn.success = true
        }
        return rtn
    }

    def resizeDisk(opts, diskId, diskSizeBytes) {
        log.debug "resizeDisk: ${diskId} ${diskSizeBytes}"
        def commands = []
        commands << "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${opts.externalId}\""
        commands << "\$VirtualDiskDrive = Get-SCVirtualDiskDrive -VM \$VM | where { \$_.VirtualHardDiskId -eq \"${diskId}\" }"
        commands << "\$ignore = Expand-SCVirtualDiskDrive -RunAsynchronously -JobVariable \"expandHD\" -VirtualDiskDrive \$VirtualDiskDrive -VirtualHardDiskSizeGB ${(int) (diskSizeBytes.toLong()).div(ComputeUtility.ONE_GIGABYTE)}"
        commands << "\$expandHD.ID"
        def cmd = commands.join(';')

        log.debug "resizeDisk: ${cmd}"
        def resizeResults = wrapExecuteCommand(generateCommandString(cmd), opts)
        def jobGuid = resizeResults.data?.getAt(0)?.value
        if (!jobGuid) {
            throw new Exception("Did not receive a job guid for resize disk: ${resizeResults}")
        }
        // Wait for the job to complete.. might take awhile!
        def waitResults = waitForJobToComplete(opts, jobGuid)
        return waitResults
    }


    def createAndAttachDisk(opts, dataDiskNumber, diskSizeBytes, busNumber, path = null, returnDiskDrives = true) {
        log.debug("createAndAttachDisk : opts: ${opts}, ${path}")
        def commands = []
        def sizeMB = (int) (diskSizeBytes.toLong()).div(ComputeUtility.ONE_MEGABYTE)
        def fileName = "data-${UUID.randomUUID().toString()}.vhd"

        def command = """
\$busNumber = ${busNumber}
\$lunNumber = 0
\$success = \$false
\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${opts.externalId}\"
\$startDiskCount=\$VM.VirtualDiskDrives.VirtualHardDisk.Count
For (\$i=0; \$i -le 63; \$i++) {
	\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${opts.externalId}\"
	\$currentDiskCount=\$VM.VirtualDiskDrives.VirtualHardDisk.Count
	If ((\$success -eq \$false) -and (\$currentDiskCount -lt \$startDiskCount + 1)) {
		\$jobGuid = New-Guid
"""
        if (path) {
            command += """
		\$ignore = New-SCVirtualDiskDrive -VMMServer localhost -SCSI -Bus \$busNumber -LUN \$lunNumber -JobGroup \$jobGuid -VirtualHardDiskSizeMB ${sizeMB} -CreateDiffDisk \$false -Dynamic -FileName \"$fileName\" -Path \"$path\" -VolumeType None
"""
        } else {
            command += """
		\$ignore = New-SCVirtualDiskDrive -VMMServer localhost -SCSI -Bus \$busNumber -LUN \$lunNumber -JobGroup \$jobGuid -VirtualHardDiskSizeMB ${sizeMB} -CreateDiffDisk \$false -Dynamic -FileName \"$fileName\" -VolumeType None
"""
        }

        command += """
		\$ignore = Set-SCVirtualMachine -VM \$VM -JobGroup \$jobGuid
		if( -not \$? ) {
			\$lunNumber = \$lunNumber + 1
			\$ignore = Repair-SCVirtualMachine -VM \$VM -Dismiss -Force
		} else {
			\$success = \$true
		}
	}
}
\$report = New-Object -Type PSObject -Property @{
	'success'=\$success
	'BUS'=\$busNumber
	'LUN'=\$lunNumber}
\$report"""

        def out = wrapExecuteCommand(generateCommandString(command), opts)
        if (out.success && returnDiskDrives) {
            def listResults = listVirtualDiskDrives(opts, opts.externalId, fileName)
            return [success: listResults.success, disk: listResults.disks.first()]
        }
    }

    def removeDisk(opts, diskId) {
        def commands = []
        def diskJobGuid = UUID.randomUUID().toString()
        commands << "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${opts.externalId}\""
        commands << "\$VirtualDiskDrive = Get-SCVirtualDiskDrive -VM \$VM | where { \$_.VirtualHardDiskId -eq \"${diskId}\" }"
        commands << "\$ignore = Remove-SCVirtualDiskDrive -VirtualDiskDrive \$VirtualDiskDrive -JobGroup ${diskJobGuid}"
        commands << "\$ignore = Set-SCVirtualMachine -VM \$VM -JobGroup ${diskJobGuid}"
        def cmd = commands.join(';')
        log.debug "removeDisk: ${cmd}"
        return wrapExecuteCommand(generateCommandString(cmd), opts)
    }

    def checkServerCreated(opts, vmId) {
        log.debug "checkServerCreated: ${vmId}"
        def rtn = [success: false]
        try {
            def pending = true
            def attempts = 0
            while (pending) {
                sleep(1000l * 5l)
                def serverDetail = getServerDetails(opts, vmId)
                if (serverDetail.success == true) {
                    // There isn't a state on the VM to tell us it is created.. but, if the disk size matches
                    // the expected count.. we are good
                    log.debug "serverStatus: ${serverDetail.server?.Status}, opts.dataDisks: ${opts.dataDisks?.size()}, additionalTemplateDisks: ${opts.additionalTemplateDisks?.size()}"
                    if (serverDetail.server?.Status != 'UnderCreation' &&
                            serverDetail.server?.VirtualDiskDrives?.size() == 1 + ((opts.dataDisks?.size() ?: 0) - (opts.additionalTemplateDisks?.size() ?: 0))) {
                        // additionalTemplateDisks are created after VM creation
                        rtn.success = true
                        rtn.server = serverDetail.server
                        pending = false

                        if (serverDetail.server?.Status == 'Saved') {
                            // Discard saved state... can't modify it if so
                            discardSavedState(opts, vmId)
                        }
                    } else if (serverDetail.server?.Status == 'CreationFailed') {
                        rtn.success = false
                        pending = false
                    }
                }
                attempts++
                if (attempts > 600)
                    pending = false
            }
        } catch (e) {
            log.error("An Exception Has Occurred", e)
        }
        return rtn
    }

    def waitForJobToComplete(opts, jobId) {
        def rtn = [success: false]
        try {
            log.debug "waitForJobToComplete: ${opts} ${jobId}"
            def pending = true
            def attempts = 0
            while (pending) {
                sleep(1000l * 5l)
                log.debug "waitForJobToComplete: ${jobId}"
                def getJobResults = getJob(opts, jobId)
                if (getJobResults.success == true && getJobResults.jobDetail) {

                    def status = getJobResults.jobDetail?.Status?.toLowerCase()
                    if (['completed', 'failed', 'succeedwithinfo'].indexOf(status) > -1) {
                        pending = false
                        if (status == 'completed' || status == 'succeedwithinfo') {
                            rtn.success = true
                        }
                    }
                }
                attempts++
                if (attempts > 350)
                    pending = false
            }
        } catch (e) {
            log.error("An Exception Has Occurred", e)
        }
        return rtn
    }

    def getJob(opts, jobId) {
        log.debug "getJob: ${jobId}"
        def rtn = [success: false, jobDetail: null]

        try {
            def command = """\$job = Get-SCJob -VMMServer localhost -ID \"${jobId}\"
\$report = New-Object PSObject -property @{
ID=\$job.ID
Name=\$job.Name
Progress=\$job.Progress
Status=\$job.Status.toString()
}
\$report"""
            def out = wrapExecuteCommand(generateCommandString(command), opts)
            if (!out.success) {
                throw new Exception("Error in getting job")
            }

            rtn.jobDetail = out.data.getAt(0)
            rtn.success = true
        } catch (e) {
            log.error "error in calling job detail: ${e}", e
        }

        return rtn
    }

    def checkServerReady(opts, vmId) {
        def rtn = [success: false]
        try {
            log.debug "checkServerReady: ${opts} ${vmId}"
            def pending = true
            def attempts = 0
            def notFoundAttempts = 0
            def serverId = opts.server.id
            def waitForIp = opts.waitForIp
            while (pending) {
                sleep(1000l * 5l)
                ComputeServer.withNewSession {
                    log.debug "checkServerReady: ${vmId}"
                    ComputeServer server = ComputeServer.get(serverId)
                    opts.server = server
                    // Refresh the VM in SCVMM (seems to be needed for it to get the IP for windows)
                    refreshVM(opts, vmId)
                    def serverDetail = getServerDetails(opts, vmId)
                    if (serverDetail.success == true && serverDetail.server) {
                        server.refresh()
                        def ipAddress = serverDetail.server?.internalIp ?: server?.externalIp
                        log.debug "ipAddress found: ${ipAddress}"
                        if (ipAddress) {
                            server.internalIp = ipAddress
                        }

                        if (waitForIp && !ipAddress) {
                            // Keep waiting
                        } else {
                            // Most likely, server gets its IP from cloud-init calling back to cloudconfigcontroller/ipaddress... wait for that to happen
                            // Or... if the desire is to NOT install the agent, then we are not expecting an IP address
                            if (serverDetail.server?.VirtualMachineState == 'Running') {
                                rtn.success = true
                                rtn.server = serverDetail.server
                                rtn.server.ipAddress = ipAddress ?: server?.internalIp
                                pending = false
                            } else if (serverDetail.server?.Status == 'CreationFailed') {
                                rtn.success = false
                                rtn.server = serverDetail.server
                                rtn.server.ipAddress = ipAddress ?: server?.internalIp
                                pending = false
                            } else {
                                server.refresh()
                                log.debug("check server loading server: ip: ${server.internalIp}")
                                if (server.internalIp) {
                                    rtn.success = true
                                    rtn.server = serverDetail.server
                                    rtn.server.ipAddress = ipAddress ?: server.internalIp
                                    pending = false
                                }
                            }
                        }
                    } else {
                        if (serverDetail.error == 'VM_NOT_FOUND') {
                            notFoundAttempts++
                        }
                    }
                }
                attempts++
                if (attempts > 300 || notFoundAttempts > 10)
                    pending = false
            }
        } catch (e) {
            log.error("An Exception Has Occurred", e)
        }
        return rtn
    }

    def startServer(opts, vmId) {
        def rtn = [success: false]
        try {
            // Only start if it isn't already running
            def serverDetail = getServerDetails(opts, vmId)
            if (serverDetail.success == true) {
                if (serverDetail.server?.VirtualMachineState != 'Running') {
                    def out = wrapExecuteCommand(generateCommandString("\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${vmId}\"; \$ignore = Start-SCVirtualMachine -VM \$VM ${opts.async ? '-RunAsynchronously' : ''}"), opts)
                    rtn.success = out.success
                } else {
                    rtn.msg = 'VM is already powered on'
                    rtn.success = true
                }
            }
        } catch (e) {
            log.error("startServer error: ${e}", e)
        }
        return rtn
    }

	def stopServer(opts, vmId) {
		def rtn = [success: false]
		try {
			def command = """\$VM = Get-SCVirtualMachine -VMMServer localhost  -ID \"${vmId}\"
if(\$VM.Status -ne 'PowerOff') { 
	\$ignore = Stop-SCVirtualMachine -VM \$VM; 
} \$true """
			def out = wrapExecuteCommand(generateCommandString(command), opts)
			rtn.success = out.success
		} catch (e) {
			log.error("stopServer error: ${e}", e)
		}
		return rtn
	}

    def deleteServer(opts, vmId) {
        def rtn = [success: false]
        try {
            def sharePath = opts.rootSharePath ?: getRootSharePath(opts)

            if (sharePath) {
                def serverFolder = "${sharePath}\\${opts.serverFolder}"
                def diskFolder = "${opts.diskRoot}\\${opts.serverFolder}"
                if (!opts.serverFolder) {
                    throw new Exception("serverFolder MUST be specified")
                }
                def command = """\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${vmId}\"
if(\$VM) { 
  \$ignore = Stop-SCVirtualMachine -VM \$VM -Force
  \$ignore = Remove-SCVirtualMachine -VM \$VM 
} 
\$ignore = Remove-Item -Path  \"${serverFolder}\" -Recurse -Force
\$ignore = Remove-Item -LiteralPath \"${diskFolder}\" -Recurse -Force"""
                def out = wrapExecuteCommand(generateCommandString(command), opts)
                rtn.success = true
            }
        } catch (e) {
            log.error("deleteServer error: ${e}", e)
        }
        return rtn
    }

    def importPhysicalResource(opts, sourcePath, imageFolderName, resourceName) {
        log.debug "importPhysicalResource: ${opts}, ${sourcePath}, ${imageFolderName}, ${resourceName}"
        def rtn = [success: false]
        def rootSharePath = opts.rootSharePath ?: getRootSharePath(opts)

        def sharePath = "${rootSharePath}\\$imageFolderName"
        def command = "New-Item -ItemType directory -Path \"${sharePath}\";Copy-Item -Path \"$sourcePath\" -Destination \"${sharePath}\\${resourceName}\""

        def attempts = 0
        def importOpts = [baseBoxProvisionService: opts.scvmmProvisionService, controllerServer: opts.controllerNode] + opts
        while (!rtn.success && attempts < 5) {
            def out = executeCommand(command, importOpts)
            rtn.success = out.success
            if (!rtn.success) {
                attempts++
                sleep(5000)
            }
        }

        if (!rtn.success) {
            throw new Exception("Error in importing physical resource: ${rtn}")
        } else {
            executeCommand("\$libraryshare = Get-SCLibraryShare -VMMServer localhost | where { \$_.Path -eq \"${rootSharePath}\" }; Read-SCLibraryShare -Path \"${sharePath}\" -LibraryShare \$libraryshare", importOpts)
        }
        rtn.success = true
        rtn.sharePath = "${sharePath}\\${resourceName}"

        return rtn
    }

    def getRootSharePath(opts) {
        def command = """\$report = @()
\$shares = Get-SCLibraryShare -VMMServer localhost 
foreach(\$share in \$shares) {
  \$data = New-Object PSObject -property @{
    ID=\$share.ID
    Name=\$share.Name
    Path=\$share.Path
}
\$report += \$data
}
\$report"""
        def out = wrapExecuteCommand(generateCommandString(command), opts)
        if (!out.success) {
            throw new Exception("Error in getting library share")
        }

        def shareBlocks = out.data
        if (shareBlocks.size() == 0) {
            throw new Exception("No library share found")
        }

        return shareBlocks.first().Path
    }

    def deleteIso(opts, sharePath) {
        def commands = []
        commands << "\$iso = Get-SCISO -VMMServer localhost | where {\$_.SharePath -eq \"$sharePath\"}"
        commands << "\$ignore = Remove-SCISO -ISO \$iso -Force"
        return wrapExecuteCommand(generateCommandString(commands.join(';')), opts)
    }

    def deleteUnattend(opts, unattendPath) {
        def commands = []
        commands << "Remove-Item -Path \"${unattendPath}\" -Force"
        return wrapExecuteCommand(generateCommandString(commands.join(';')), opts)
    }

    def setCdrom(opts, cdPath = null) {
        log.debug("setCdrom: ${cdPath}")
        def commands = []
        commands << "\$vm = Get-SCVirtualMachine -VMMServer localhost -ID \"$opts.externalId\""
        commands << "\$dvd = Get-SCVirtualDVDDrive -VM \$vm"
        if (cdPath) {
            commands << "\$iso = Get-SCISO -VMMServer localhost | where {\$_.SharePath -eq \"$cdPath\"}"
            commands << "\$ignore = Set-SCVirtualDVDDrive -VirtualDVDDrive \$dvd -Bus \$dvd.Bus -LUN \$dvd.Lun -NoMedia"
            commands << "\$ignore = Set-SCVirtualDVDDrive -VirtualDVDDrive \$dvd -Bus \$dvd.Bus -LUN \$dvd.Lun -ISO \$iso"
        } else {
            commands << "\$ignore = Set-SCVirtualDVDDrive -VirtualDVDDrive \$dvd -Bus \$dvd.Bus -LUN \$dvd.Lun -NoMedia"
        }
        return wrapExecuteCommand(generateCommandString(commands.join(';')), opts)
    }

    def importScript(content, diskFolder, imageFolderName, opts) {
        log.debug "importScript: ${diskFolder}, ${imageFolderName}, ${opts}"
        def scriptPath
        def importAction = [inline: true, action: 'rawfile', content: content.encodeAsBase64(), targetPath: "${diskFolder}\\${opts.fileName}".toString(), opts: [:]]
        def importPromise = opts.scvmmProvisionService.commandService.sendAction(opts.hypervisor, importAction)
        def importResult = importPromise.get(1000l * 60l * 3l)
        if (!importResult.success) {
            throw new Exception("Script Upload to SCVMM Host Failed. Perhaps an agent communication issue...${opts.hypervisor.name}")
        }
        def importResults = importPhysicalResource(opts, importAction.targetPath, imageFolderName, opts.fileName)
        scriptPath = importResults.sharePath
        return scriptPath
    }

    def createDVD(opts) {
        log.debug "createDVD: ${opts.externalId}"

        // If gen2... ALWAYS -Bus 0
        // If gen1... ALWAYS -Bus 1

        def busNumber = 0
        def lunNumber = opts.scvmmGeneration == 'generation1' ? 0 : 1

        def command = """\$busNumber = ${busNumber}
\$lunNumber = ${lunNumber}
\$externalId = "${opts.externalId}"
\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \$externalId
\$success = \$false
For (\$i=0; \$i -le 10; \$i++) {
	If (\$success -eq \$false) {
		\$jobGuid = New-Guid
		\$ignore = New-SCVirtualDVDDrive -VMMServer localhost -JobGroup \$jobGuid -Bus \$busNumber -LUN \$lunNumber
		\$ignore = Set-SCVirtualMachine -VM \$VM -JobGroup \$jobGuid
		if( -not \$? ) {
			\$lunNumber = \$lunNumber + 1
			\$ignore = Repair-SCVirtualMachine -VM \$VM -Dismiss -Force
		} else {
			\$success = \$true
		}
	}
}

\$report = New-Object -Type PSObject -Property @{
	'success'=\$success
	'BUS'=\$busNumber
	'LUN'=\$lunNumber}
\$report"""

        def out = wrapExecuteCommand(generateCommandString(command), opts)
        if (!out.success) {
            log.warn "Error in creating a DVD: ${out}"
        }
    }

    def importAndMountIso(cloudConfigBytes, diskFolder, imageFolderName, opts) {
        log.debug "importAndMountIso: ${diskFolder}, ${imageFolderName}, ${opts}"
        def cloudInitIsoPath
        def isoAction = [inline: true, action: 'rawfile', content: cloudConfigBytes.encodeAsBase64(), targetPath: "${diskFolder}\\config.iso".toString(), opts: [:]]
        def isoPromise = opts.scvmmProvisionService.commandService.sendAction(opts.hypervisor, isoAction)
        def isoUploadResult = isoPromise.get(1000l * 60l * 3l)
        if (!isoUploadResult.success) {
            throw new Exception("ISO Upload to SCVMM Host Failed. Perhaps an agent communication issue...${opts.hypervisor.name}")
        }
        def importResults = importPhysicalResource(opts, isoAction.targetPath, imageFolderName, 'config.iso')
        cloudInitIsoPath = importResults.sharePath
        setCdrom(opts, cloudInitIsoPath)
        return cloudInitIsoPath
    }

    def toList(value) {
        [value].flatten()
    }

    def validateServerConfig(Map opts = [:]) {
        log.debug("validateServerConfig: ${opts}")
        def rtn = [success: false, errors: []]
        try {
            if (!opts.scvmmCapabilityProfile) {
                rtn.errors += [field: 'scvmmCapabilityProfile', msg: 'You must select a capability profile']
            }
            // if(!opts.networkId && opts.networkInterfaces?.size() == 0) {
            // 	rtn.errors += [field: 'networkInterface', msg: 'You must choose a network']
            // }
            if (opts.networkId) {
                // great
            } else if (opts?.networkInterfaces) {
                // JSON (or Map from parseNetworks)
                log.debug("validateServerConfig networkInterfaces: ${opts?.networkInterfaces}")
                opts?.networkInterfaces?.eachWithIndex { nic, index ->
                    def networkId = nic.network?.id ?: nic.network.group
                    log.debug("network.id: ${networkId}")
                    if (!networkId) {
                        rtn.errors << [field: 'networkInterface', msg: 'Network is required']
                    }
                    if (nic.ipMode == 'static' && !nic.ipAddress) {
                        rtn.errors = [field: 'networkInterface', msg: 'You must enter an ip address']
                    }
                }
            } else if (opts?.networkInterface) {
                // UI params
                log.debug("validateServerConfig networkInterface: ${opts.networkInterface}")
                toList(opts?.networkInterface?.network?.id)?.eachWithIndex { networkId, index ->
                    log.debug("network.id: ${networkId}")
                    if (networkId?.length() < 1) {
                        rtn.errors << [field: 'networkInterface', msg: 'Network is required']
                    }
                    if (networkInterface[index].ipMode == 'static' && !networkInterface[index].ipAddress) {
                        rtn.errors = [field: 'networkInterface', msg: 'You must enter an ip address']
                    }
                }
            } else {
                rtn.errors << [field: 'networkId', msg: 'Network is required']
            }
            if (opts.containsKey('nodeCount') && opts.nodeCount == '') {
                rtn.errors += [field: 'nodeCount', msg: 'You must indicate number of hosts']
            }
            rtn.success = (rtn.errors.size() == 0)
            log.debug "validateServer results: ${rtn}"
        } catch (e) {
            log.error "error in validateServerConfig: ${e}", e
        }
        return rtn
    }

    def updateServer(opts, vmId, updates = [:]) {
        log.debug("updateServer: vmId: ${vmId}, updates: ${updates}")
        def rtn = [success: false]
        try {
            def minDynamicMemory = updates.minDynamicMemory ? (int) updates.minDynamicMemory.div(ComputeUtility.ONE_MEGABYTE) : null
            def maxDynamicMemory = updates.maxDynamicMemory ? (int) updates.maxDynamicMemory.div(ComputeUtility.ONE_MEGABYTE) : null

            if (updates.maxMemory || updates.maxCores || minDynamicMemory || maxDynamicMemory) {
                def command = "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${vmId}\";"
                if (updates.maxCores) {
                    command += "\$ignore = Set-SCVirtualMachine -VM \$VM -CPUCount ${updates.maxCores};"
                }

                if (updates.maxMemory) {
                    def maxMemory = (int) updates.maxMemory.div(ComputeUtility.ONE_MEGABYTE)
                    command += "\$maxMemory = ${maxMemory};"
                    command += "\$minDynamicMemory = ${minDynamicMemory ?: '$null'};"
                    command += "\$maxDynamicMemory = ${maxDynamicMemory ?: '$null'};"
                    command += "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${vmId}\";"
                    command += "if(\$minDynamicMemory -and \$maxDynamicMemory -eq \$null) { \$maxDynamicMemory = [int32]::MaxValue };"
                    command += "if(\$maxDynamicMemory -and \$minDynamicMemory -eq \$null) { \$minDynamicMemory = 32 };"
                    command += "\$dynamicMemoryEnabled = (\$minDynamicMemory -ne \$null);"
                    command += "if(\$dynamicMemoryEnabled -eq \$true -and \$minDynamicMemory -gt \$maxMemory) { \$maxMemory = \$minDynamicMemory };"
                    command += "if(\$dynamicMemoryEnabled) { \$ignore = Set-SCVirtualMachine -VM \$VM -DynamicMemoryEnabled \$true -MemoryMB \$maxMemory -DynamicMemoryMinimumMB \$minDynamicMemory -DynamicMemoryMaximumMB \$maxDynamicMemory } else { \$ignore = Set-SCVirtualMachine -VM \$VM -DynamicMemoryEnabled \$false -MemoryMB \$maxMemory };"
                    command += "\$true"
                    // Add logic to handle dynamic memory... if the startup memory is lower than dynamic max memory, it won't start.  So, set them equal
                }

                log.debug "updateServer: ${command}"
                def out = wrapExecuteCommand(generateCommandString(command), opts)
                log.debug "updateServer results: ${out}"
                rtn.success = out.success && out.exitValue == 0
            } else {
                log.debug("No updates for server: ${vmId}")
                rtn.success = true
            }
        } catch (e) {
            log.error "updateServer error: ${e}", e
        }
        return rtn
    }

    def cleanData(data, ignoreString = null) {
        def rtn = ''
        def lines = data.tokenize('\n')
        lines = lines?.findAll { it?.trim()?.length() > 1 }
        if (lines?.size() > 0) {
            lines?.each { line ->
                def trimLine = line.trim()
                if (rtn == null && ignoreString == null || trimLine != ignoreString)
                    rtn = trimLine
            }
        }
        return rtn
    }

    def getMapScvmmOsType(searchFor, findByKey = true, defaultOsType = null) {
        def SCVMM_OS_TYPE_MAP = [
                '64-bit edition of Windows 10'                       : 'windows.10.64',
                '64-bit edition of Windows 7'                        : 'windows.7.64',
                '64-bit edition of Windows 8'                        : 'windows.8.64',
                '64-bit edition of Windows 8.1'                      : 'windows.8.64',
                '64-bit edition of Windows Server 2008 Datacenter'   : 'windows.server.2008',
                '64-bit edition of Windows Server 2008 Enterprise'   : 'windows.server.2008',
                '64-bit edition of Windows Server 2008 R2 Datacenter': 'windows.server.2008.r2',
                '64-bit edition of Windows Server 2008 R2 Enterprise': 'windows.server.2008.r2',
                '64-bit edition of Windows Server 2008 R2 Standard'  : 'windows.server.2008.r2',
                '64-bit edition of Windows Server 2008 Standard'     : 'windows.server.2008',
                '64-bit edition of Windows Server 2012 Datacenter'   : 'windows.server.2012',
                '64-bit edition of Windows Server 2012 Essentials'   : 'windows.server.2012',
                '64-bit edition of Windows Server 2012 Standard'     : 'windows.server.2012',
                '64-bit edition of Windows Vista'                    : 'windows',
                '64-bit edition of Windows Web Server 2008'          : 'windows.server.2008',
                '64-bit edition of Windows Web Server 2008 R2'       : 'windows.server.2008.r2',
                'CentOS Linux 5 (32 bit)'                            : 'cent',
                'CentOS Linux 5 (64 bit)'                            : 'cent',
                'CentOS Linux 6 (32 bit)'                            : 'cent.6',
                'CentOS Linux 6 (64 bit)'                            : 'cent.6.64',
                'CentOS Linux 7 (64 bit)'                            : 'cent.7.64',
                'Debian GNU/Linux 7 (32 bit)'                        : 'debian.7',
                'Debian GNU/Linux 7 (64 bit)'                        : 'debian.6.64',
                'Debian GNU/Linux 8 (32 bit)'                        : 'debian.8',
                'Debian GNU/Linux 8 (64 bit)'                        : 'debian.8.64',
                'None'                                               : 'other',
                'Novell NetWare 5.1'                                 : 'other',
                'Novell NetWare 6.x'                                 : 'other',
                'Open Enterprise Server'                             : 'other',
                'Oracle Linux 5 (32 bit)'                            : 'oracle.32',
                'Oracle Linux 5 (64 bit)'                            : 'oracle.linux.64',
                'Oracle Linux 6 (32 bit)'                            : 'oracle.32',
                'Oracle Linux 6 (64 bit)'                            : 'oracle.linux.64',
                'Oracle Linux 7 (64 bit)'                            : 'oracle.linux.64',
                'Other (32 bit)'                                     : 'other.32',
                'Other (64 bit)'                                     : 'other.64',
                'Other Linux (32 bit)'                               : 'linux.32',
                'Other Linux (64 bit)'                               : 'linux.64',
                'Red Hat Enterprise Linux 2'                         : 'redhat',
                'Red Hat Enterprise Linux 3'                         : 'redhat',
                'Red Hat Enterprise Linux 3 (64 bit)'                : 'redhat',
                'Red Hat Enterprise Linux 4'                         : 'redhat',
                'Red Hat Enterprise Linux 4 (64 bit)'                : 'redhat',
                'Red Hat Enterprise Linux 5'                         : 'redhat',
                'Red Hat Enterprise Linux 5 (64 bit)'                : 'redhat',
                'Red Hat Enterprise Linux 6'                         : 'redhat.6',
                'Red Hat Enterprise Linux 6 (64 bit)'                : 'redhat.64',
                'Red Hat Enterprise Linux 7 (64 bit)'                : 'redhat.7.64',
                'Sun Solaris 10 (32 bit)'                            : 'linux.32',
                'Sun Solaris 10 (64 bit)'                            : 'linux.64',
                'Suse Linux Enterprise Server 10 (32 bit)'           : 'suse',
                'Suse Linux Enterprise Server 10 (64 bit)'           : 'suse',
                'Suse Linux Enterprise Server 11 (32 bit)'           : 'suse',
                'Suse Linux Enterprise Server 11 (64 bit)'           : 'suse',
                'Suse Linux Enterprise Server 12 (64 bit)'           : 'suse.12.64',
                'Suse Linux Enterprise Server 9 (32 bit)'            : 'suse',
                'Suse Linux Enterprise Server 9 (64 bit)'            : 'suse',
                'Ubuntu Linux (32 bit)'                              : 'ubuntu',
                'Ubuntu Linux (64 bit)'                              : 'ubuntu.64',
                'Ubuntu Linux 12.04 (32 bit)'                        : 'ubuntu.12.04',
                'Ubuntu Linux 12.04 (64 bit)'                        : 'ubuntu.12.04.64',
                'Ubuntu Linux 14.04 (32 bit)'                        : 'ubuntu.14.04',
                'Ubuntu Linux 14.04 (64 bit)'                        : 'ubuntu.14.04.64',
                'Ubuntu Linux 16.04 (32 bit)'                        : 'ubuntu',
                'Ubuntu Linux 16.04 (64 bit)'                        : 'ubuntu.64',
                'Windows 10'                                         : 'windows.10',
                'Windows 2000 Advanced Server'                       : 'windows',
                'Windows 2000 Server'                                : 'windows',
                'Windows 7'                                          : 'windows.7',
                'Windows 8'                                          : 'windows.8',
                'Windows 8.1'                                        : 'windows.8',
                'Windows NT Server 4.0'                              : 'windows',
                'Windows Server 2003 Datacenter Edition (32-bit x86)': 'windows',
                'Windows Server 2003 Datacenter x64 Edition'         : 'windows',
                'Windows Server 2003 Enterprise Edition (32-bit x86)': 'windows',
                'Windows Server 2003 Enterprise x64 Edition'         : 'windows',
                'Windows Server 2003 Standard Edition (32-bit x86)'  : 'windows',
                'Windows Server 2003 Standard x64 Edition'           : 'windows',
                'Windows Server 2003 Web Edition'                    : 'windows',
                'Windows Server 2008 Datacenter 32-Bit'              : 'windows.server.2008',
                'Windows Server 2008 Enterprise 32-Bit'              : 'windows.server.2008',
                'Windows Server 2008 Standard 32-Bit'                : 'windows.server.2008',
                'Windows Server 2012 R2 Datacenter'                  : 'windows.server.2012',
                'Windows Server 2012 R2 Essentials'                  : 'windows.server.2012',
                'Windows Server 2012 R2 Standard'                    : 'windows.server.2012',
                'Windows Server 2016 Datacenter'                     : 'windows.server.2016',
                'Windows Server 2016 Essentials'                     : 'windows.server.2016',
                'Windows Server 2016 Standard'                       : 'windows.server.2016',
                'Windows Server 2019 Datacenter'                     : 'windows.server.2019',
                'Windows Server 2019 Essentials'                     : 'windows.server.2019',
                'Windows Server 2019 Standard'                       : 'windows.server.2019',
                'Windows Small Business Server 2003'                 : 'windows',
                'Windows Vista'                                      : 'windows',
                'Windows Web Server 2008'                            : 'windows.8',
                'Windows XP 64-Bit Edition'                          : 'windows',
                'Windows XP Professional'                            : 'windows',
                ''                                                   : 'other'
        ]

        if (findByKey) {
            return SCVMM_OS_TYPE_MAP[searchFor] ?: defaultOsType ?: 'other'
        } else {
            // Passed in the value... find the key
            def found = SCVMM_OS_TYPE_MAP.find { k, v -> v == searchFor }
            return found?.key
        }
    }

    def findImage(opts, imageName) {
        def rtn = [success: false, imageExists: false]
        def zoneRoot = opts.zoneRoot ?: defaultRoot
        def imageFolder = formatImageFolder(imageName)
        def imageFolderPath = "${zoneRoot}\\images\\${imageFolder}"
        def command = "(Get-ChildItem -File \"${imageFolderPath}\").FullName"
        log.debug("findImage command: ${command}")
        def out = executeCommand(command, opts)
        log.debug("findImage: ${out.data}")
        rtn.success = out.success
        if (out.data?.length() > 0) {
            rtn.imageExists = true
            rtn.imageName = out.data.trim()
        }
        return rtn
    }

    def deleteImage(opts, imageName) {
        def rtn = [success: false, imageExists: false]
        def zoneRoot = opts.zoneRoot ?: defaultRoot
        def imageFolder = formatImageFolder(imageName)
        def imageFolderPath = "${zoneRoot}\\images\\${imageFolder}"
        def command = "Remove-Item -LiteralPath \"${imageFolderPath}\" -Recurse -Force"
        log.debug("deleteImage command: ${command}")
        def out = wrapExecuteCommand(generateCommandString(command), opts)
        log.debug("deleteImage: ${out.data}")
        rtn.success = out.success
        return rtn
    }

    def transferImage(opts, cloudFiles, imageName) {
        def rtn = [success: false, results: []]
        CloudFile metadataFile = (CloudFile) cloudFiles?.findAll { cloudFile -> cloudFile.name == 'metadata.json' }
        def vhdFiles = cloudFiles?.findAll { cloudFile -> cloudFile.name.indexOf('.vhd') > -1 }
        log.debug("vhdFiles: ${vhdFiles}")
        def zoneRoot = opts.zoneRoot ?: defaultRoot
        def imageFolderName = formatImageFolder(imageName)
        def fileList = []
        def tgtFolder = "${zoneRoot}\\images\\${imageFolderName}"
        opts.targetImageFolder = tgtFolder
        def cachePath = opts.cachePath
        def command = "\$ignore = mkdir \"${tgtFolder}\""
        log.debug("command: ${command}")
        def dirResults = wrapExecuteCommand(generateCommandString(command), opts)

        if (metadataFile) {
            fileList << [inputStream: metadataFile.inputStream, contentLength: metadataFile.contentLength, targetPath: "${tgtFolder}\\metadata.json".toString(), copyRequestFileName: "metadata.json"]
        }
        vhdFiles.each { CloudFile vhdFile ->
            def imageFileName = extractImageFileName(vhdFile.name)
            def filename = extractFileName(vhdFile.name)
            fileList << [inputStream: vhdFile.inputStream, contentLength: vhdFile.getContentLength(), targetPath: "${tgtFolder}\\${imageFileName}".toString(), copyRequestFileName: filename]
        }
        fileList.each { Map fileItem ->
            Long contentLength = (Long) fileItem.contentLength
            def fileResults = morpheusContext.services.fileCopy.copyToServer(opts.hypervisor, fileItem.copyRequestFileName, fileItem.targetPath, fileItem.inputStream, contentLength, null, true)
            rtn.success = fileResults.success
        }

        return rtn
    }

    def snapshotServer(opts, vmId) {
        def rtn = [success: false]
        try {
            def snapshotId = opts.snapshotId ?: "${vmId}.${System.currentTimeMillis()}"
            def command = "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${vmId}\"; \$ignore = New-SCVMCheckpoint -VM \$VM -Name \"${snapshotId}\""
            def out = wrapExecuteCommand(generateCommandString(command), opts)
            rtn.success = out.success && out.exitValue == 0
            rtn.snapshotId = snapshotId
            log.debug("snapshot server: ${out}")
        } catch (e) {
            log.error("snapshotServer error: ${e}")
        }

        return rtn
    }

    def deleteSnapshot(opts, vmId, snapshotId) {
        def rtn = [success: false]
        try {
            def commands = []
            commands << "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${vmId}\""
            commands << "\$Checkpoint = Get-SCVMCheckpoint -VM \$VM | where {\$_.Name -like \"${snapshotId}\"}"
            commands << "\$ignore = Remove-SCVMCheckpoint -VMCheckpoint \$Checkpoint"
            def out = wrapExecuteCommand(generateCommandString(commands.join(';')), opts)
            rtn.success = out.success && out.exitValue == 0
            rtn.snapshotId = snapshotId
            log.debug("delete snapshot: ${out}")
        } catch (e) {
            log.error("deleteSnapshot error: ${e}")
        }

        return rtn
    }

    def restoreServer(opts, vmId, snapshotId) {
        def rtn = [success: false]
        try {
            def commands = []
            commands << "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${vmId}\""
            commands << "\$Checkpoint = Get-SCVMCheckpoint -VM \$VM | where {\$_.Name -like \"${snapshotId}\"}"
            commands << "Restore-SCVMCheckpoint -VMCheckpoint \$Checkpoint"
            def out = wrapExecuteCommand(generateCommandString(commands.join(';')), opts)
            rtn.success = out.success && out.exitValue == 0
            log.debug("restore server: ${out}")
        } catch (e) {
            log.error("restoreServer error: ${e}")
        }

        return rtn
    }

    def changeVolumeTypeForClonedBootDisk(opts, originalVMId, newVMId) {
        def rtn = [success: false]
        try {
            def commands = []
            commands << "\$ClonedVM = Get-SCVirtualMachine -VMMServer localhost -ID \"$originalVMId\""
            commands << "\$OriginalBootDisk = Get-SCVirtualDiskDrive -VMMServer localhost -VM \$ClonedVM | where {\$_.VolumeType -eq \"BootAndSystem\"}"
            commands << "\$NewVM = Get-SCVirtualMachine -VMMServer localhost -ID \"$newVMId\""
            commands << "\$ClonedBootDisk = Get-SCVirtualDiskDrive -VMMServer localhost -VM \$NewVM | where {\$_.VirtualHardDisk -like [io.path]::GetFileNameWithoutExtension(\$OriginalBootDisk.VirtualHardDisk)}"
            commands << "Set-SCVirtualDiskDrive -VirtualDiskDrive \$ClonedBootDisk -VolumeType BootAndSystem"

            def out = wrapExecuteCommand(generateCommandString(commands.join(';')), opts)
            rtn.success = out.success && out.exitValue == 0
            log.debug("changeVolumeTypeForClonedBootDisk: ${out}")
        } catch (e) {
            log.error("changeVolumeTypeForClonedBootDisk error: ${e}")
        }

        return rtn
    }


    def buildCreateServerCommands(opts) {
        log.debug "buildCreateServerCommands: ${opts}"
        def rtn = [launchCommand: null, hardwareProfileName: '', templateName: '']
        def commands = []

        def hardwareGuid = UUID.randomUUID().toString()
        def networkConfig = opts.networkConfig
        def scvmmCapabilityProfile = opts.scvmmCapabilityProfile
        def scvmmGeneration = opts.scvmmGeneration ?: 'generation1'
        def hardwareProfileName = "Profile${UUID.randomUUID().toString()}"
        def maxCores = opts.maxCores
        def memoryMB = (int) opts.memory.div(ComputeUtility.ONE_MEGABYTE)
        def minDynamicMemoryMB = opts.minDynamicMemory ? (int) opts.minDynamicMemory.div(ComputeUtility.ONE_MEGABYTE) : null
        def maxDynamicMemoryMB = opts.maxDynamicMemory ? (int) opts.maxDynamicMemory.div(ComputeUtility.ONE_MEGABYTE) : null
        def zone = opts.zone
        def cloneVMId = opts.cloneVMId
        def vmId = opts.vmId
        def imageId = opts.imageId
        def templateName = "Temporary Morpheus Template ${UUID.randomUUID().toString()}"
        def dataDisks = opts.dataDisks
        def hostExternalId = opts.hostExternalId
        def volumePath = opts.volumePath
        def highlyAvailable = opts.highlyAvailable
        def isSyncdImage = opts.isSyncdImage
        def diskExternalIdMappings = opts.diskExternalIdMappings
        def isSysprep = opts.isSysprep
        def unattendPath = opts.unattendPath
        def OSName = opts.OSName
        def isTemplate = opts.isTemplate
        def templateId = opts.templateId
        def deployingToCloud = opts.zone.regionCode ? true : false
        def volumePaths = (opts.volumePaths && opts.volumePaths?.size() == 1 + dataDisks?.size()) ? opts.volumePaths : null

        // Static v DHCP
        def doStatic = networkConfig?.doStatic
        def doPool = doStatic && networkConfig?.primaryInterface?.poolType == 'scvmm'
        def ipAddress = networkConfig?.primaryInterface?.ipAddress
        def poolId = networkConfig?.primaryInterface?.networkPool?.externalId
        def vlanEnabled = networkConfig.primaryInterface?.vlanId > 0
        def vlanId = networkConfig.primaryInterface?.vlanId
        // network may be a vlan network... therefore, the externalId includes the VLAN id.. need to remove it
        def networkExternalId = networkConfig.primaryInterface.network.externalId?.take(36)
        def subnetExternalId = networkConfig.primaryInterface.subnet?.externalId?.take(36)

        if (isTemplate && templateId) {
            commands << "\$template = Get-SCVMTemplate -VMMServer localhost | where {\$_.ID -eq \"$templateId\"}"
        }
        // mac settings
        def hasMACAddress = false
        if (doStatic && doPool) {  // This seems weird.. why does the static networking affect the MAC setting?
            hasMACAddress = true
            commands << "\$MACAddress = \"00:00:00:00:00:00\""
            commands << "\$MACAddressType = \"Static\""
        } else if (isTemplate && templateId) {
            // Fetch the MAC settings from the template
            commands << "\$MACAddressTypeSetting = If (-not ([string]::IsNullOrEmpty(\$template.VirtualNetworkAdapters.MACAddressType))) { \$template.VirtualNetworkAdapters.MACAddressType} Else { \"Dynamic\" }"

            commands << "if( \$MACAddressTypeSetting -eq \"Static\") { \$MACAddress = \"00:00:00:00:00:00\"; \$MACAddressType = \"Static\"; }"

            commands << "if( \$MACAddressTypeSetting -eq \"Dynamic\") { \$MACAddress = \"\"; \$MACAddressType = \"Dynamic\"; }"
        } else {
            commands << "\$MACAddress = \"\""
            commands << "\$MACAddressType = \"Dynamic\""
        }

        commands << "\$ignore = New-SCVirtualScsiAdapter -VMMServer localhost -JobGroup $hardwareGuid -AdapterID 7 -ShareVirtualScsiAdapter \$false -ScsiControllerType DefaultTypeNoType"
        commands << "\$VMNetwork = Get-SCVMNetwork -VMMServer localhost -ID \"${networkExternalId}\""
        if (subnetExternalId) {
            commands << "\$VMSubnet = Get-SCVMSubnet -VMMServer localhost -ID \"${subnetExternalId}\""
        }
        commands << "If (-not ([string]::IsNullOrEmpty(\$MACAddress))) {"
        commands << "\$ignore = New-SCVirtualNetworkAdapter -VMMServer localhost -JobGroup $hardwareGuid -MACAddress \$MACAddress -MACAddressType \$MACAddressType -VLanEnabled ${vlanEnabled ? "\$true" : "\$false"} ${vlanEnabled ? "-VLanID ${vlanId}" : ''} -Synthetic -EnableVMNetworkOptimization \$false -EnableMACAddressSpoofing \$false -EnableGuestIPNetworkVirtualizationUpdates \$false -IPv4AddressType ${doStatic && doPool ? 'Static' : 'Dynamic'} -IPv6AddressType Dynamic ${subnetExternalId ? '-VMSubnet \$VMSubnet' : ''} -VMNetwork \$VMNetwork"
        commands << "} else {"
        commands << "\$ignore = New-SCVirtualNetworkAdapter -VMMServer localhost -JobGroup $hardwareGuid -MACAddressType \$MACAddressType -VLanEnabled ${vlanEnabled ? "\$true" : "\$false"} ${vlanEnabled ? "-VLanID ${vlanId}" : ''} -Synthetic -EnableVMNetworkOptimization \$false -EnableMACAddressSpoofing \$false -EnableGuestIPNetworkVirtualizationUpdates \$false -IPv4AddressType ${doStatic && doPool ? 'Static' : 'Dynamic'} -IPv6AddressType Dynamic ${subnetExternalId ? '-VMSubnet \$VMSubnet' : ''} -VMNetwork \$VMNetwork"
        commands << "}"

        if (scvmmCapabilityProfile) {
            commands << "\$CapabilityProfile = Get-SCCapabilityProfile -VMMServer localhost | where {\$_.Name -eq \"${scvmmCapabilityProfile?.trim()}\"}"
        }

        // Generation
        def generationNumber = !scvmmGeneration || scvmmGeneration == 'generation1' ? '1' : '2'
        if (isTemplate && templateId) {
            // Copying all of the hardware profiles from the existing template over
            commands << "\$CPUExpectedUtilizationPercent = If (-not ([string]::IsNullOrEmpty(\$template.CPUExpectedUtilizationPercent))) {\$template.CPUExpectedUtilizationPercent} Else { 20 }"
            commands << "\$DiskIops = If (-not ([string]::IsNullOrEmpty(\$template.DiskIops))) {\$template.DiskIops} Else { 0 }"
            commands << "\$CPUMaximumPercent = If (-not ([string]::IsNullOrEmpty(\$template.CPUMaximumPercent))) {\$template.CPUMaximumPercent} Else { 100 }"
            commands << "\$NetworkUtilizationMbps = If (-not ([string]::IsNullOrEmpty(\$template.NetworkUtilizationMbps))) {\$template.NetworkUtilizationMbps} Else { 0 }"
            commands << "\$CPURelativeWeight = If (-not ([string]::IsNullOrEmpty(\$template.CPURelativeWeight))) {\$template.CPURelativeWeight} Else { 100 }"
            commands << "\$DynamicMemoryEnabled = If (-not ([string]::IsNullOrEmpty(\$template.DynamicMemoryEnabled))) {\$template.DynamicMemoryEnabled} Else { \$false }"
            commands << "\$MemoryWeight = If (-not ([string]::IsNullOrEmpty(\$template.MemoryWeight))) {\$template.MemoryWeight} Else { 5000 }"
            commands << "\$VirtualVideoAdapterEnabled = If (-not ([string]::IsNullOrEmpty(\$template.VirtualVideoAdapterEnabled))) {\$template.VirtualVideoAdapterEnabled} Else { \$false }"
            commands << "\$CPUReserve = If (-not ([string]::IsNullOrEmpty(\$template.CPUReserve))) {\$template.CPUReserve} Else { 0 }"
            commands << "\$NumaIsolationRequired = If (-not ([string]::IsNullOrEmpty(\$template.NumaIsolationRequired))) {\$template.NumaIsolationRequired} Else { \$false }"
            commands << "\$DRProtectionRequired = If (-not ([string]::IsNullOrEmpty(\$template.DRProtectionRequired))) {\$template.DRProtectionRequired} Else { \$false }"
            commands << "\$CPULimitForMigration = If (-not ([string]::IsNullOrEmpty(\$template.LimitCPUForMigration))) {\$template.LimitCPUForMigration} Else { \$false }"
            commands << "\$CPULimitFunctionality = If (-not ([string]::IsNullOrEmpty(\$template.CPULimitFunctionality))) {\$template.CPULimitFunctionality} Else { \$false }"

            commands << "\$ignore = New-SCHardwareProfile -VMMServer localhost -Name \"$hardwareProfileName\" -Description \"Morpheus created profile\" -CPUCount ${maxCores} -MemoryMB ${memoryMB} -DynamicMemoryEnabled \$DynamicMemoryEnabled -MemoryWeight \$MemoryWeight -VirtualVideoAdapterEnabled \$VirtualVideoAdapterEnabled -CPUExpectedUtilizationPercent \$CPUExpectedUtilizationPercent -DiskIops \$DiskIops -CPUMaximumPercent \$CPUMaximumPercent -CPUReserve \$CPUReserve -NumaIsolationRequired \$NumaIsolationRequired -NetworkUtilizationMbps \$NetworkUtilizationMbps -CPURelativeWeight \$CPURelativeWeight -HighlyAvailable ${highlyAvailable ? '\$true' : '\$false'} -DRProtectionRequired \$DRProtectionRequired -CPULimitFunctionality \$CPULimitFunctionality -CPULimitForMigration \$CPULimitForMigration -CheckpointType Production ${scvmmCapabilityProfile ? '-CapabilityProfile \$CapabilityProfile' : ''} -Generation $generationNumber -JobGroup $hardwareGuid"
            commands << "\$HardwareProfile = Get-SCHardwareProfile -VMMServer localhost | where {\$_.Name -eq \"$hardwareProfileName\"}"

            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.HAVMPriority))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -HAVMPriority \$template.HAVMPriority}"

            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.ReplicationGroup))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -ReplicationGroup \$template.ReplicationGroup}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.SecureBootEnabled))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -SecureBootEnabled \$template.SecureBootEnabled}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.NumLock))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -NumLock \$template.NumLock}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.Owner))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -Owner \$template.Owner}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.UserRole))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -UserRole \$template.UserRole}"
            commands << "if( \$DynamicMemoryEnabled -eq \$True) {"
            // We have 3 memories to deal with... startup, min, and max
            // SCVMMM blows up if we are adjusting values and the startup memory is less than the min memory, or the max memory is less than the startup memory, etc.. ugh
            commands << "\$startupMemory = ${memoryMB}; If (\$template.Memory -gt ${memoryMB}) { \$startupMemory = \$template.Memory };"
            if (minDynamicMemoryMB) {
                commands << "\$minimumDynamicMemory = ${minDynamicMemoryMB}; If (\$startupMemory -gt \$minimumDynamicMemory) { \$minimumDynamicMemory = \$startupMemory };"
            } else {
                commands << "\$minimumDynamicMemory = \$template.DynamicMemoryMinimumMB; If (\$startupMemory -gt \$minimumDynamicMemory) { \$minimumDynamicMemory = \$startupMemory };"
            }
            if (maxDynamicMemoryMB) {
                commands << "\$maximumDynamicMemory = ${maxDynamicMemoryMB}; If (\$startupMemory -gt \$maximumDynamicMemory) { \$maximumDynamicMemory = \$startupMemory };"
            } else {
                commands << "\$maximumDynamicMemory = \$template.DynamicMemoryMaximumMB; If (\$startupMemory -gt \$maximumDynamicMemory) { \$maximumDynamicMemory = \$startupMemory };"
            }
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.Memory))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -DynamicMemoryEnabled \$True -MemoryMB \$startupMemory}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.DynamicMemoryMaximumMB))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -DynamicMemoryEnabled \$True -DynamicMemoryMinimumMB \$minimumDynamicMemory}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.DynamicMemoryMaximumMB))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -DynamicMemoryEnabled \$True -DynamicMemoryMaximumMB \$maximumDynamicMemory}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.DynamicMemoryBufferPercentage))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -DynamicMemoryEnabled \$True -DynamicMemoryBufferPercentage \$template.DynamicMemoryBufferPercentage}"
            commands << "}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.MonitorMaximumCount))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -MonitorMaximumCount \$template.MonitorMaximumCount}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.MonitorMaximumResolution))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -MonitorMaximumResolution \$template.MonitorMaximumResolution}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.RecoveryPointObjective))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -RecoveryPointObjective \$template.RecoveryPointObjective}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.ProtectionProvider))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -ProtectionProvider \$template.ProtectionProvider}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.BootOrder))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -BootOrder \$template.BootOrder}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.FirstBootDevice))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -FirstBootDevice \$template.FirstBootDevice}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.SecureBootTemplate))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -SecureBootTemplate \$template.SecureBootTemplate}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.CPUType))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -CPUType \$template.CPUType}"
            commands << "if( \$NumaIsolationRequired -eq \$True) {"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.CPUPerVirtualNumaNodeMaximum))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -CPUPerVirtualNumaNodeMaximum \$template.CPUPerVirtualNumaNodeMaximum}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.MemoryPerVirtualNumaNodeMaximumMB))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -MemoryPerVirtualNumaNodeMaximumMB \$template.MemoryPerVirtualNumaNodeMaximumMB}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.VirtualNumaNodesPerSocketMaximum))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -VirtualNumaNodesPerSocketMaximum \$template.VirtualNumaNodesPerSocketMaximum}"
            commands << "}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.AutomaticCriticalErrorAction))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -AutomaticCriticalErrorAction \$template.AutomaticCriticalErrorAction}"
            commands << "\$ignore = If (-not ([string]::IsNullOrEmpty(\$template.AutomaticCriticalErrorActionTimeout))) { Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -AutomaticCriticalErrorActionTimeout \$template.AutomaticCriticalErrorActionTimeout}"
        } else {
            commands << "\$ignore = New-SCHardwareProfile -VMMServer localhost -Name \"$hardwareProfileName\" -Description \"Morpheus created profile\" -CPUCount ${maxCores} -MemoryMB ${memoryMB} -DynamicMemoryEnabled \$false -MemoryWeight 5000 -VirtualVideoAdapterEnabled \$false -CPUExpectedUtilizationPercent 20 -DiskIops 0 -CPUMaximumPercent 100 -CPUReserve 0 -NumaIsolationRequired \$false -NetworkUtilizationMbps 0 -CPURelativeWeight 100 -HighlyAvailable ${highlyAvailable ? '\$true' : '\$false'} -DRProtectionRequired \$false -CPULimitFunctionality \$false -CPULimitForMigration \$false -CheckpointType Production ${scvmmCapabilityProfile ? '-CapabilityProfile \$CapabilityProfile' : ''} -Generation $generationNumber -JobGroup $hardwareGuid"
            commands << "\$HardwareProfile = Get-SCHardwareProfile -VMMServer localhost | where {\$_.Name -eq \"$hardwareProfileName\"}"
        }

        if (minDynamicMemoryMB && maxDynamicMemoryMB) {
            commands << "\$ignore = Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -DynamicMemoryEnabled \$True -DynamicMemoryMinimumMB ${minDynamicMemoryMB}"
            commands << "\$ignore = Set-SCHardwareProfile -HardwareProfile \$HardwareProfile -DynamicMemoryEnabled \$True -DynamicMemoryMaximumMB ${maxDynamicMemoryMB}"
        }

        if (deployingToCloud) {
            commands << "\$cloud = Get-SCCloud -ID \"${zone.regionCode}\""
        }


        if (!cloneVMId) {
            if (isSysprep) {
                commands << "\$OS = Get-SCOperatingSystem -VMMServer localhost | where {\$_.Name -eq \"$OSName\"}"
            }

            def diskJobGuid = UUID.randomUUID().toString()
            if (isTemplate && templateId) {
                //commands << "New-SCVMTemplate -Name \"$templateName\" -Template \$Template -Generation $generationNumber -HardwareProfile \$HardwareProfile -JobGroup $diskJobGuid ${isSysprep ? '-OperatingSystem $OS' : '-NoCustomization'}"
//				commands << "New-SCVMTemplate -VMTemplate \$Template -Name \"$templateName\" -HardwareProfile \$HardwareProfile -JobGroup $diskJobGuid ${isSysprep ? '-OperatingSystem $OS' : ''}"
//				commands << "if( -not \$? ) { Exit 23 }"
            } else {
                // Disks (OS)
                commands << "\$VirtualHardDisk = Get-SCVirtualHardDisk -VMMServer localhost -ID \"${imageId}\""
                if (volumePath && !deployingToCloud) {
                    commands << "\$ignore = New-SCVirtualDiskDrive -VMMServer localhost ${generationNumber == '1' ? '-IDE' : '-SCSI'} -Bus 0 -LUN 0 -JobGroup $diskJobGuid -CreateDiffDisk \$false -VirtualHardDisk \$VirtualHardDisk -Path \"$volumePath\" -VolumeType BootAndSystem"
                } else {
                    commands << "\$ignore = New-SCVirtualDiskDrive -VMMServer localhost ${generationNumber == '1' ? '-IDE' : '-SCSI'} -Bus 0 -LUN 0 -JobGroup $diskJobGuid -CreateDiffDisk \$false -VirtualHardDisk \$VirtualHardDisk -VolumeType BootAndSystem"
                }

                // Data Disks
                dataDisks?.eachWithIndex { dataDisk, index ->
                    def fromDisk
                    if (isSyncdImage) {
                        fromDisk = "\$VirtualHardDisk${index}"
                        def diskExternalId = diskExternalIdMappings[1 + index]?.externalId
                        if (diskExternalId) {
                            commands << "${fromDisk} = Get-SCVirtualHardDisk -VMMServer localhost -ID \"${diskExternalId}\""
                        }
                    }
                    def busNumber = '0'
                    def generateResults = generateDataDiskCommand(busNumber, index, diskJobGuid, (int) dataDisk.maxStorage.div(ComputeUtility.ONE_MEGABYTE), dataDisk.volumePath, fromDisk, deployingToCloud)
                    commands << generateResults.command
                }

                commands << "\$ignore = New-SCVMTemplate -Name \"$templateName\" -Generation $generationNumber -HardwareProfile \$HardwareProfile -JobGroup $diskJobGuid ${isSysprep ? '-OperatingSystem $OS' : '-NoCustomization'}"
                commands << "if( -not \$? ) { Exit 23 }"
                commands << "\$template = Get-SCVMTemplate -All | where { \$_.Name -eq \"$templateName\" }"
            }

            // Create a template based on the configuration above and create it
//			commands << "\$template = Get-SCVMTemplate -All | where { \$_.Name -eq \"$templateName\" }"
            commands << "\$virtualMachineConfiguration = New-SCVMConfiguration -VMTemplate \$template -Name \"$vmId\""

            if (doStatic && doPool) {
                commands << "\$VNAConfig = Get-SCVirtualNetworkAdapterConfiguration -VMConfiguration \$virtualMachineConfiguration"
                if (doPool) {
                    commands << "\$ippool = Get-SCStaticIPAddressPool -ID \"$poolId\""
                    commands << "\$ipaddress = Get-SCIPAddress -IPAddress \"$ipAddress\""
                } else {
                    commands << "\$ipaddress = \"$ipAddress\""
                }

                commands << "\$ignore = Set-SCVirtualNetworkAdapterConfiguration -VirtualNetworkAdapterConfiguration \$VNAConfig ${doPool ? "-IPv4Address \$ipaddress -IPv4AddressPool \$ippool" : "-IPv4Address \$ipAddress"} -MACAddress \"00:00:00:00:00:00\""
            }

//			commands << "Write-Output \$virtualMachineConfiguration"

            if (isSysprep && unattendPath) {
                // Need to fetch the answerFile
                commands << "\$AnswerFile = Get-SCScript | where {\$_.IsXMLAnswerFile -eq \$True} | where {\$_.SharePath -eq \"$unattendPath\"}"
            }

            if (hostExternalId) {
                commands << "\$vmHost = Get-SCVMHost -ID \"$hostExternalId\""
                commands << "\$ignore = Set-SCVMConfiguration -VMConfiguration \$virtualMachineConfiguration -VMHost \$vmHost"
                commands << "\$ignore = Update-SCVMConfiguration -VMConfiguration \$virtualMachineConfiguration"
                if (volumePath && !deployingToCloud) {
                    commands << "\$VHDConfiguration = Get-SCVirtualHardDiskConfiguration -VMConfiguration \$virtualMachineConfiguration"
                    if (volumePaths) {
                        commands << "\$volumePaths = @( \"${volumePaths.join('","')}\" )"
                        commands << "\$currentIndex = 0"
                        commands << "Foreach (\$conf in \$VHDConfiguration) { \$ignore = Set-SCVirtualHardDiskConfiguration -VHDConfiguration \$conf -PinSourceLocation \$false -DestinationLocation \$volumePaths[\$currentIndex] -PinFileName \$false -StorageQoSPolicy \$null -DeploymentOption \"UseNetwork\"; \$currentIndex = \$currentIndex + 1 } "
                    } else {
                        commands << "Foreach (\$conf in \$VHDConfiguration) { \$ignore = Set-SCVirtualHardDiskConfiguration -VHDConfiguration \$conf -PinSourceLocation \$false -DestinationLocation \"$volumePath\" -PinFileName \$false -StorageQoSPolicy \$null -DeploymentOption \"UseNetwork\" } "
                    }
                }

                def newVMString = "\$createdVm = New-SCVirtualMachine -Name \"$vmId\" -VMConfiguration \$virtualMachineConfiguration ${isSysprep ? "-AnswerFile \$AnswerFile" : ""} ${isTemplate ? "-HardwareProfile \$HardwareProfile" : ""} -JobGroup \"$diskJobGuid\" -StartAction \"TurnOnVMIfRunningWhenVSStopped\" -RunAsynchronously -StopAction \"SaveVM\""
                newVMString = appendOSCustomization(newVMString, opts)
                commands << newVMString
            } else {
                def newVMString = "\$createdVm = New-SCVirtualMachine -Name \"$vmId\" -VMConfiguration \$virtualMachineConfiguration -Cloud \$cloud ${isSysprep ? "-AnswerFile \$AnswerFile" : ""} -HardwareProfile \$HardwareProfile -JobGroup \"$diskJobGuid\" -StartAction \"TurnOnVMIfRunningWhenVSStopped\" -RunAsynchronously -StopAction \"SaveVM\""
                newVMString = appendOSCustomization(newVMString, opts)
                commands << newVMString
            }
        } else {
            def virtualNetworkGuid = UUID.randomUUID().toString()
            commands << "\$VM = Get-SCVirtualMachine -VMMServer localhost -ID \"${cloneVMId}\""
            commands << "if (\$VMNetwork.VMSubnet) { if (\$VMNetwork.VMSubnet -is [Array] -or \$VMNetwork.VMSubnet -is [System.Collections.Generic.List[Microsoft.SystemCenter.VirtualMachineManager.VMSubnet]]) { \$VMSubnet = \$VMNetwork.VMSubnet[0]; } else { \$VMSubnet = \$VMNetwork.VMSubnet } }"
            commands << "\$VirtualNetworkAdapter = Get-SCVirtualNetworkAdapter -VMMServer localhost -VM \$VM"
            commands << "\$VirtualNetwork = Get-SCVirtualNetwork -VMMServer localhost -Name \$VirtualNetworkAdapter.VirtualNetwork | Select-Object -first 1"
            commands << "if (\$VMSubnet) {"
            commands << "Set-SCVirtualNetworkAdapter -VirtualNetworkAdapter \$VirtualNetworkAdapter -VMNetwork \$VMNetwork -VMSubnet \$VMSubnet ${vlanEnabled ? "-VLanEnabled \$true" : ""} ${vlanEnabled ? "-VLanID ${vlanId}" : ''} -VirtualNetwork \$VirtualNetwork -MACAddressType Dynamic -IPv4AddressType Dynamic -IPv6AddressType Dynamic -NoPortClassification -EnableVMNetworkOptimization \$false -EnableMACAddressSpoofing \$false -JobGroup $virtualNetworkGuid"
            commands << "} else {"
            commands << "Set-SCVirtualNetworkAdapter -VirtualNetworkAdapter \$VirtualNetworkAdapter -VMNetwork \$VMNetwork ${vlanEnabled ? "-VLanEnabled \$true" : ""} ${vlanEnabled ? "-VLanID ${vlanId}" : ''} -VirtualNetwork \$VirtualNetwork -MACAddressType Dynamic -IPv4AddressType Dynamic -IPv6AddressType Dynamic -NoPortClassification -EnableVMNetworkOptimization \$false -EnableMACAddressSpoofing \$false -JobGroup $virtualNetworkGuid"
            commands << "}"
            if (hostExternalId) {
                commands << "\$vmHost = Get-SCVMHost -ID \"$hostExternalId\""
                def newVMString
                if (deployingToCloud) {
                    newVMString = "\$createdVm = New-SCVirtualMachine -VM \$VM -Name \"$vmId\" -JobGroup $virtualNetworkGuid -UseDiffDiskOptimization -RunAsynchronously -Cloud \$cloud -HardwareProfile \$HardwareProfile -StartAction TurnOnVMIfRunningWhenVSStopped -StopAction SaveVM"
                } else {
                    newVMString = "\$createdVm = New-SCVirtualMachine -VM \$VM -Name \"$vmId\" -JobGroup $virtualNetworkGuid -UseDiffDiskOptimization -RunAsynchronously -VMHost \$vmHost -Path \"$volumePath\" -HardwareProfile \$HardwareProfile -StartAction TurnOnVMIfRunningWhenVSStopped -StopAction SaveVM"
                }

                newVMString = appendOSCustomization(newVMString, opts)
                commands << newVMString

            } else {
                def newVMString = "\$createdVm = New-SCVirtualMachine -VM \$VM -Name \"$vmId\" ${deployingToCloud ? "-Cloud \$cloud" : ""} -JobGroup $virtualNetworkGuid -UseDiffDiskOptimization -RunAsynchronously -Cloud \$cloud -HardwareProfile \$HardwareProfile -StartAction TurnOnVMIfRunningWhenVSStopped -StopAction SaveVM"
                newVMString = appendOSCustomization(newVMString, opts)
                commands << newVMString
            }
        }
        commands << "\$createdVm | Select ID, ObjectType"

        rtn.launchCommand = commands.join('\n')
        rtn.hardwareProfileName = hardwareProfileName
        rtn.templateName = templateName
        rtn
    }

    def findBootDiskIndex(diskDrives) {
        def bookDiskIndex = 0
        diskDrives.disks?.eachWithIndex { disk, diskIndex ->
            if (disk.VolumeType == 'BootAndSystem') {
                bookDiskIndex = diskIndex
            }
        }
        bookDiskIndex
    }

    def appendOSCustomization(sourceString, opts) {
        def retString = sourceString
        if (opts.isSysprep && !opts.cloneVMId) {
            if (opts.hostname) {
                retString += " -ComputerName \"${opts.hostname}\""
            }
            if (opts.license?.fullName) {
                retString += " -FullName \"${opts.license.fullName}\""
            }
            if (opts.license?.productKey) {
                retString += " -ProductKey \"${opts.license.productKey}\""
            }
            if (opts.license?.orgName) {
                retString += " -OrganizationName \"${opts.license.orgName}\""
            }
        }

        retString
    }

    def generateDataDiskCommand(busNumber = '0', dataDiskNumber, diskJobGuid, sizeMB, path = null, fromDisk = null, discoverAvailableLUN = false, deployingToCloud = false) {
        def rtn = [command: null, fileName: '']

        // def diskParamMap = [
        //         [type: 'IDE', bus: '0', lun: '1'],
        //         [type: 'IDE', bus: '1', lun: '1']
        // ]

        def diskParams
        // if(dataDiskNumber > 1) {

        // } else {
        // 	diskParams = diskParamMap[dataDiskNumber]
        // }
        diskParams = [type: 'SCSI', bus: busNumber, lun: (dataDiskNumber).toString()]

        def fileName = "data${dataDiskNumber}-${UUID.randomUUID().toString()}.vhd"
        rtn.fileName = fileName

        if (fromDisk && !deployingToCloud) {
            if (path) {
                rtn.command = "\$ignore = New-SCVirtualDiskDrive -VMMServer localhost -${diskParams.type} -Bus ${diskParams.bus} -LUN ${diskParams.lun} -JobGroup ${diskJobGuid} -CreateDiffDisk \$false -VirtualHardDisk $fromDisk -FileName \"$fileName\" -Path \"$path\" -VolumeType None"
                // Can't set size when creating from another existing disk
            } else {
                rtn.command = "\$ignore = New-SCVirtualDiskDrive -VMMServer localhost -${diskParams.type} -Bus ${diskParams.bus} -LUN ${diskParams.lun} -JobGroup ${diskJobGuid} -CreateDiffDisk \$false -VirtualHardDisk $fromDisk -FileName \"$fileName\" -VolumeType None"
                // Can't set size when creating from another existing disk
            }
        } else {
            if (path && !deployingToCloud) {
                rtn.command = "\$ignore = New-SCVirtualDiskDrive -VMMServer localhost -${diskParams.type} -Bus ${diskParams.bus} -LUN ${diskParams.lun} -JobGroup ${diskJobGuid} -VirtualHardDiskSizeMB ${sizeMB} -CreateDiffDisk \$false -Dynamic -FileName \"$fileName\" -Path \"$path\" -VolumeType None"
            } else {
                rtn.command = "\$ignore = New-SCVirtualDiskDrive -VMMServer localhost -${diskParams.type} -Bus ${diskParams.bus} -LUN ${diskParams.lun} -JobGroup ${diskJobGuid} -VirtualHardDiskSizeMB ${sizeMB} -CreateDiffDisk \$false -Dynamic -FileName \"$fileName\" -VolumeType None"
            }

        }

        rtn
    }

    def getScvmmZoneOpts(MorpheusContext context, Cloud cloud) {
        def cloudConfig = cloud.getConfigMap()
        def keyPair = context.services.keyPair.find(new DataQuery().withFilter("accountId", cloud?.account?.id))
        return [account                : cloud.account,
                zoneConfig             : cloudConfig,
                zone                   : cloud,
                zoneId                 : cloud?.id,
                publicKey              : keyPair?.publicKey,
                privateKey             : keyPair?.privateKey,
                //controllerServer       : controllerServer,
                rootSharePath          : cloudConfig['libraryShare'],
                regionCode             : cloud.regionCode]
                //baseBoxProvisionService: scvmmProvisionService]
    }

    def getScvmmControllerOpts(zone, hypervisor) {
        def serverConfig = hypervisor.getConfigMap()
        def zoneConfig = zone.getConfigMap()
        log.debug("scvmm hypervisor config:${serverConfig}")
        def configuredDiskPath = zoneConfig.diskPath?.length() > 0 ? zoneConfig.diskPath : serverConfig.diskPath?.length() > 0 ? serverConfig.diskPath : null
        def diskRoot = configuredDiskPath ? configuredDiskPath : defaultRoot + '\\Disks'
        def configuredWorkingPath = zoneConfig.workingPath?.length() > 0 ? zoneConfig.workingPath : serverConfig.workingPath?.length() > 0 ? serverConfig.workingPath : null
        def zoneRoot = configuredWorkingPath ? configuredWorkingPath : defaultRoot
        return [hypervisorConfig: serverConfig, hypervisor: hypervisor, sshHost: hypervisor.sshHost, sshUsername: hypervisor.sshUsername,
                sshPassword: hypervisor.sshPassword, zoneRoot: zoneRoot, diskRoot: diskRoot]
    }

    def getScvmmZoneAndHypervisorOpts(zone, hypervisor, scvmmProvisionService) {
        getScvmmZoneOpts(zone, hypervisor, scvmmProvisionService) + getScvmmControllerOpts(zone, hypervisor)
    }

    def wrapExecuteCommand(String command, Map opts = [:]) {
        def out = executeCommand(command, opts)
        if (out.data) {
            def payload = out.data
            if (!out.data.startsWith('[')) {
                payload = "[${out.data}]"
            }
            try {
                log.debug "Received: ${JsonOutput.prettyPrint(payload)}"
            } catch (e) {
//				File file = new File("/Users/bob/Desktop/bad.json")
//				file.write payload
            }
            out.data = new groovy.json.JsonSlurper().parseText(payload)
        }
        out
    }

    def loadControllerServer(opts) {
        if (opts.controllerServerId && opts.scvmmProvisionService) {
            opts.controllerServer = opts.scvmmProvisionService.loadControllerServer(opts.controllerServerId)
        }
    }

    def isHostInHostGroup(String currentHostPath, String testHostPath) {
        return (currentHostPath == testHostPath || (testHostPath && currentHostPath?.startsWith(testHostPath + "\\")))
    }

    def extractFileName(imageName) {
        def rtn = imageName
        def lastIndex = imageName?.lastIndexOf('/')
        if (lastIndex > -1)
            rtn = imageName.substring(lastIndex + 1)
        return rtn
    }

    def extractImageFileName(imageName) {
        def rtn = extractFileName(imageName)
        if (rtn.indexOf('.tar.gz') > -1)
            rtn = rtn.replaceAll('.tar.gz', '')
        if (rtn.indexOf('.gz') > -1)
            rtn = rtn.replaceAll('.gz', '')
        return rtn
    }

    def formatImageFolder(imageName) {
        def rtn = imageName
        rtn = rtn.replaceAll(' ', '_')
        rtn = rtn.replaceAll('\\.', '_')
    }

    def getScvmmInitializationOpts(cloud) {
        def cloudConfig = cloud.getConfigMap()
        def diskRoot = cloudConfig.diskPath?.length() > 0 ? cloudConfig.diskPath : defaultRoot + '\\Disks'
        def cloudRoot = cloudConfig.workingPath?.length() > 0 ? cloudConfig.workingPath : defaultRoot
        return [sshHost:cloudConfig.host, sshUsername:getUsername(cloud), sshPassword:getPassword(cloud), zoneRoot:cloudRoot,
                diskRoot:diskRoot]
    }

    private getUsername(cloud) {
        cloud.accountCredentialData?.username ?: cloud.getConfigProperty('username') ?: 'dunno'
    }

    private getPassword(cloud) {
        cloud.accountCredentialData.credentialData?.password ?: cloud.getConfigProperty('password')
    }
}
