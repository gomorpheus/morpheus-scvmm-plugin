package com.morpheus.scvmm.sync

import com.morpheus.scvmm.ScvmmApiService
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.data.DataFilter
import com.morpheusdata.core.data.DataOrFilter
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.CloudProvider
import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.*
import com.morpheusdata.model.projection.StorageVolumeIdentityProjection
import com.morpheusdata.model.projection.VirtualImageIdentityProjection
import com.morpheusdata.model.projection.VirtualImageLocationIdentityProjection
import groovy.util.logging.Slf4j
import io.reactivex.rxjava3.core.Observable

/**
 * @author rahul.ray
 */

@Slf4j
class TemplatesSync {
    private Cloud cloud
    private ComputeServer node
    private MorpheusContext context
    private ScvmmApiService apiService
    private CloudProvider cloudProvider

    TemplatesSync(Cloud cloud, ComputeServer node, MorpheusContext context, CloudProvider cloudProvider) {
        this.cloud = cloud
        this.node = node
        this.context = context
        this.apiService = new ScvmmApiService(context)
        this.@cloudProvider = cloudProvider

    }

    def execute() {
        log.debug "TemplatesSync"
        def scvmmOpts = apiService.getScvmmZoneAndHypervisorOpts(context, cloud, node)
        def listResults = apiService.listTemplates(scvmmOpts)
        if (listResults.success && listResults.templates) {
            def existingLocations = context.services.virtualImage.location.listIdentityProjections(new DataQuery()
                    .withFilter('refType', 'ComputeZone')
                    .withFilter('refId', cloud.id)
                    .withFilter('virtualImage.imageType', 'in', ['vhd', 'vhdx'])
                    .withJoin('virtualImage'))
            def groupedLocations = existingLocations.groupBy({ row -> row.externalId })
            def dupedLocations = groupedLocations.findAll { key, value -> value.size() > 1 }
            def dupeCleanup = []
            if (dupedLocations?.size() > 0)
                log.warn("removing duplicate image locations: {}", dupedLocations.collect { it.key })
            dupedLocations?.each { key, value ->
                value.eachWithIndex { row, index ->
                    if (index > 0)
                        dupeCleanup << row
                }
            }
            dupeCleanup?.each { row ->
                def dupeResults = context.async.virtualImage.location.remove([row.id]).blockingGet()
                if (dupeResults == true) {
                    existingLocations.remove(row)
                }
            }
            def domainRecords = Observable.fromIterable(existingLocations)
            SyncTask<VirtualImageLocationIdentityProjection, Map, VirtualImageLocation> syncTask = new SyncTask<>(domainRecords, listResults.templates as Collection<Map>)
            syncTask.addMatchFunction { VirtualImageLocationIdentityProjection domainObject, Map cloudItem ->
                domainObject.externalId == cloudItem.ID.toString()
            }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<VirtualImageLocationIdentityProjection, Map>> updateItems ->
                context.async.virtualImage.location.listById(updateItems.collect { it.existingItem.id } as List<Long>)
            }.onAdd { itemsToAdd ->
                log.debug("TemplatesSync, onAdd: ${itemsToAdd}")
                addMissingVirtualImageLocations(itemsToAdd)
            }.onUpdate { List<SyncTask.UpdateItem<VirtualImageLocation, Map>> updateItems ->
                log.debug("TemplatesSync, onUpdate: ${updateItems}")
                updateMatchedVirtualImageLocations(updateItems)
            }.onDelete { List<VirtualImageLocationIdentityProjection> removeItems ->
                log.debug("TemplatesSync, onDelete: ${removeItems}")
                removeMissingVirtualImages(removeItems)
            }.start()
        }
    }

    protected removeMissingVirtualImages(List<VirtualImageLocationIdentityProjection> removeList) {
        try {
            log.debug("removeMissingVirtualImageLocations: ${cloud} ${removeList.size()}")
            context.async.virtualImage.location.remove(removeList).blockingGet()
        } catch (e) {
            log.error("error deleting synced virtual image: ${e}", e)
        }
    }

    private updateMatchedVirtualImageLocations(List<SyncTask.UpdateItem<VirtualImageLocation, Map>> updateItems) {
        log.debug "updateMatchedVirtualImages: ${updateItems.size()}"
        try {

            def locationIds = updateItems.findAll { it.existingItem.id }?.collect { it.existingItem.id }
            def existingLocations = locationIds ? context.services.virtualImage.location.list(new DataQuery()
                    .withFilter('id', 'in', locationIds)
                    .withFilter('refType', 'ComputeZone')
                    .withFilter('refId', cloud.id)) : []
            def imageIds = updateItems?.findAll { it.existingItem.virtualImage?.id }?.collect { it.existingItem.virtualImage?.id }
            def externalIds = updateItems?.findAll { it.existingItem.externalId }?.collect { it.existingItem.externalId }
            List<VirtualImage> existingItems = []
            if (imageIds && externalIds) {
                existingItems = context.services.virtualImage.list(new DataQuery().withFilters(
                        new DataFilter('id', 'in', imageIds),
                        new DataOrFilter(
                                new DataFilter('refType', 'ComputeZone'),
                                new DataFilter('refId', cloud.id?.toString()),
                                new DataFilter('externalId', 'in', externalIds),
                                new DataFilter('locations', null)
                        )
                ).withJoin('locations'))
            } else if (imageIds) {
                existingItems = context.services.virtualImage.list(new DataQuery().withFilter('id', 'in', imageIds))
            }
            //dedupe
            def groupedImages = existingItems.groupBy({ row -> row.externalId })
            def dupedImages = groupedImages.findAll { key, value -> key != null && value.size() > 1 }
            if (dupedImages?.size() > 0)
                log.warn("removing duplicate images: {}", dupedImages.collect { it.key })
            dupedImages?.each { key, value ->
                //each pass is set of all the images with the same external id
                def dupeCleanup = []
                value.eachWithIndex { row, index ->
                    def locationMatch = existingLocations.find { it.virtualImage.id == row.id }
                    if (locationMatch == null) {
                        dupeCleanup << row
                        existingItems.remove(row)
                    }
                }
                //cleanup
                log.info("duplicate key: ${key} total: ${value.size()} remove count: ${dupeCleanup.size()}")
            }
            //updates
            List<VirtualImageLocation> locationsToCreate = []
            List<VirtualImageLocation> locationsToUpdate = []
            List<VirtualImage> imagesToUpdate = []
            updateItems?.each { update ->
                def matchedTemplate = update.masterItem
                def imageLocation = existingLocations?.find { it.id == update.existingItem.id }
                if (imageLocation) {
                    def save = false
                    def saveImage = false
                    def virtualImage = existingItems.find { it.id == imageLocation.virtualImage.id }
                    if (virtualImage) {
                        if (imageLocation.imageName != matchedTemplate.Name) {
                            imageLocation.imageName = matchedTemplate.Name
                            if (virtualImage.refId == imageLocation.refId.toString()) {
                                virtualImage.name = matchedTemplate.Name
                                saveImage = true
                            }
                            save = true
                        }
                    }

                    if (imageLocation.code == null) {
                        imageLocation.code = "scvmm.image.${cloud.id}.${matchedTemplate.ID}"
                        save = true
                    }
                    if (imageLocation.externalId != matchedTemplate.ID) {
                        imageLocation.externalId = matchedTemplate.ID
                        save = true
                    }

                    if (matchedTemplate.Disks) {
                        def changed = syncVolumes(imageLocation, matchedTemplate.Disks)
                        if (changed == true)
                            save = true
                    }

                    if (virtualImage?.isPublic != false) {
                        virtualImage.isPublic = false
                        imageLocation.isPublic = false
                        save = true
                        saveImage = true
                    }

                    if (save) {
                        locationsToUpdate << imageLocation
                    }
                    if (saveImage) {
                        imagesToUpdate << virtualImage
                    }
                } else {
                    def image = existingItems?.find { it.externalId == matchedTemplate.ID || it.name == matchedTemplate.Name }
                    if (image) {
                        //if we matched by virtual image and not a location record we need to create that location record
                        def locationConfig = [
                                code        : "scvmm.image.${cloud.id}.${matchedTemplate.ID}",
                                externalId  : matchedTemplate.ID,
                                virtualImage: image,
                                refType     : 'ComputeZone',
                                refId       : cloud.id,
                                imageName   : matchedTemplate.Name,
                                imageRegion : cloud.regionCode,
                                isPublic    : false
                        ]
                        def addLocation = new VirtualImageLocation(locationConfig)
                        log.debug("save VirtualImageLocation: ${addLocation.errors}")
                        locationsToCreate << addLocation
                        //tmp fix
                        if (!image.owner && !image.systemImage)
                            image.owner = cloud.owner
                        image.deleted = false
                        //image.addToLocations(addLocation)
                        image.isPublic = false
                        imagesToUpdate << image
                    }
                }
            }
            if (locationsToCreate.size() > 0) {
                context.async.virtualImage.location.create(locationsToCreate, cloud).blockingGet()
            }
            if (locationsToUpdate.size() > 0) {
                context.async.virtualImage.location.save(locationsToUpdate, cloud).blockingGet()
            }
            if (imagesToUpdate.size() > 0) {
                context.async.virtualImage.save(imagesToUpdate, cloud).blockingGet()
            }
        } catch (e) {
            log.error("Error in updateMatchedVirtualImageLocations: ${e}", e)
        }
    }

    def addMissingVirtualImageLocations(Collection<Map> addList) {
        log.debug "addMissingVirtualImageLocations: ${addList?.size()}"
        try {
            def names = addList.collect { it.Name }?.unique()
            def uniqueIds = [] as Set
            def existingItems = context.async.virtualImage.listIdentityProjections(new DataQuery().withFilters(
                    new DataFilter('imageType', 'in', ['vhd', 'vhdx', 'vmdk']),
                    new DataFilter('name', 'in', names),
                    new DataOrFilter(
                            new DataFilter('systemImage', true),
                            new DataOrFilter(
                                    new DataFilter('owner', null),
                                    new DataFilter('owner.id', cloud.owner?.id)
                            )
                    )
            )).filter { proj ->
                def uniqueKey = "${proj.imageType.toString()}:${proj.name}".toString()
                if (!uniqueIds.contains(uniqueKey)) {
                    uniqueIds << uniqueKey
                    return true
                }
                return false
            }
            SyncTask<VirtualImageIdentityProjection, Map, VirtualImage> syncTask = new SyncTask<>(existingItems, addList)
            syncTask.addMatchFunction { VirtualImageIdentityProjection domainObject, Map cloudItem ->
                domainObject.name == cloudItem.Name
            }.onAdd { itemsToAdd ->
                addMissingVirtualImages(itemsToAdd)
            }.onUpdate { List<SyncTask.UpdateItem<VirtualImage, Map>> updateItems ->
                updateMatchedVirtualImages(updateItems)
            }.withLoadObjectDetailsFromFinder { updateItems ->
                return context.async.virtualImage.listById(updateItems?.collect { it.existingItem.id } as List<Long>)
            }.start()
        } catch (e) {
            log.error("Error in addMissingVirtualImageLocations: ${e}", e)
        }
    }

    private updateMatchedVirtualImages(List<SyncTask.UpdateItem<VirtualImage, Map>> updateItems) {
        log.debug "updateMatchedVirtualImages: ${updateItems.size()}"
        try {
            def locationIds = []
            updateItems?.each {
                def ids = it.existingItem.locations
                locationIds << ids
            }
            def existingLocations = locationIds ? context.services.virtualImage.location.list(new DataQuery()
                    .withFilter('id', 'in', locationIds)
                    .withFilter('refType', 'ComputeZone')
                    .withFilter('refId', cloud.id)) : []
            def imageIds = updateItems?.findAll { it.existingItem.id }?.collect { it.existingItem.id }
            def externalIds = updateItems?.findAll { it.existingItem.externalId }?.collect { it.existingItem.externalId }
            List<VirtualImage> existingItems = []
            if (imageIds && externalIds) {
                existingItems = context.services.virtualImage.list(new DataQuery().withFilters(
                        new DataFilter('id', 'in', imageIds),
                        new DataOrFilter(
                                new DataFilter('refType', 'ComputeZone'),
                                new DataFilter('refId', cloud.id?.toString()),
                                new DataFilter('externalId', 'in', externalIds),
                                new DataFilter('locations', null)
                        )
                ).withJoin('locations'))
            } else if (imageIds) {
                existingItems = context.services.virtualImage.list(new DataQuery().withFilter('id', 'in', imageIds))
            }
            //dedupe
            def groupedImages = existingItems.groupBy({ row -> row.externalId })
            def dupedImages = groupedImages.findAll { key, value -> key != null && value.size() > 1 }
            if (dupedImages?.size() > 0)
                log.warn("removing duplicate images: {}", dupedImages.collect { it.key })
            dupedImages?.each { key, value ->
                //each pass is set of all the images with the same external id
                def dupeCleanup = []
                value.eachWithIndex { row, index ->
                    def locationMatch = existingLocations.find { it.virtualImage.id == row.id }
                    if (locationMatch == null) {
                        dupeCleanup << row
                        existingItems.remove(row)
                    }
                }
                log.info("duplicate key: ${key} total: ${value.size()} remove count: ${dupeCleanup.size()}")
            }
            //updates
            List<VirtualImageLocation> locationsToCreate = []
            List<VirtualImageLocation> locationsToUpdate = []
            List<VirtualImage> imagesToUpdate = []
            updateItems?.each { update ->
                def matchedTemplate = update.masterItem
                def imageLocation = existingLocations?.find { it.id == update.existingItem.id }
                if (imageLocation) {
                    def save = false
                    def saveImage = false
                    def virtualImage = existingItems.find { it.id == imageLocation.virtualImage.id }
                    if (virtualImage) {
                        if (imageLocation.imageName != matchedTemplate.Name) {
                            imageLocation.imageName = matchedTemplate.Name
                            if (virtualImage.refId == imageLocation.refId.toString()) {
                                virtualImage.name = matchedTemplate.Name
                                saveImage = true
                            }
                            save = true
                        }
                    }

                    if (imageLocation.code == null) {
                        imageLocation.code = "scvmm.image.${cloud.id}.${matchedTemplate.ID}"
                        save = true
                    }
                    if (imageLocation.externalId != matchedTemplate.ID) {
                        imageLocation.externalId = matchedTemplate.ID
                        save = true
                    }

                    if (matchedTemplate.Disks) {
                        def changed = syncVolumes(imageLocation, matchedTemplate.Disks)
                        if (changed == true)
                            save = true
                    }

                    if (virtualImage?.isPublic != false) {
                        virtualImage.isPublic = false
                        imageLocation.isPublic = false
                        save = true
                        saveImage = true
                    }
                    if (save) {
                        locationsToUpdate << imageLocation
                    }
                    if (saveImage) {
                        imagesToUpdate << virtualImage
                    }
                } else {
                    def image = existingItems?.find { it.externalId == matchedTemplate.ID || it.name == matchedTemplate.Name }
                    if (image) {
                        //if we matched by virtual image and not a location record we need to create that location record
                        def locationConfig = [
                                code        : "scvmm.image.${cloud.id}.${matchedTemplate.ID}",
                                externalId  : matchedTemplate.ID,
                                virtualImage: image,
                                refType     : 'ComputeZone',
                                refId       : cloud.id,
                                imageName   : matchedTemplate.Name,
                                imageRegion : cloud.regionCode,
                                isPublic    : false
                        ]
                        def addLocation = new VirtualImageLocation(locationConfig)
                        log.debug("save VirtualImageLocation: ${addLocation.errors}")
                        locationsToCreate << addLocation
                        //tmp fix
                        if (!image.owner && !image.systemImage)
                            image.owner = cloud.owner
                        image.deleted = false
                        //image.addToLocations(addLocation)
                        image.isPublic = false
                        imagesToUpdate << image
                    }
                }
            }
            if (locationsToCreate.size() > 0) {
                context.async.virtualImage.location.create(locationsToCreate, cloud).blockingGet()
            }
            if (locationsToUpdate.size() > 0) {
                context.async.virtualImage.location.save(locationsToUpdate, cloud).blockingGet()
            }
            if (imagesToUpdate.size() > 0) {
                context.async.virtualImage.save(imagesToUpdate, cloud).blockingGet()
            }
        } catch (e) {
            log.error("Error in updateMatchedVirtualImages: ${e}", e)
        }
    }

    private addMissingVirtualImages(Collection<Map> addList) {
        log.debug "addMissingVirtualImages ${addList?.size()}"
        try {
            addList?.each { it ->
                def imageConfig = [
                        name       : it.Name,
                        code       : "scvmm.image.${cloud.id}.${it.ID}",
                        refId      : "${cloud.id}",
                        owner      : cloud.owner,
                        status     : 'Active',
                        account    : cloud.account,
                        refType    : 'ComputeZone',
                        isPublic   : false,
                        category   : "scvmm.image.${cloud.id}",
                        imageType  : it.VHDFormatType?.toLowerCase() ?: 'vhdx',
                        visibility : 'private',
                        externalId : it.ID,
                        imageRegion: cloud.regionCode
                ]
                if (it.Memory)
                    imageConfig.minRam = it.Memory.toLong() * ComputeUtility.ONE_MEGABYTE
                if (it.Location)
                    imageConfig.remotePath = it.Location
                def osTypeCode = apiService.getMapScvmmOsType(it.OperatingSystem)
                log.debug "cacheTemplates osTypeCode: ${osTypeCode}"
                def osType = context.services.osType.find(new DataQuery().withFilter('code', osTypeCode ?: 'other'))
                log.debug "osType: ${osType}"
                imageConfig.osType = osType
                imageConfig.platform = osType?.platform
                if (imageConfig.platform == 'windows') {
                    imageConfig.isCloudInit = false
                }
                def add = new VirtualImage(imageConfig)
                if (it.Generation) {
                    add.setConfigProperty('generation', it.Generation?.toString() == '1' ? 'generation1' : 'generation2')
                } else if (it.VHDFormatType) {
                    add.setConfigProperty('generation', it.VHDFormatType.toLowerCase() == 'vhd' ? 'generation1' : 'generation2')
                }

                def locationConfig = [
                        code       : "scvmm.image.${cloud.id}.${it.ID}",
                        refId      : cloud.id,
                        refType    : 'ComputeZone',
                        isPublic   : false,
                        imageName  : it.Name,
                        externalId : it.ID,
                        imageRegion: cloud.regionCode
                ]
                def addLocation = new VirtualImageLocation(locationConfig)
                add.imageLocations = [addLocation]
                def isSaved = context.async.virtualImage.create([add], cloud).blockingGet()
                def saved = syncVolumes(addLocation, it.Disks)
                if (saved) {
                    context.async.virtualImage.save([add], cloud).blockingGet()
                }
            }
        } catch (e) {
            log.error("Error in addMissingVirtualImages: ${e}", e)
        }
    }

    def syncVolumes(addLocation, externalVolumes) {
        log.debug "syncVolumes: ${addLocation}, ${groovy.json.JsonOutput.prettyPrint(externalVolumes?.encodeAsJSON()?.toString())}"
        def changes = false //returns if there are changes to be saved
        try {
            def maxStorage = 0

            def existingVolumes = addLocation.volumes
            def masterItems = externalVolumes

            def existingItems = Observable.fromIterable(existingVolumes)
            def diskNumber = masterItems.size()

            SyncTask<StorageVolumeIdentityProjection, Map, StorageVolume> syncTask = new SyncTask<>(existingItems, masterItems as Collection<Map>)

            syncTask.addMatchFunction { StorageVolumeIdentityProjection storageVolume, Map masterItem ->
                storageVolume.externalId == masterItem.ID
            }.withLoadObjectDetailsFromFinder { List<SyncTask.UpdateItemDto<StorageVolumeIdentityProjection, StorageVolume>> updateItems ->
                context.async.storageVolume.listById(updateItems.collect { it.existingItem.id } as List<Long>)
            }.onAdd { itemsToAdd ->
                addMissingStorageVolumes(itemsToAdd, addLocation, diskNumber, maxStorage, changes)
            }.onUpdate { List<SyncTask.UpdateItem<StorageVolume, Map>> updateItems ->
                updateMatchedStorageVolumes(updateItems, addLocation, maxStorage, changes)
            }.onDelete { removeItems ->
                removeMissingStorageVolumes(removeItems, addLocation, changes)
            }.start()
        } catch (e) {
            log.error("syncVolumes error: ${e}", e)
        }
        return changes
    }


    def addMissingStorageVolumes(itemsToAdd, VirtualImageLocation addLocation, int diskNumber, maxStorage, changes) {
        def provisionProvider = cloudProvider.getProvisionProvider('morpheus-scvmm-plugin.provision')
        itemsToAdd?.each { diskData ->
            log.debug("adding new volume: ${diskData}")
            def datastore = diskData.datastore ?: loadDatastoreForVolume(diskData.HostVolumeId, diskData.FileShareId, diskData.PartitionUniqueId) ?: null
            def volumeConfig = [
                    name      : diskData.Name,
                    size      : diskData.TotalSize?.toLong() ?: 0,
                    rootVolume: diskData.VolumeType == 'BootAndSystem' || !addLocation.volumes?.size(),
                    deviceName: diskData.deviceName, //(diskData.deviceName ?: provisionProvider.getDiskName(diskNumber)), // check: need to check with dustin how to get the diskName (morpheus-logs-2024-12-09-11-00-15)
                    externalId: diskData.ID,
                    internalId: diskData.Name
            ]
            if (datastore)
                volumeConfig.datastoreId = "${datastore.id}"
            def storageVolume = buildStorageVolume(cloud.account, addLocation, volumeConfig)
            def createdStorageVolume = context.async.storageVolume.create([storageVolume], addLocation).blockingGet()
            maxStorage += storageVolume.maxStorage ?: 0l
            diskNumber++
            log.debug("added volume: ${storageVolume?.dump()}")
        }
    }

    def updateMatchedStorageVolumes(updateItems, addLocation, maxStorage, changes) {
        updateItems?.each { updateMap ->
            log.debug("updating volume: ${updateMap.masterItem}")
            StorageVolume volume = updateMap.existingItem
            def masterItem = updateMap.masterItem

            def masterDiskSize = masterItem?.TotalSize?.toLong() ?: 0
            def sizeRange = [min: (volume.maxStorage - ComputeUtility.ONE_GIGABYTE), max: (volume.maxStorage + ComputeUtility.ONE_GIGABYTE)]
            def save = false
            if (masterDiskSize && volume.maxStorage != masterDiskSize && (masterDiskSize <= sizeRange.min || masterDiskSize >= sizeRange.max)) {
                volume.maxStorage = masterDiskSize
                save = true
            }
            if (volume.internalId != masterItem.Name) {
                volume.internalId = masterItem.Name
                save = true
            }
            def isRootVolume = (masterItem?.VolumeType == 'BootAndSystem') || (addLocation.volumes.size() == 1)
            if (volume.rootVolume != isRootVolume) {
                volume.rootVolume = isRootVolume
                save = true
            }
            if (save) {
                context.async.storageVolume.save(volume).blockingGet()
                changes = true
            }
            maxStorage += masterDiskSize
        }
    }

    def removeMissingStorageVolumes(removeItems, addLocation, changes) {
        removeItems?.each { currentVolume ->
            log.debug "removing volume: ${currentVolume}"
            changes = true
            currentVolume.controller = null
            currentVolume.datastore = null

            context.async.storageVolume.save(currentVolume).blockingGet()
            context.async.storageVolume.remove([currentVolume], addLocation).blockingGet()
            context.async.storageVolume.remove(currentVolume).blockingGet()
        }
    }

    def buildStorageVolume(account, addLocation, volume) {
        def storageVolume = new StorageVolume()
        storageVolume.name = volume.name
        storageVolume.account = account

        def storageType = context.services.storageVolume.storageVolumeType.find(new DataQuery()
                .withFilter('code', 'standard'))
        storageVolume.type = storageType

        storageVolume.rootVolume = volume.rootVolume == true
        if (volume.datastoreId) {
            storageVolume.datastoreOption = volume.datastoreId
            storageVolume.refType = 'Datastore'
            storageVolume.refId = volume.datastoreId
        }

        if (volume.externalId)
            storageVolume.externalId = volume.externalId
        if (volume.internalId)
            storageVolume.internalId = volume.internalId

        if (addLocation instanceof VirtualImageLocation && addLocation.refType == 'ComputeZone') {
            storageVolume.cloudId = addLocation.refId?.toLong()
        }

        storageVolume.deviceName = volume.deviceName

        storageVolume.removable = storageVolume.rootVolume != true
        storageVolume.displayOrder = volume.displayOrder ?: addLocation?.volumes?.size() ?: 0
        return storageVolume
    }

    def loadDatastoreForVolume(hostVolumeId = null, fileShareId = null, partitionUniqueId = null) {
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
}