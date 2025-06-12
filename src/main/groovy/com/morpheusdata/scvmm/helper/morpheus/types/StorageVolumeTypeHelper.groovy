package com.morpheusdata.scvmm.helper.morpheus.types

import com.morpheusdata.core.util.ComputeUtility
import com.morpheusdata.model.StorageVolumeType
import groovy.transform.CompileStatic

@CompileStatic
class StorageVolumeTypeHelper {
    // Constants
    protected static final String VOLUME_TYPE_DISK = 'disk'
    protected static final String VOLUME_TYPE_DATASTORE = 'datastore'
    protected static final String VOLUME_CATEGORY_DISK = 'disk'
    protected static final String VOLUME_CATEGORY_DATASTORE = 'datastore'

    // Display names
    protected static final String SCVMM_DATASTORE_DISPLAY = 'SCVMM Datastore'
    protected static final String SCVMM_FILESHARE_DISPLAY = 'SCVMM Registered File Share'
    protected static final String FIXED_VHD_DISPLAY = 'Fixed Size VHD'
    protected static final String DYNAMIC_VHD_DISPLAY = 'Dynamically Expanding VHD'
    protected static final String DIFFERENCING_VHD_DISPLAY = 'Differencing VHD'
    protected static final String FIXED_VHDX_DISPLAY = 'Fixed Size VHDX'
    protected static final String DYNAMIC_VHDX_DISPLAY = 'Dynamically Expanding VHDX'
    protected static final String DIFFERENCING_VHDX_DISPLAY = 'Differencing VHDX'
    protected static final String LINKED_PHYSICAL_DISPLAY = 'Linked Physical'

    // Volume names
    protected static final String SCVMM_DATASTORE_NAME = 'SCVMM Datastore'
    protected static final String SCVMM_FILESHARE_NAME = 'SCVMM Registered File Share'
    protected static final String FIXED_VHD_NAME = 'Fixed Size VHD'
    protected static final String DYNAMIC_VHD_NAME = 'Dynamically Expanding VHD'
    protected static final String DIFFERENCING_VHD_NAME = 'Differencing VHD'
    protected static final String FIXED_VHDX_NAME = 'Fixed Size VHDX'
    protected static final String DYNAMIC_VHDX_NAME = 'Dynamically Expanding VHDX'
    protected static final String DIFFERENCING_VHDX_NAME = 'Differencing VHDX'
    protected static final String LINKED_PHYSICAL_NAME = 'Linked Physical'

    // Codes
    public static final String SCVMM_DATASTORE_CODE = 'scvmm-datastore'
    public static final String SCVMM_FILESHARE_CODE = 'scvmm-registered-file-share-datastore'
    public static final String FIXED_VHD_CODE = 'scvmm-fixedsize-vhd'
    public static final String DYNAMIC_VHD_CODE = 'scvmm-dynamicallyexpanding-vhd'
    public static final String DIFFERENCING_VHD_CODE = 'scvmm-differencing-vhd'
    public static final String FIXED_VHDX_CODE = 'scvmm-fixedsize-vhdx'
    public static final String DYNAMIC_VHDX_CODE = 'scvmm-dynamicallyexpanding-vhdx'
    public static final String DIFFERENCING_VHDX_CODE = 'scvmm-differencing-vhdx'
    public static final String LINKED_PHYSICAL_CODE = 'scvmm-linkedphysical'

    // External IDs
    protected static final String FIXED_VHD_EXTERNAL_ID = 'fixed-vhd'
    protected static final String DYNAMIC_VHD_EXTERNAL_ID = 'dynamic-vhd'
    protected static final String DIFFERENCING_EXTERNAL_ID = 'differencing'
    protected static final String FIXED_VHDX_EXTERNAL_ID = 'fixed-vhdx'
    protected static final String DYNAMIC_VHDX_EXTERNAL_ID = 'dynamic-vhdx'
    protected static final String LINKED_EXTERNAL_ID = 'linked'

    static Collection<StorageVolumeType> getDatastoreStorageTypes() {
        Collection<StorageVolumeType> volumeTypes = []

        volumeTypes << new StorageVolumeType(
                code: SCVMM_DATASTORE_CODE,
                displayName: SCVMM_DATASTORE_DISPLAY,
                name: SCVMM_DATASTORE_NAME,
                description: SCVMM_DATASTORE_DISPLAY,
                volumeType: VOLUME_TYPE_DATASTORE,
                enabled: true,
                displayOrder: 1,
                customLabel: false,
                customSize: false,
                defaultType: false,
                autoDelete: true,
                minStorage: (1L * ComputeUtility.ONE_GIGABYTE),
                allowSearch: false,
                volumeCategory: VOLUME_CATEGORY_DATASTORE
        )

        volumeTypes << new StorageVolumeType(
                code: SCVMM_FILESHARE_CODE,
                displayName: SCVMM_FILESHARE_DISPLAY,
                name: SCVMM_FILESHARE_NAME,
                description: SCVMM_FILESHARE_DISPLAY,
                volumeType: VOLUME_TYPE_DATASTORE,
                enabled: true,
                displayOrder: 2,
                customLabel: false,
                customSize: false,
                defaultType: false,
                autoDelete: true,
                minStorage: (1L * ComputeUtility.ONE_GIGABYTE),
                allowSearch: false,
                volumeCategory: VOLUME_CATEGORY_DATASTORE
        )

        return volumeTypes
    }

    static Collection<StorageVolumeType> getVhdDiskStorageTypes() {
        Collection<StorageVolumeType> diskTypes = []

        // VHD format - Generation 1 VM only
        diskTypes << new StorageVolumeType(
                code: FIXED_VHD_CODE,
                displayName: FIXED_VHD_DISPLAY,
                name: FIXED_VHD_NAME,
                description: FIXED_VHD_DISPLAY,
                volumeType: VOLUME_TYPE_DISK,
                externalId: FIXED_VHD_EXTERNAL_ID,
                enabled: true,
                displayOrder: 1,
                customLabel: true,
                customSize: true,
                defaultType: true,
                autoDelete: true,
                hasDatastore: true,
                allowSearch: true,
                volumeCategory: VOLUME_CATEGORY_DISK
        )

        diskTypes << new StorageVolumeType(
                code: DYNAMIC_VHD_CODE,
                displayName: DYNAMIC_VHD_DISPLAY,
                name: DYNAMIC_VHD_NAME,
                description: DYNAMIC_VHD_DISPLAY,
                volumeType: VOLUME_TYPE_DISK,
                externalId: DYNAMIC_VHD_EXTERNAL_ID,
                enabled: true,
                displayOrder: 2,
                customLabel: true,
                customSize: true,
                defaultType: true,
                autoDelete: true,
                hasDatastore: true,
                allowSearch: true,
                volumeCategory: VOLUME_CATEGORY_DISK
        )

        diskTypes << new StorageVolumeType(
                code: DIFFERENCING_VHD_CODE,
                displayName: DIFFERENCING_VHD_DISPLAY,
                name: DIFFERENCING_VHD_NAME,
                description: DIFFERENCING_VHD_DISPLAY,
                volumeType: VOLUME_TYPE_DISK,
                externalId: DIFFERENCING_EXTERNAL_ID,
                enabled: true,
                displayOrder: 3,
                customLabel: true,
                customSize: true,
                defaultType: true,
                autoDelete: true,
                hasDatastore: true,
                allowSearch: true,
                volumeCategory: VOLUME_CATEGORY_DISK
        )

        return diskTypes
    }

    static Collection<StorageVolumeType> getVhdxDiskStorageTypes() {
        Collection<StorageVolumeType> diskTypes = []

        diskTypes << new StorageVolumeType(
                code: FIXED_VHDX_CODE,
                displayName: FIXED_VHDX_DISPLAY,
                name: FIXED_VHDX_NAME,
                description: FIXED_VHDX_DISPLAY,
                volumeType: VOLUME_TYPE_DISK,
                externalId: FIXED_VHDX_EXTERNAL_ID,
                enabled: true,
                displayOrder: 4,
                customLabel: true,
                customSize: true,
                defaultType: true,
                autoDelete: true,
                hasDatastore: true,
                allowSearch: true,
                volumeCategory: VOLUME_CATEGORY_DISK
        )

        diskTypes << new StorageVolumeType(
                code: DYNAMIC_VHDX_CODE,
                displayName: DYNAMIC_VHDX_DISPLAY,
                name: DYNAMIC_VHDX_NAME,
                description: DYNAMIC_VHDX_DISPLAY,
                volumeType: VOLUME_TYPE_DISK,
                externalId: DYNAMIC_VHDX_EXTERNAL_ID,
                enabled: true,
                displayOrder: 5,
                customLabel: true,
                customSize: true,
                defaultType: true,
                autoDelete: true,
                hasDatastore: true,
                allowSearch: true,
                volumeCategory: VOLUME_CATEGORY_DISK
        )

        diskTypes << new StorageVolumeType(
                code: DIFFERENCING_VHDX_CODE,
                displayName: DIFFERENCING_VHDX_DISPLAY,
                name: DIFFERENCING_VHDX_NAME,
                description: DIFFERENCING_VHDX_DISPLAY,
                volumeType: VOLUME_TYPE_DISK,
                externalId: DIFFERENCING_EXTERNAL_ID,
                enabled: true,
                displayOrder: 6,
                customLabel: true,
                customSize: true,
                defaultType: true,
                autoDelete: true,
                hasDatastore: true,
                allowSearch: true,
                volumeCategory: VOLUME_CATEGORY_DISK
        )

        return diskTypes
    }

    static Collection<StorageVolumeType> getLinkedPhysicalDiskStorageTypes() {
        Collection<StorageVolumeType> diskTypes = []

        diskTypes << new StorageVolumeType(
                code: LINKED_PHYSICAL_CODE,
                displayName: LINKED_PHYSICAL_DISPLAY,
                name: LINKED_PHYSICAL_NAME,
                description: LINKED_PHYSICAL_DISPLAY,
                volumeType: VOLUME_TYPE_DISK,
                externalId: LINKED_EXTERNAL_ID,
                enabled: true,
                displayOrder: 7,
                customLabel: true,
                customSize: true,
                defaultType: true,
                autoDelete: true,
                hasDatastore: true,
                allowSearch: true,
                volumeCategory: VOLUME_CATEGORY_DISK
        )

        return diskTypes
    }

    static Collection<StorageVolumeType> getAllStorageVolumeTypes() {
        Collection<StorageVolumeType> allTypes = []
        allTypes.addAll(getDatastoreStorageTypes())
        allTypes.addAll(getVhdDiskStorageTypes())
        allTypes.addAll(getVhdxDiskStorageTypes())
        allTypes.addAll(getLinkedPhysicalDiskStorageTypes())
        return allTypes
    }
}