package com.morpheusdata.scvmm

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.backup.BackupJobProvider
import com.morpheusdata.core.backup.MorpheusBackupProvider
import groovy.util.logging.Slf4j

@Slf4j
class ScvmmBackupProvider extends MorpheusBackupProvider {

	BackupJobProvider backupJobProvider;

	ScvmmBackupProvider(ScvmmPlugin plugin, MorpheusContext morpheusContext) {
		super(plugin, morpheusContext)

		ScvmmBackupTypeProvider backupTypeProvider = new ScvmmBackupTypeProvider(plugin, morpheus)
		plugin.registerProvider(backupTypeProvider)
		addScopedProvider(backupTypeProvider, "scvmm", null)
	}
}
