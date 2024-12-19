/*
* Copyright 2022 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.morpheus.scvmm

import com.morpheusdata.core.Plugin

class ScvmmPlugin extends Plugin {

    @Override
    String getCode() {
        return 'morpheus-scvmm-plugin'
    }

    @Override
    void initialize() {
        this.setName("SCVMM")
        this.registerProviders(
                new ScvmmCloudProvider(this,this.morpheus),
                new ScvmmProvisionProvider(this,this.morpheus),
                new ScvmmBackupProvider(this,this.morpheus),
                new ScvmmOptionSourceProvider(this, this.morpheus)
        )
    }

	/**
	 * Called when a plugin is being removed from the plugin manager (aka Uninstalled)
	 */
	@Override
	void onDestroy() {
		List<String> seedsToRun = [
			"application.ZoneTypesSCVMMSeed",
			"application.ProvisionTypeScvmmSeed",
		]
		this.morpheus.services.seed.reinstallSeedData(seedsToRun)
		// needs to be synchronous to prevent seeds from running during plugin install
	}

}
