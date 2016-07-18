/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package mvm.rya.shell.command.accumulo.administrative;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Test;

import mvm.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import mvm.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import mvm.rya.shell.AccumuloITBase;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.administrative.Install;
import mvm.rya.shell.command.administrative.Install.DuplicateInstanceNameException;
import mvm.rya.shell.command.administrative.Install.InstallConfiguration;
import mvm.rya.shell.command.administrative.InstanceExists;

/**
 * Integration tests the methods of {@link AccumuloInstall}.
 */
public class AccumuloInstallIT extends AccumuloITBase {

    @Test
    public void install() throws AccumuloException, AccumuloSecurityException, DuplicateInstanceNameException, CommandException, NotInitializedException, RyaDetailsRepositoryException {
        // Install an instance of Rya.
        final String instanceName = "testInstance_";
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableEntityCentricIndex(true)
                .setEnableFreeTextIndex(true)
                .setEnableTemporalIndex(true)
                .setEnablePcjIndex(true)
                .setEnableGeoIndex(true)
                .setFluoPcjAppName("fluo_app_name")
                .build();

        final Install install = new AccumuloInstall(getConnectionDetails(), getConnector());
        install.install(instanceName, installConfig);

        // Check that the instance exists.
        final InstanceExists instanceExists = new AccumuloInstanceExists(getConnectionDetails(), getConnector());
        instanceExists.exists(instanceName);
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void install_alreadyExists() throws DuplicateInstanceNameException, CommandException, AccumuloException, AccumuloSecurityException {
        // Install an instance of Rya.
        final String instanceName = "testInstance_";
        final InstallConfiguration installConfig = InstallConfiguration.builder().build();

        final Install install = new AccumuloInstall(getConnectionDetails(), getConnector());
        install.install(instanceName, installConfig);

        // Install it again.
        install.install(instanceName, installConfig);
    }
}