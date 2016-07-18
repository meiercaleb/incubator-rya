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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;

import mvm.rya.shell.AccumuloITBase;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.administrative.Install;
import mvm.rya.shell.command.administrative.Install.InstallConfiguration;
import mvm.rya.shell.command.administrative.ListInstances;

/**
 * Integration tests the methods of {@link AccumuloListInstances}.
 */
public class AccumuloListInstancesIT extends AccumuloITBase {

    @Test
    public void listInstances_hasRyaDetailsTable() throws AccumuloException, AccumuloSecurityException, CommandException {
        // Install a few instances of Rya using the install command.
        final Install install = new AccumuloInstall(getConnectionDetails(), getConnector());
        install.install("instance1_", InstallConfiguration.builder().build());
        install.install("instance2_", InstallConfiguration.builder().build());
        install.install("instance3_", InstallConfiguration.builder().build());

        // Fetch the list and verify it matches what is expected.
        final ListInstances listInstances = new AccumuloListInstances(getConnectionDetails(), getConnector());
        final List<String> instances = listInstances.listInstances();
        Collections.sort(instances);

        final List<String> expected = Lists.newArrayList("instance1_", "instance2_", "instance3_");
        assertEquals(expected, instances);
    }
}