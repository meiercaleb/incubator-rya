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
package mvm.rya.shell;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import mvm.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import mvm.rya.api.instance.RyaDetails.GeoIndexDetails;
import mvm.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import mvm.rya.api.instance.RyaDetails.ProspectorDetails;
import mvm.rya.api.instance.RyaDetails.TemporalIndexDetails;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.RyaCommands;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;
import mvm.rya.shell.command.administrative.CreatePCJ;
import mvm.rya.shell.command.administrative.DeletePCJ;
import mvm.rya.shell.command.administrative.GetInstanceDetails;
import mvm.rya.shell.command.administrative.Install;
import mvm.rya.shell.command.administrative.Install.DuplicateInstanceNameException;
import mvm.rya.shell.command.administrative.Install.InstallConfiguration;
import mvm.rya.shell.command.administrative.ListInstances;
import mvm.rya.shell.command.administrative.Uninstall;
import mvm.rya.shell.util.InstallPrompt;

/**
 * Unit tests the methods of {@link RyaAdminCommands}.
 */
public class RyaAdminCommandsTest {

    @Test
    public void createPCJ() throws InstanceDoesNotExistException, CommandException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String sparql = "SELECT * WHERE { ?person <http://isA> ?noun }";
        final String pcjId = "123412342";
        final PCJUpdateStrategy updateStrategy = PCJUpdateStrategy.INCREMENTAL;
        final CreatePCJ mockCreatePCJ = mock(CreatePCJ.class);
        when(mockCreatePCJ.createPCJ( eq(instanceName), eq(sparql) ) ).thenReturn( pcjId );

        final RyaCommands mockCommands = mock(RyaCommands.class);
        when(mockCommands.getCreatePCJ()).thenReturn(mockCreatePCJ);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class));
        final String message = commands.createPcj(sparql);

        // Verify the values that were provided to the command were passed through to CreatePCJ.
        verify(mockCreatePCJ).createPCJ(eq(instanceName), eq(sparql));

        // Verify a message is returned that explains what was created.
        final String expected = "The PCJ has been created. Its ID is '123412342'.";
        assertEquals(expected, message);
    }

    @Test
    public void deletePCJ() throws InstanceDoesNotExistException, CommandException {
        // Mock the object that performs the delete operation.
        final DeletePCJ mockDeletePCJ = mock(DeletePCJ.class);

        final RyaCommands mockCommands = mock(RyaCommands.class);
        when(mockCommands.getDeletePCJ()).thenReturn( mockDeletePCJ );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        final String instanceName = "unitTests";
        state.connectedToInstance(instanceName);

        // Execute the command.
        final String pcjId = "123412342";

        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class));
        final String message = commands.deletePcj(pcjId);

        // Verify the values that were provided to the command were passed through to the DeletePCJ.
        verify(mockDeletePCJ).deletePCJ(eq(instanceName), eq(pcjId));

        // Verify a message is returned that explains what was deleted.
        final String expected = "The PCJ has been deleted.";
        assertEquals(expected, message);
    }

    @Test
    public void getInstanceDetails() throws InstanceDoesNotExistException, CommandException {
        // Mock the object that performs the get operation.
        final GetInstanceDetails mockGetInstanceDetails = mock(GetInstanceDetails.class);
        final String instanceName = "test_instance";
        final RyaDetails details = RyaDetails.builder().setRyaInstanceName(instanceName)
                .setRyaVersion("1.2.3.4")
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
                .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails( new TemporalIndexDetails(true) )
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(true)
                            .setFluoDetails( new FluoDetails("test_instance_rya_pcj_updater") )
                            .addPCJDetails(
                                    PCJDetails.builder()
                                        .setId("pcj 1")
                                        .setUpdateStrategy(PCJUpdateStrategy.BATCH)
                                        .setLastUpdateTime( new Date(1252521351L) ))
                            .addPCJDetails(
                                    PCJDetails.builder()
                                        .setId("pcj 2")
                                        .setUpdateStrategy(PCJUpdateStrategy.INCREMENTAL)))
                .setProspectorDetails( new ProspectorDetails(Optional.of(new Date(12525211L))) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date(125221351L))) )
                .build();

        when(mockGetInstanceDetails.getDetails(eq(instanceName))).thenReturn( Optional.of(details) );

        final RyaCommands mockCommands = mock(RyaCommands.class);
        when(mockCommands.getGetInstanceDetails()).thenReturn( mockGetInstanceDetails );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class));
        final String message = commands.getInstanceDetails();

        // Verify the values that were provided to the command were passed through to the GetInstanceDetails.
        verify(mockGetInstanceDetails).getDetails(eq(instanceName));

        // Verify a message is returned that includes the details.
        final String expected =
                "General Metadata:\n" +
                "  Instance Name: test_instance\n" +
                "  RYA Version: 1.2.3.4\n" +
                "Secondary Indicies:\n" +
                "  Entity Centric Index:\n" +
                "    Enabled: true\n" +
                "  Geospatial Index:\n" +
                "    Enabled: true\n" +
                "  Free Text Index:\n" +
                "    Enabled: true\n" +
                "  Temporal Index:\n" +
                "    Enabled: true\n" +
                "  PCJ Index:\n" +
                "    Enabled: true\n" +
                "    Fluo App Name: test_instance_rya_pcj_updater\n" +
                "    PCJs:\n" +
                "      ID: pcj 1\n" +
                "        Update Strategy: BATCH\n" +
                "        Last Update Time: Thu Jan 15 06:55:21 EST 1970\n" +
                "      ID: pcj 2\n" +
                "        Update Strategy: INCREMENTAL\n" +
                "        Last Update Time: unavailable\n" +
                "Statistics:\n" +
                "  Prospector:\n" +
                "    Last Update Time: Wed Dec 31 22:28:45 EST 1969\n" +
                "  Join Selectivity:\n" +
                "    Last Updated Time: Fri Jan 02 05:47:01 EST 1970\n";
        assertEquals(expected, message);
    }

    @Test
    public void install() throws DuplicateInstanceNameException, CommandException, IOException {
        // Mock the object that performs the install operation.
        final Install mockInstall = mock(Install.class);

        final RyaCommands mockCommands = mock(RyaCommands.class);
        when(mockCommands.getInstall()).thenReturn( mockInstall );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);

        // Execute the command.
        final String instanceName = "unitTests";
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableGeoIndex(true)
                .setEnablePcjIndex(true)
                .build();

        final InstallPrompt mockInstallPrompt = mock(InstallPrompt.class);
        when(mockInstallPrompt.promptInstanceName()).thenReturn( instanceName );
        when(mockInstallPrompt.promptInstallConfiguration()).thenReturn( installConfig );
        when(mockInstallPrompt.promptVerified(eq(instanceName), eq(installConfig))).thenReturn(true);

        final RyaAdminCommands commands = new RyaAdminCommands(state, mockInstallPrompt);
        final String message = commands.install();

        // Verify the values that were provided to the command were passed through to the Install.
        verify(mockInstall).install(eq(instanceName), eq(installConfig));

        // Verify a message is returned that indicates the success of the operation.
        final String expected = "The Rya instance named 'unitTests' has been installed.";
        assertEquals(expected, message);
    }

    @Test
    public void listInstances() throws CommandException, IOException {
        // Mock the object that performs the list operation.
        final ListInstances mockListInstances = mock(ListInstances.class);
        final List<String> instanceNames = Lists.newArrayList("a", "b", "c", "d");
        when(mockListInstances.listInstances()).thenReturn(instanceNames);

        final RyaCommands mockCommands = mock(RyaCommands.class);
        when(mockCommands.getListInstances()).thenReturn( mockListInstances );

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance("b");

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class));
        final String message = commands.listInstances();

        // Verify a message is returned that lists the the instances.
        final String expected =
                "Rya instance names:\n" +
                "   a\n" +
                " * b\n" +
                "   c\n" +
                "   d\n";
        assertEquals(expected, message);
    }

    @Test
    public void uninstall() throws InstanceDoesNotExistException, CommandException {
        // Mock the object that performs the uninstall operation.
        final Uninstall mockUninstall = mock(Uninstall.class);

        final RyaCommands mockCommands = mock(RyaCommands.class);
        when(mockCommands.getUninstall()).thenReturn( mockUninstall );

        final String instanceName = "unitTest";
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        // Execute the command.
        final RyaAdminCommands commands = new RyaAdminCommands(state, mock(InstallPrompt.class));
        final String message = commands.uninstall();

        // Verify the values that were provided to the command were passed through to the Uninstall.
        verify(mockUninstall).uninstall( eq(instanceName) );

        // Verify a message is returned that indictes the uninstall operation was successful.
        final String expected = "Rya instance named 'unitTest' uninstalled.";
        assertEquals(expected, message);
    }
}