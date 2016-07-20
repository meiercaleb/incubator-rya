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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Optional;

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.shell.SharedShellState.ConnectionState;
import mvm.rya.shell.SharedShellState.ShellState;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.RyaCommands;
import mvm.rya.shell.command.administrative.GetInstanceDetails;
import mvm.rya.shell.command.administrative.Install.DuplicateInstanceNameException;
import mvm.rya.shell.command.administrative.Install.InstallConfiguration;
import mvm.rya.shell.util.InstallPrompt;
import mvm.rya.shell.util.InstanceNamesFormatter;
import mvm.rya.shell.util.RyaDetailsFormatter;

/**
 * Rya Shell commands that have to do with administrative tasks.
 */
@Component
public class RyaAdminCommands implements CommandMarker {

    public static final String CREATE_PCJ_CMD = "create-pcj";
    public static final String DELETE_PCJ_CMD = "delete-pcj";
    public static final String GET_INSTANCE_DETAILS_CMD = "get-instance-details";
    public static final String INSTALL_CMD = "install";
    public static final String LIST_INSTANCES_CMD = "list-instances";
    public static final String UNINSTALL_CMD = "uninstall";

    private final SharedShellState state;
    private final InstallPrompt installPrompt;

    /**
     * Constructs an instance of {@link RyaAdminCommands}.
     *
     * @param state - Holds shared state between all of the command classes. (not null)
     * @param installPrompt - Prompts a user for installation details. (not null)
     */
    @Autowired
    public RyaAdminCommands(final SharedShellState state, final InstallPrompt installPrompt) {
        this.state = requireNonNull( state );
        this.installPrompt = requireNonNull(installPrompt);
    }

    @CliAvailabilityIndicator({
        LIST_INSTANCES_CMD,
        INSTALL_CMD })
    public boolean areStorageCommandsAvailable() {
        switch(state.getShellState().getConnectionState()) {
            case CONNECTED_TO_STORAGE:
            case CONNECTED_TO_INSTANCE:
                return true;
            default:
                return false;
        }
    }

    @CliAvailabilityIndicator({
        GET_INSTANCE_DETAILS_CMD,
        UNINSTALL_CMD })
    public boolean areInstanceCommandsAvailable() {
        switch(state.getShellState().getConnectionState()) {
            case CONNECTED_TO_INSTANCE:
                return true;
            default:
                return false;
        }
    }

    @CliAvailabilityIndicator({
        CREATE_PCJ_CMD,
        DELETE_PCJ_CMD })
    public boolean arePCJCommandsAvailable() {
        // The PCJ commands are only available if the Shell is connected to an instance of Rya
        // that is new enough to use the RyaDetailsRepository and is configured to maintain PCJs.
        final ShellState shellState = state.getShellState();
        if(shellState.getConnectionState() == ConnectionState.CONNECTED_TO_INSTANCE) {
            final GetInstanceDetails getInstanceDetails = shellState.getConnectedCommands().get().getGetInstanceDetails();
            final String instanceName = shellState.getConnectionDetails().get().getInstanceName();
            try {
                final Optional<RyaDetails> instanceDetails = getInstanceDetails.getDetails( instanceName );
                if(instanceDetails.isPresent()) {
                    return instanceDetails.get().getPCJIndexDetails().isEnabled();
                }
            } catch (final CommandException e) {
                return false;
            }
        }
        return false;
    }

    @CliCommand(value = LIST_INSTANCES_CMD, help = "List the names of the installed Rya instances.")
    public String listInstances() {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaCommands commands = shellState.getConnectedCommands().get();
        final Optional<String> ryaInstance = shellState.getRyaInstanceName();

        try {
            // Sort the names alphabetically.
            final List<String> instanceNames = commands.getListInstances().listInstances();
            Collections.sort( instanceNames );

            String report;
            final InstanceNamesFormatter formatter = new InstanceNamesFormatter();
            if(ryaInstance.isPresent()) {
                report = formatter.format(instanceNames, ryaInstance.get());
            } else {
                report = formatter.format(instanceNames);
            }
            return report;

        } catch (final CommandException e) {
            throw new RuntimeException("Can not list the Rya instances. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = INSTALL_CMD, help = "Create a new instance of Rya.")
    public String install() {
        // Fetch the commands that are connected to the store.
        final RyaCommands commands = state.getShellState().getConnectedCommands().get();

        String instanceName = null;
        InstallConfiguration installConfig = null;
        try {
            boolean verified = false;
            while(!verified) {
                // Use the install prompt to fetch the user's installation options.
                instanceName = installPrompt.promptInstanceName();
                installConfig = installPrompt.promptInstallConfiguration();

                // Verify the configuration is what the user actually wants to do.
                verified = installPrompt.promptVerified(instanceName, installConfig);
            }

            // Execute the command.
            commands.getInstall().install(instanceName, installConfig);
            return String.format("The Rya instance named '%s' has been installed.", instanceName);

        } catch(final DuplicateInstanceNameException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' already exists. Try again with a different name.", instanceName), e);
        } catch (final IOException | CommandException e) {
            throw new RuntimeException("Could not install a new instance of Rya. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = GET_INSTANCE_DETAILS_CMD, help = "Print information about how the Rya instance is configured.")
    public String getInstanceDetails() {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaCommands commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            final Optional<RyaDetails> details = commands.getGetInstanceDetails().getDetails(ryaInstance);
            if(details.isPresent()) {
                return new RyaDetailsFormatter().format(details.get());
            } else {
                return "This instance of Rya does not have a Rya Details table. Consider migrating to a newer version of Rya.";
            }
        } catch(final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final CommandException e) {
            throw new RuntimeException("Could not get the instance details. Reason: " + e.getMessage(), e);
        }
    }


    @CliCommand(value = CREATE_PCJ_CMD, help = "Creates and starts the maintenance of a new PCJ using a Fluo application.")
    public String createPcj(
            @CliOption(key = {"sparql"}, mandatory = true, help = "The SPARQL that defines the PCJ.")
            final String sparql) {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaCommands commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            // Execute the command.
            final String pcjId = commands.getCreatePCJ().createPCJ(ryaInstance, sparql);
            // Return a message that indicates the ID of the newly created ID.
            return String.format("The PCJ has been created. Its ID is '%s'.", pcjId);
        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final CommandException e) {
            throw new RuntimeException("Could not create the PCJ. Provided reasons: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = DELETE_PCJ_CMD, help = "Deletes and halts maintenance of a PCJ.")
    public String deletePcj(
            @CliOption(key = {"pcjId"}, mandatory = true, help = "The ID of the PCJ that will be deleted.")
            final String pcjId) {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaCommands commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            // Execute the command.
            commands.getDeletePCJ().deletePCJ(ryaInstance, pcjId);
            return "The PCJ has been deleted.";

        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final CommandException e) {
            throw new RuntimeException("The PCJ could not be deleted. Provided reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = UNINSTALL_CMD, help = "Remove an instance of Rya.")
    public String uninstall() {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaCommands commands = shellState.getConnectedCommands().get();
        final String ryaInstance = shellState.getRyaInstanceName().get();

        try {
            commands.getUninstall().uninstall(ryaInstance);
            return String.format("Rya instance named '%s' uninstalled.", ryaInstance);

        } catch (final InstanceDoesNotExistException e) {
            throw new RuntimeException(String.format("A Rya instance named '%s' does not exist.", ryaInstance), e);
        } catch (final CommandException e) {
            throw new RuntimeException(String.format("Could not uninstall the Rya instance named '%s'. Reason: %s", ryaInstance, e.getMessage()), e);
        }
    }
}