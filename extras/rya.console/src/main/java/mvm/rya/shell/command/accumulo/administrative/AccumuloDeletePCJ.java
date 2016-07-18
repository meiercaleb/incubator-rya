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

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.accumulo.AccumuloCommand;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;
import mvm.rya.shell.command.administrative.DeletePCJ;
import mvm.rya.shell.command.administrative.GetInstanceDetails;

// TODO impl, test

/**
 * An Accumulo implementation of the {@link DeletePCJ} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloDeletePCJ extends AccumuloCommand implements DeletePCJ {

    private static final Logger log = LoggerFactory.getLogger(AccumuloDeletePCJ.class);

    private final GetInstanceDetails getInstanceDetails;

    /**
     * Constructs an instance of {@link AccumuloDeletePCJ}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloDeletePCJ(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, connector);
    }

    @Override
    public void deletePCJ(final String instanceName, final String pcjId) throws InstanceDoesNotExistException, CommandException {
        requireNonNull(instanceName);
        requireNonNull(pcjId);

        // Only newer instance of Rya maintain details about how they are configured.
        // These are also the only instances that use Fluo to maintain PCJs. If details
        // are present, then update them to exclude the deleted PCJ. If that PCJ is also
        // being maintained by Fluo, then also remove it from the Fluo app.
        final Optional<RyaDetails> originalDetails = getInstanceDetails.getDetails(instanceName);
        if(originalDetails.isPresent()) {

            // Only continue if PCJ Indexing is enabled.
            if(!originalDetails.get().getPCJIndexDetails().isEnabled()) {
                throw new CommandException(String.format("The '%s' instance of Rya does not have PCJ Indexing enabled.", instanceName));
            }

            // If the PCJ was being maintained by a Fluo application, then stop that process.
            final PCJIndexDetails pcjIndexDetails  = originalDetails.get().getPCJIndexDetails();
            final PCJDetails droppedPcjDetails = pcjIndexDetails.getPCJDetails().get( pcjId );
            if(droppedPcjDetails.getUpdateStrategy().isPresent()) {
                if(droppedPcjDetails.getUpdateStrategy().get() == PCJUpdateStrategy.INCREMENTAL) {
                    final Optional<FluoDetails> fluoDetailsHolder = pcjIndexDetails.getFluoDetails();

                    if(fluoDetailsHolder.isPresent()) {
                        final String fluoAppName = pcjIndexDetails.getFluoDetails().get().getUpdateAppName();
                        stopUpdatingPCJ(fluoAppName, pcjId);
                    } else {
                        log.error(String.format("Could not stop the Fluo application from updating the PCJ because the Fluo Details are " +
                                "missing for the Rya instance named '%s'.", instanceName));
                    }
                }
            }
        }

        // Drop the table that holds the PCJ results from Accumulo.
        final PrecomputedJoinStorage pcjs = new AccumuloPcjStorage(getConnector(), instanceName);
        try {
            pcjs.dropPcj(pcjId);
        } catch (final PCJStorageException e) {
            throw new CommandException("Could not drop the PCJ's table from Accumulo.", e);
        }
    }

    private void stopUpdatingPCJ(final String fluoApp, final String pcjId) {
        // TODO impl this
        // WE HAVE A PROBLEM HERE. THE FLUO APPLICATION DOES NOT SUPPORT THIS AT THE MOMENT!
    }
}