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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.google.common.base.Optional;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.RyaSailRepository;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.accumulo.AccumuloCommand;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;
import mvm.rya.shell.command.administrative.CreatePCJ;
import mvm.rya.shell.command.administrative.GetInstanceDetails;

// TODO test

/**
 * An Accumulo implementation of the {@link CreatePCJ} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloCreatePCJ extends AccumuloCommand implements CreatePCJ {

    private final GetInstanceDetails getInstanceDetails;

    /**
     * Constructs an instance of {@link AccumuloCreatePCJ}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloCreatePCJ(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
        getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, connector);
    }

    @Override
    public String createPCJ(final String instanceName, final String sparql) throws InstanceDoesNotExistException, CommandException {
        requireNonNull(instanceName);
        requireNonNull(sparql);

        // Ensure the instance of Rya is new enough that it maintains Rya Details that describe its configuration.
        final Optional<RyaDetails> ryaDetailsHolder = getInstanceDetails.getDetails(instanceName);
        if(!ryaDetailsHolder.isPresent()) {
            throw new CommandException( String.format("The '%s' instance of Rya appears to be an old version. It does not " +
                    "have any Rya Details specifying how to maintain the PCJ index, so the PCJ could not be created.", instanceName) );
        }

        // Ensure PCJ Indexing is turned on.
        final PCJIndexDetails pcjIndexDetails = ryaDetailsHolder.get().getPCJIndexDetails();
        if(!pcjIndexDetails.isEnabled()) {
            throw new CommandException(String.format("The '%s' instance of Rya does not have PCJ Indexing enabled.", instanceName));
        }

        // Ensure a Fluo application is associated with this instance of Rya.
        final Optional<FluoDetails> fluoDetailsHolder = pcjIndexDetails.getFluoDetails();
        if(!fluoDetailsHolder.isPresent()) {
            throw new CommandException( String.format("Can not create a PCJ for the '%s' instance of Rya because it does" +
                    "not have a Fluo application associated with it. Update the instance's PCJ Index Details to fix this problem.", instanceName) );
        }

        // Create the PCJ table that will receive the index results.
        final String pcjId;
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getConnector(), instanceName);
        try {
            pcjId = pcjStorage.createPcj(sparql);
        } catch (final PCJStorageException e) {
            throw new CommandException("Problem while initializing the PCJ table.", e);
        }

        // Task the Fluo application with updating the PCJ.
        final String fluoAppName = fluoDetailsHolder.get().getUpdateAppName();
        try {
            updateFluoApp(instanceName, fluoAppName, pcjStorage, pcjId);
        } catch (RepositoryException | MalformedQueryException | SailException | QueryEvaluationException | PcjException e) {
            throw new CommandException("Problem while initializing the Fluo application with the new PCJ.", e);
        }

        // Return the ID that was assigned to the PCJ.
        return pcjId;
    }

    private void updateFluoApp(final String ryaInstance, final String fluoAppName, final PrecomputedJoinStorage pcjStorage, final String pcjId) throws RepositoryException, MalformedQueryException, SailException, QueryEvaluationException, PcjException {
        requireNonNull(pcjStorage);
        requireNonNull(pcjId);

        // Setup the Fluo Client that is able to talk to the fluo updater app.
        final FluoClient fluoClient = createFluoClient(getAccumuloConnectionDetails(), fluoAppName);

        // Setup the Rya client that is able to talk to scan Rya's statements.
        final RyaSailRepository ryaSailRepo = makeRyaRepository(getConnector(), ryaInstance);

        // Initialize the PCJ within the Fluo application.
        final org.apache.rya.indexing.pcj.fluo.api.CreatePcj fluoCreatePcj = new org.apache.rya.indexing.pcj.fluo.api.CreatePcj();
        fluoCreatePcj.withRyaIntegration(pcjId, pcjStorage, fluoClient, ryaSailRepo);
    }

    private static FluoClient createFluoClient(final AccumuloConnectionDetails connectionDetails, final String appName) {
        requireNonNull(connectionDetails);
        requireNonNull(appName);

        final FluoConfiguration fluoConfig = new FluoConfiguration();

        // Fluo configuration values.
        fluoConfig.setApplicationName( appName );
        fluoConfig.setInstanceZookeepers( connectionDetails.getZookeepers() +  "/fluo" );

        // Accumulo Connection Stuff.
        fluoConfig.setAccumuloZookeepers( connectionDetails.getZookeepers() );
        fluoConfig.setAccumuloInstance( connectionDetails.getInstanceName() );
        fluoConfig.setAccumuloUser( connectionDetails.getUsername() );
        fluoConfig.setAccumuloPassword( new String(connectionDetails.getPassword()) );

        // Connect the client.
        return FluoFactory.newClient(fluoConfig);
    }

    private static RyaSailRepository makeRyaRepository(final Connector connector, final String ryaInstance) throws RepositoryException {
        checkNotNull(connector);
        checkNotNull(ryaInstance);

        // Setup Rya configuration values.
        final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
        ryaConf.setTablePrefix( ryaInstance );

        // Connect to the Rya repo.
        final AccumuloRyaDAO accumuloRyaDao = new AccumuloRyaDAO();
        accumuloRyaDao.setConnector(connector);
        accumuloRyaDao.setConf(ryaConf);

        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        ryaStore.setRyaDAO(accumuloRyaDao);

        final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
        ryaRepo.initialize();
        return ryaRepo;
    }
}