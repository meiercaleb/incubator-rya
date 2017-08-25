package org.apache.rya.mongodb.instance;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.AlreadyInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.ConcurrentUpdateException;
import org.apache.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;

/**
 * Tests the methods of {@link AccumuloRyaDetailsRepository} by using a {@link MiniAccumuloCluster}.
 */
public class MongoRyaDetailsRepositoryIT extends MongoTestBase {
    private MongoClient client;

    @Before
    public void startMongo() {
        client = super.getMongoClient();
    }

    @Test
    public void initializeAndGet() throws AlreadyInitializedException, RyaDetailsRepositoryException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
          //RYA-215            .setGeoIndexDetails( new GeoIndexDetails(true) )
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
                                    .setLastUpdateTime( new Date() ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of MongoDB.
        final RyaDetailsRepository repo = new MongoRyaInstanceDetailsRepository(client, instanceName);

        // Initialize the repository
        repo.initialize(details);

        // Fetch the stored details.
        final RyaDetails stored = repo.getRyaInstanceDetails();

        // Ensure the fetched object is equivalent to what was stored.
        assertEquals(details, stored);
    }

    @Test(expected = AlreadyInitializedException.class)
    public void initialize_alreadyInitialized() throws AlreadyInitializedException, RyaDetailsRepositoryException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
          //RYA-215            .setGeoIndexDetails( new GeoIndexDetails(true) )
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
                                    .setLastUpdateTime( new Date() ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of MongoDB.
        final RyaDetailsRepository repo = new MongoRyaInstanceDetailsRepository(client, instanceName);

        // Initialize the repository
        repo.initialize(details);

        // Initialize it again.
        repo.initialize(details);
    }

    @Test(expected = NotInitializedException.class)
    public void getRyaInstance_notInitialized() throws NotInitializedException, RyaDetailsRepositoryException {
        // Setup the repository that will be tested using a mock instance of Accumulo.
        final RyaDetailsRepository repo = new MongoRyaInstanceDetailsRepository(client, "testInstance");

        // Try to fetch the details from the uninitialized repository.
        repo.getRyaInstanceDetails();
    }

    @Test
    public void isInitialized_true() throws AlreadyInitializedException, RyaDetailsRepositoryException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
          //RYA-215            .setGeoIndexDetails( new GeoIndexDetails(true) )
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
                                    .setLastUpdateTime( new Date() ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of MongoDB.
        final RyaDetailsRepository repo = new MongoRyaInstanceDetailsRepository(client, "testInstance");

        // Initialize the repository
        repo.initialize(details);

        // Ensure the repository reports that it has been initialized.
        assertTrue( repo.isInitialized() );
    }

    @Test
    public void isInitialized_false() throws RyaDetailsRepositoryException {
        // Setup the repository that will be tested using a mock instance of MongoDB.
        final RyaDetailsRepository repo = new MongoRyaInstanceDetailsRepository(client, "testInstance");

        // Ensure the repository reports that is has not been initialized.
        assertFalse( repo.isInitialized() );
    }

    @Test
    public void update() throws AlreadyInitializedException, RyaDetailsRepositoryException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
          //RYA-215            .setGeoIndexDetails( new GeoIndexDetails(true) )
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
                                    .setLastUpdateTime( new Date() ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of MongoDB.
        final RyaDetailsRepository repo = new MongoRyaInstanceDetailsRepository(client, "testInstance");

        // Initialize the repository
        repo.initialize(details);

        // Create a new state for the details.
        final RyaDetails updated = new RyaDetails.Builder( details )
                .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false) )
                .build();

        // Execute the update.
        repo.update(details, updated);

        // Show the new state that is stored matches the updated state.
        final RyaDetails fetched = repo.getRyaInstanceDetails();
        assertEquals(updated, fetched);
    }

    @Test(expected = ConcurrentUpdateException.class)
    public void update_outOfDate() throws AlreadyInitializedException, RyaDetailsRepositoryException {
        final String instanceName = "testInstance";

        // Create the metadata object the repository will be initialized with.
        final RyaDetails details = RyaDetails.builder()
            .setRyaInstanceName(instanceName)
            .setRyaVersion("1.2.3.4")
            .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
          //RYA-215            .setGeoIndexDetails( new GeoIndexDetails(true) )
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
                                    .setLastUpdateTime( new Date() ))
                        .addPCJDetails(
                                PCJDetails.builder()
                                    .setId("pcj 2")))
            .setProspectorDetails( new ProspectorDetails(Optional.of(new Date())) )
            .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.of(new Date())) )
            .build();

        // Setup the repository that will be tested using a mock instance of MongoDB.
        final RyaDetailsRepository repo = new MongoRyaInstanceDetailsRepository(client, "testInstance");

        // Initialize the repository
        repo.initialize(details);

        // Create a new state for the details.
        final RyaDetails wrongOriginal = new RyaDetails.Builder( details )
                .setTemporalIndexDetails( new TemporalIndexDetails(false) )
                .build();

        final RyaDetails updated = new RyaDetails.Builder( details )
                .setEntityCentricIndexDetails(new EntityCentricIndexDetails(false) )
                .build();

        // Try to execute the update where the old state is not the currently stored state.
        repo.update(wrongOriginal, updated);
    }
}