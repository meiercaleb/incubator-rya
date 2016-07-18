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
package mvm.rya.accumulo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

/**
 * Contains boilerplate code that can be used by an integration test that
 * uses a {@link MiniAccumuloCluster}.
 * <p>
 * You can just extend {@link AccumuloITBase} if your test only requires Accumulo.
 */
public class MiniAccumuloClusterInstance {

    private static final String USERNAME = "root";
    private static final String PASSWORD = "password";

    /**
     * A mini Accumulo cluster that can be used by the tests.
     */
    private static MiniAccumuloCluster cluster = null;

    /**
     * The tables that exist in a newly created cluster.
     */
    private static List<String> originalTableNames = new ArrayList<>();

    /**
     * Start the {@link MiniAccumuloCluster}.
     */
    public void startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        // Setup the mini cluster.
        final File tempDirectory = Files.createTempDirectory("testDir").toFile();
        cluster = new MiniAccumuloCluster(tempDirectory, "password");
        cluster.start();

        // Store a list of the original table names.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector(USERNAME, new PasswordToken( new String(PASSWORD) ));
        originalTableNames.addAll( connector.tableOperations().list() );
    }

    /**
     * Deletes the tables that have been created since initialization.
     */
    public void clearLastTest() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        // Get a list of the tables that have been added by the test.
        final Instance instance = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
        final Connector connector = instance.getConnector("root", new PasswordToken("password"));
        final TableOperations tableOps = connector.tableOperations();

        final List<String> newTables = new ArrayList<>();
        newTables.addAll( tableOps.list() );
        newTables.removeAll( originalTableNames );

        // Delete all the new tables.
        for(final String newTable : newTables) {
            tableOps.delete( newTable );
        }
    }

    /**
     * Stop the {@link MiniAccumuloCluster}.
     */
    public void stopMiniAccumulo() throws IOException, InterruptedException {
        cluster.stop();
    }

    /**
     * @return The {@link MiniAccumuloCluster} managed by this class.
     */
    public MiniAccumuloCluster getCluster() {
        return cluster;
    }

    /**
     * @return An accumulo connector that is connected to the mini cluster.
     */
    public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
        return cluster.getConnector(USERNAME, PASSWORD);
    }

    /**
     * @return The root username.
     */
    public String getUsername() {
        return USERNAME;
    }

    /**
     * @return The root password.
     */
    public String getPassword() {
        return PASSWORD;
    }

    /**
     * @return The MiniAccumulo's zookeeper instance name.
     */
    public String getInstanceName() {
        return cluster.getInstanceName();
    }

    /**
     * @return The MiniAccumulo's zookeepers.
     */
    public String getZookeepers() {
        return cluster.getZooKeepers();
    }
}