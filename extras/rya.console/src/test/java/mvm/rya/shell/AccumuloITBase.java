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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import mvm.rya.accumulo.MiniAccumuloClusterInstance;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;

/**
 * The base class for any integration test that needs to use a {@link MiniAccumuloCluster}.
 */
public class AccumuloITBase {

    /**
     * TODO doc
     */
    private static final MiniAccumuloClusterInstance cluster = new MiniAccumuloClusterInstance();

    /**
     * Details about the values that were used to create the connector to the cluster.
     */
    private static AccumuloConnectionDetails connectionDetails = null;

    @BeforeClass
    public static void startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        cluster.startMiniAccumulo();

        connectionDetails = new AccumuloConnectionDetails(
                cluster.getUsername(),
                cluster.getPassword().toCharArray(),
                cluster.getInstanceName(),
                cluster.getZookeepers());
    }

    @Before
    public void clearLastTest() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        cluster.clearLastTest();
    }

    @AfterClass
    public static void stopMiniAccumulo() throws IOException, InterruptedException {
        cluster.stopMiniAccumulo();
    }

    /**
     * @return A mini Accumulo cluster that can be used to test the commands against.
     */
    public MiniAccumuloCluster getTestCluster() {
        return cluster.getCluster();
    }

    /**
     * @return An accumulo connector that is connected to the mini cluster.
     */
    public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
        return cluster.getConnector();
    }

    /**
     * @return Details about the values that were used to create the connector to the cluster.
     */
    public AccumuloConnectionDetails getConnectionDetails() {
        return connectionDetails;
    }
}