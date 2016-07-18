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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Boilerplate code for a unit test that uses a {@link MiniAccumuloCluster}.
 * <p>
 * It uses the same instance of {@link MiniAccumuloCluster} and just clears out
 * any tables that were added between tests.
 */
public class AccumuloITBase {

    // Managed the MiniAccumuloCluster
    private static final MiniAccumuloClusterInstance cluster = new MiniAccumuloClusterInstance();

    @BeforeClass
    public static void initCluster() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        cluster.startMiniAccumulo();
    }

    @Before
    public void clearLastTest() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        cluster.clearLastTest();
    }

    @AfterClass
    public static void tearDownCluster() throws IOException, InterruptedException {
        cluster.stopMiniAccumulo();
    }

    /**
     * @return The {@link MiniAccumuloClusterInstance} used by the tests.
     */
    public MiniAccumuloClusterInstance getClusterInstance() {
        return cluster;
    }
}