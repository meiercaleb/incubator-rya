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
package mvm.rya.shell.util;

import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.file.Files;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import mvm.rya.shell.util.ConnectorFactory;

/**
 * Tests the methods of {@link ConnectorFactory}.
 */
public class ConnectorFactoryIT {

    private static MiniAccumuloCluster cluster = null;

    @BeforeClass
    public static void startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        // Setup the mini cluster.
        final File tempDirectory = Files.createTempDirectory("testDir").toFile();
        cluster = new MiniAccumuloCluster(tempDirectory, "password");
        cluster.start();
    }

    @AfterClass
    public static void stopMiniAccumulo() throws IOException, InterruptedException {
        cluster.stop();
    }

    @Test
    public void connect_successful() throws AccumuloException, AccumuloSecurityException {
        // Setup the values that will be tested with.
        final String username = "root";
        final CharSequence password = CharBuffer.wrap( new char[] {'p','a','s','s','w','o','r','d'} );
        final String instanceName = cluster.getInstanceName();
        final String zookeepers = cluster.getZooKeepers();

        final ConnectorFactory ac = new ConnectorFactory();
        ac.connect(username, password, instanceName, zookeepers);
    }

    @Test(expected = AccumuloSecurityException.class)
    public void connect_wrongCredentials() throws AccumuloException, AccumuloSecurityException {
        // Setup the values that will be tested with.
        final String username = "root";
        final CharSequence password = CharBuffer.wrap( new char[] {'p','a','s','s'} );
        final String instanceName = cluster.getInstanceName();
        final String zookeepers = cluster.getZooKeepers();

        final ConnectorFactory ac = new ConnectorFactory();
        ac.connect(username, password, instanceName, zookeepers);
    }
}