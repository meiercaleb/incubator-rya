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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.Test;

import mvm.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.shell.AccumuloITBase;
import mvm.rya.shell.command.CommandException;

/**
 * Integration tests the methods of {@link AccumuloInstanceExists}.
 */
public class AccumuloInstanceExistsIT extends AccumuloITBase {

    @Test
    public void exists_ryaDetailsTable() throws AccumuloException, AccumuloSecurityException, CommandException, TableExistsException {
        final Connector connector = getConnector();
        final TableOperations tableOps = connector.tableOperations();

        // Create the Rya instance's Rya details table.
        final String instanceName = "test_instance_";
        final String ryaDetailsTable = instanceName + AccumuloRyaInstanceDetailsRepository.INSTANCE_DETAILS_TABLE_NAME;
        tableOps.create(ryaDetailsTable);

        // Verify the command reports the instance exists.
        final AccumuloInstanceExists instanceExists = new AccumuloInstanceExists(getConnectionDetails(), getConnector());
        assertTrue( instanceExists.exists(instanceName) );
    }

    @Test
    public void exists_dataTables() throws AccumuloException, AccumuloSecurityException, CommandException, TableExistsException {
        final Connector connector = getConnector();
        final TableOperations tableOps = connector.tableOperations();

        // Create the Rya instance's Rya details table.
        final String instanceName = "test_instance_";

        final String spoTableName = instanceName + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
        final String ospTableName = instanceName + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX;
        final String poTableName = instanceName + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX;
        tableOps.create(spoTableName);
        tableOps.create(ospTableName);
        tableOps.create(poTableName);

        // Verify the command reports the instance exists.
        final AccumuloInstanceExists instanceExists = new AccumuloInstanceExists(getConnectionDetails(), getConnector());
        assertTrue( instanceExists.exists(instanceName) );
    }

    @Test
    public void doesNotExist() throws CommandException, AccumuloException, AccumuloSecurityException {
        // Verify the command reports the instance does not exists.
        final AccumuloInstanceExists instanceExists = new AccumuloInstanceExists(getConnectionDetails(), getConnector());
        assertFalse( instanceExists.exists("some_instance") );
    }
}