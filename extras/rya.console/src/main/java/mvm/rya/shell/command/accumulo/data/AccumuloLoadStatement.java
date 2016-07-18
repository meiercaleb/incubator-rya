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
package mvm.rya.shell.command.accumulo.data;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.openrdf.model.Statement;

import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.accumulo.AccumuloCommand;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;
import mvm.rya.shell.command.data.LoadStatement;

// TODO impl, test

/**
 * An Accumulo implementation of the {@link LoadStatement} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloLoadStatement extends AccumuloCommand implements LoadStatement {

    /**
     * Constructs an instance of {@link AccumuloLoadStatement}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloLoadStatement(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
    }

    @Override
    public void loadStatement(final String instanceName, final Statement statement) throws InstanceDoesNotExistException, CommandException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}