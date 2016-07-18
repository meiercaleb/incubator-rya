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
package mvm.rya.shell.command.data;

import java.util.Iterator;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.query.BindingSet;

import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;

/**
 * Queries a Rya instance with a SPARQL query.
 */
@ParametersAreNonnullByDefault
public interface Query {

    /**
     * Queries a Rya instance with a SPARQL query.
     *
     * @param instanceName - Indicates which Rya instance to query. (not null)
     * @param sparql - The SPARQL query that will be executed. (not null)
     * @return The query's results.
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws CommandException Something caused the command to fail.
     */
    public Iterator<BindingSet> queryInstance(final String instanceName, final String sparql) throws InstanceDoesNotExistException, CommandException;
}