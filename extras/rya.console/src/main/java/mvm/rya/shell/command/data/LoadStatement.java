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

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.model.Statement;

import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;

/**
 * Loads a single {@link Statement} into an instance of Rya.
 */
@ParametersAreNonnullByDefault
public interface LoadStatement {

    /**
     * Loads a single {@link Statement} into an instance of Rya.
     *
     * @param instanceName - Indicates which Rya instance will store the {@link Statement}. (not null)
     * @param statement - The statement to load. (not null)
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws CommandException Something caused the command to fail.
     */
    public void loadStatement(final String instanceName, final Statement statement) throws InstanceDoesNotExistException, CommandException;
}