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

import java.nio.file.Path;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.model.Statement;

import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;

/**
 * Loads the {@link Statement}s that are in a file on the local filesystem
 * into an instance of Rya.
 */
@ParametersAreNonnullByDefault
public interface LoadStatementsFile {

    /**
     * Loads the {@link Statement}s that are in a file on the local filesystem
     * into an instance of Rya.
     *
     * @param instanceName - Indicates which Rya instance will store the {@link Statement}s. (not null)
     * @param statementsFile - The file to load. (not null)
     * @param format - The format of the statement file. (not null)
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws CommandException Something caused the command to fail.
     */
    public void load(final String instanceName, final Path statementsFile, final StatementFileFormat format)
            throws InstanceDoesNotExistException, CommandException;

    /**
     * Defines the RDF Statement file formats that are supported by the {@link LoadStatementsFile} command.
     */
    public static enum StatementFileFormat {
        NTRIPLE,
        TURTLE;

        // TODO can replace this with the RIO jar's thing.

        // TODO Fill this in.
    }
}