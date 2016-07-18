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
package mvm.rya.shell.command.administrative;

import javax.annotation.ParametersAreNonnullByDefault;

import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;

/**
 * Deletes a PCJ from an instance of Rya.
 */
@ParametersAreNonnullByDefault
public interface DeletePCJ {

    /**
     * Deletes a PCJ from an instance of Rya.
     *
     * @param instanceName - Indicates which Rya instance is maintaining the PCJ. (not null)
     * @param pcjId - The ID of the PCJ that will be deleted. (not null)
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws CommandException Something caused the command to fail.
     */
    public void deletePCJ(String instanceName, final String pcjId) throws InstanceDoesNotExistException, CommandException;

    // TODO pcj does not exist exception
}