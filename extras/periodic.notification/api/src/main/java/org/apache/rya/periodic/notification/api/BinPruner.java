/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.periodic.notification.api;

import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;

/**
 * Object that cleans up old {@link BindingSet}s corresponding to the specified
 * {@link NodeBin}. This class deletes all BindingSets with the bin 
 * indicated by {@link NodeBin#getBin()}.  A BindingSet corresponds to a given
 * bin if it contains a {@link Binding} with name {@link IncrementalUpdateConstants#PERIODIC_BIN_ID}
 * and value equal to the given bin.
 *
 */
public interface BinPruner {
    
    /**
     * Cleans up all {@link BindingSet}s associated with the indicated {@link NodeBin}.
     * @param bin - NodeBin that indicates which BindingSets to delete..
     */
    public void pruneBindingSetBin(NodeBin bin);
    
}
