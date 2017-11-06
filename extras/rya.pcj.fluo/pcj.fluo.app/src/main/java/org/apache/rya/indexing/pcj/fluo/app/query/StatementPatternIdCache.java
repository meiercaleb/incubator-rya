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
package org.apache.rya.indexing.pcj.fluo.app.query;

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.STATEMENT_PATTERN_ID;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.VAR_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.STATEMENT_PATTERN_IDS;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.STATEMENT_PATTERN_IDS_HASH;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;

import com.google.common.collect.Sets;

/**
 * This class caches the StatementPattern Ids so they don't have
 * to be looked up each time a new Statement needs to be processed
 * in the TripleObserver.
 *
 */
public class StatementPatternIdCache {

    private final ReentrantLock lock = new ReentrantLock();
    private static Optional<String> HASH;
    private static Set<String> IDS;
    private boolean workDone = false;

    public StatementPatternIdCache() {
        HASH = Optional.empty();
        IDS = new HashSet<>();
    }

    /**
     * This method retrieves the StatementPattern NodeIds registered in the Fluo table.
     * This method looks up the hash of the Statement Pattern Id String hash, and if it
     * is the same as the cached hash, then the cache Set of nodeIds is returned.  Otherwise,
     * this method retrieves the ids from the Fluo table.  This method is thread safe.
     * @param tx
     * @return - Set of StatementPattern nodeIds
     */
    public Set<String> getStatementPatternIds(TransactionBase tx) {
        String hash = tx.get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS_HASH).toString();
        if (HASH.isPresent() && HASH.get().equals(hash)) {
            return IDS;
        }
        lock.lock();
        try {
            if(workDone) {
                workDone = false;
                return IDS;
            }
            String ids = tx.get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS).toString();
            IDS = Sets.newHashSet(ids.split(VAR_DELIM));
            HASH = Optional.of(hash);
            workDone = true;
            return IDS;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Clears contexts of cache so that it will be repopulated next time
     * {@link StatementPatternIdCache#getStatementPatternIds(TransactionBase)} is called.
     */
    public void clear() {
        HASH = Optional.empty();
        IDS.clear();
        workDone = false;
    }

}
