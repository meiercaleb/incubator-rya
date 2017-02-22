package org.apache.rya.indexing.pcj.fluo.app.batch;
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
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;

public abstract class AbstractBatchBindingSetUpdater implements BatchBindingSetUpdater {

    /**
     * Updates the Span to create a new {@link BatchInformation} object to be fed to the
     * {@link BatchObserver}.  This message is called in the event that the BatchBindingSetUpdater
     * reaches the batch size before processing all entries relevant to its Span.
     * @param newStart - new start to the Span
     * @param oldSpan - old Span to be updated with newStart
     * @return - updated Span used with an updated BatchInformation object to complete the batch task
     */
    public Span getNewSpan(RowColumn newStart, Span oldSpan) {
        return new Span(newStart, oldSpan.isStartInclusive(), oldSpan.getEnd(), oldSpan.isEndInclusive());
    }


}
