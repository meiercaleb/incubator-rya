package org.apache.rya.indexing.pcj.fluo.app;
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
import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTE;

import java.util.Arrays;

import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;

public class RyaStatementSerializer {

    public static final String VISIBILITY_DELIM = "\u0002";
    public static final String TIMESTAMP_DELIM = "\u0003";

    /**
     * Serializes a RyaStatement with a non-null subject, non-null predicate,
     * and non-null object. Only the subject, predicate, object, time-stamp, and
     * visibility are serialized. All other RyaStatement information will be
     * lost.
     * 
     * @param stmt
     *            - RyaStatement to be serialized
     * @return - serialized bytes of RyaStatement
     * @throws Exception
     */
    public static byte[] serialize(RyaStatement stmt) throws Exception {
        try {
            RyaURI subject = stmt.getSubject();
            RyaURI predicate = stmt.getPredicate();
            RyaType object = stmt.getObject();
            Preconditions.checkNotNull(subject);
            Preconditions.checkNotNull(predicate);
            Preconditions.checkNotNull(object);
            Long ts = stmt.getTimestamp();
            byte[] columnVisibility = stmt.getColumnVisibility();
            if(columnVisibility == null) {
                columnVisibility = new byte[0];
            }
            assert subject != null && predicate != null && object != null;
            byte[] subjBytes = subject.getData().getBytes();
            byte[] predBytes = predicate.getData().getBytes();
            byte[][] objBytes = RyaContext.getInstance().serializeType(object);

            return Bytes.concat(subjBytes, DELIM_BYTES, predBytes, DELIM_BYTES, objBytes[0], objBytes[1],
                    VISIBILITY_DELIM.getBytes("UTF-8"), columnVisibility, TIMESTAMP_DELIM.getBytes("UTF-8"),
                    Long.toString(ts).getBytes("UTF-8"));
        } catch (RyaTypeResolverException e) {
            throw new TripleRowResolverException(e);
        }
    }

    /**
     * Deserializes a RyaStatement from a byte array that is a concatenation of
     * subject bytes, predicate bytes, object bytes, column visibility bytes,
     * and timestamp bytes (separated by the delimiters
     * {@link RdfCloudTripleStoreConstants#DELIM_BYTES},
     * {@link RyaStatementSerializer#VISIBILITY_DELIM},
     * {@link RyaStatementSerializer#TIMESTAMP_DELIM}).
     * 
     * @param row - concatenated byte array of RyaStatement data
     * @return - deserialized RyaStatement
     * @throws Exception
     */
    public static RyaStatement deserialize(byte[] row) throws Exception {
        int firstIndex = Bytes.indexOf(row, DELIM_BYTE);
        int secondIndex = Bytes.lastIndexOf(row, DELIM_BYTE);
        int typeIndex = Bytes.indexOf(row, TYPE_DELIM_BYTE);
        byte[] first = Arrays.copyOf(row, firstIndex);
        byte[] second = Arrays.copyOfRange(row, firstIndex + 1, secondIndex);
        byte[] third = Arrays.copyOfRange(row, secondIndex + 1, typeIndex);
        int cvIndex = Bytes.indexOf(row, VISIBILITY_DELIM.getBytes("UTF-8"));
        byte[] type = Arrays.copyOfRange(row, typeIndex, cvIndex);
        int tsIndex = Bytes.indexOf(row, TIMESTAMP_DELIM.getBytes("UTF-8"));
        byte[] columnVisibility = Arrays.copyOfRange(row, cvIndex + 1, tsIndex);
        if(columnVisibility.length == 0) {
            columnVisibility = null;
        }
        byte[] ts = Arrays.copyOfRange(row, tsIndex + 1, row.length);

        byte[] obj = Bytes.concat(third, type);
        RyaStatement statement = new RyaStatement(new RyaURI(new String(first)), new RyaURI(new String(second)),
                RyaContext.getInstance().deserialize(obj));
        statement.setColumnVisibility(columnVisibility);
        statement.setTimestamp(Long.parseLong(new String(ts, "UTF-8")));
        return statement;
    }

}
