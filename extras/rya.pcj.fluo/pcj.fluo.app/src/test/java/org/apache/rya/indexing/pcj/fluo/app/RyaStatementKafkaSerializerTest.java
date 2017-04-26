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
import static org.junit.Assert.assertEquals;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaStatementKafkaSerializer;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;


public class RyaStatementKafkaSerializerTest {

    private static final RyaStatementKafkaSerializer serializer = new RyaStatementKafkaSerializer();
    
    @Test
    public void testSerializer() throws Exception {
        RyaStatement statement = new RyaStatement(new RyaURI("uri:123"), new RyaURI("uri:234"), new RyaType(XMLSchema.INTEGER, "345"));
        statement.setColumnVisibility("U&FOUO".getBytes("UTF-8"));
        byte[] bytes = serializer.toBytes(statement);
        RyaStatement deserializedStatement = serializer.fromBytes(bytes);
        System.out.println(statement);
        System.out.println(deserializedStatement);
        assertEquals(statement, deserializedStatement);
    }
    
    @Test
    public void testSerializerWithNullColumnVis() throws Exception {
        RyaStatement statement = new RyaStatement(new RyaURI("uri:123"), new RyaURI("uri:234"), new RyaType(XMLSchema.INTEGER, "345"));
        byte[] bytes = serializer.toBytes(statement);
        RyaStatement deserializedStatement = serializer.fromBytes(bytes);
        System.out.println(statement);
        System.out.println(deserializedStatement);
        assertEquals(statement, deserializedStatement);
    }
    
}
