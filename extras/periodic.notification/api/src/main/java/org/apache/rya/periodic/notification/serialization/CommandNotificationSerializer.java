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
package org.apache.rya.periodic.notification.serialization;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.rya.periodic.notification.api.Notification;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Kafka {@link Serializer} and {@link Deserializer} for producing and consuming {@link CommandNotification}s
 * to and from Kafka.
 *
 */
public class CommandNotificationSerializer implements Serializer<CommandNotification>, Deserializer<CommandNotification> {

    private static Gson gson = new GsonBuilder()
            .registerTypeHierarchyAdapter(Notification.class, new CommandNotificationTypeAdapter()).create();
    private static final Logger LOG = LoggerFactory.getLogger(CommandNotificationSerializer.class);

    @Override
    public CommandNotification deserialize(String topic, byte[] bytes) {
        String json = null;
        try {
            json = new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.info("Unable to deserialize notification for topic: " + topic);
        }
        return gson.fromJson(json, CommandNotification.class);
    }

    @Override
    public byte[] serialize(String topic, CommandNotification command) {
        try {
            return gson.toJson(command).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.info("Unable to serialize notification: " + command  + "for topic: " + topic);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // Do nothing. Nothing to close
    }
    
    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        // Do nothing. Nothing to configure
    }
    
}
