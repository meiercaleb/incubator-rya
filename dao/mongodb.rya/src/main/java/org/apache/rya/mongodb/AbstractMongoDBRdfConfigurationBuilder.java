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
package org.apache.rya.mongodb;

import org.apache.rya.api.RdfCloudTripleStoreConfigurationBuilder;

import com.google.common.base.Preconditions;

/**
 * This builder class will set all of the core Mongo-backed Rya configuration
 * parameters. Any builder extending this class will have setter methods for all
 * of the necessary parameters to connect to a Mongo backed Rya instance.
 *
 * @param <B>
 *            - builder returned by setter methods extending this class
 * @param <C>
 *            - configuration object constructed by the builder extending this
 *            class
 */
public abstract class AbstractMongoDBRdfConfigurationBuilder<B extends AbstractMongoDBRdfConfigurationBuilder<B, C>, C extends MongoDBRdfConfiguration>
        extends RdfCloudTripleStoreConfigurationBuilder<B, C> {

    private String user;
    private String pass;
    private boolean useMock = false;
    private String host = "localhost";
    private String port = DEFAULT_MONGO_PORT;
    protected static final String DEFAULT_MONGO_PORT = "27017";
    private String mongoCollectionPrefix = "rya_";
    private String mongoDBName = "rya";

    protected static final String MONGO_USER = "mongo.user";
    protected static final String MONGO_PASSWORD = "mongo.password";
    protected static final String MONGO_DB_NAME = "mongo.db.name";
    protected static final String MONGO_COLLECTION_PREFIX = "mongo.collection.prefix";
    protected static final String MONGO_HOST = "mongo.host";
    protected static final String MONGO_PORT = "mongo.port";
    protected static final String MONGO_AUTHS = "mongo.auths";
    protected static final String MONGO_VISIBILITIES = "mongo.visibilities";
    protected static final String MONGO_RYA_PREFIX = "mongo.rya.prefix";
    protected static final String USE_INFERENCE = "use.inference";
    protected static final String USE_DISPLAY_QUERY_PLAN = "use.display.plan";
    protected static final String USE_MOCK_MONGO = "use.mock";

    /**
     * Sets Mongo user.
     * 
     * @param user
     * @return specified builder
     */
    public B setMongoUser(String user) {
        this.user = user;
        return confBuilder();
    }

    /**
     * Sets password for Mongo user specified by
     * {@link AbstractMongoDBRdfConfigurationBuilder#setMongoUser(String)}.
     * 
     * @param password
     * @return specified builder
     */
    public B setMongoPassword(String password) {
        this.pass = password;
        return confBuilder();
    }

    /**
     * Sets Mongo port. This parameter must be set to connect to an instance of
     * MongoDB and will default to "27017" if no value is specified.
     * 
     * @param port
     * @return specified builder
     */
    public B setMongoPort(String port) {
        Preconditions.checkNotNull(port, "Mongo port cannot be null.");
        this.port = port;
        return confBuilder();
    }

    /**
     * Sets Mongo host. This parameter must be set to connect to an instance of
     * MongoDB and will default to "localhost" if no value is specified.
     * 
     * @param host
     * @return specified builder
     */
    public B setMongoHost(String host) {
        Preconditions.checkNotNull(host, "Mongo host cannot be null.");
        this.host = host;
        return confBuilder();
    }

    /**
     * Sets MongoDB name. This parameter must be set to connect to an instance
     * of MongoDB and will default to "rya_triples" is no value is specified.
     * 
     * @param name
     * @return specified builder
     */
    public B setMongoDBName(String name) {
        Preconditions.checkNotNull(name, "Mongo DBName cannot be null.");
        this.mongoDBName = name;
        return confBuilder();
    }

    /**
     * Sets MongoDB Collection prefix. This parameter must be set to connect to
     * an instance of MongoDB and will default to "rya_" is no value is
     * specified.
     * 
     * @param name
     * @return specified builder
     */
    public B setMongoCollectionPrefix(String prefix) {
        Preconditions.checkNotNull(prefix, "Mongo Collection Prefix cannot be null.");
        this.mongoCollectionPrefix = prefix;
        return confBuilder();
    }

    /**
     * Set whether to use instance of embedded Mongo as backend for Rya
     * instance.
     * 
     * @param useMock
     * @return specified builder
     */
    public B useMockMongo(boolean useMock) {
        this.useMock = useMock;
        return confBuilder();
    }

    public C build() {
        return getConf(super.build());
    }

    private C getConf(C conf) {

        conf.set("sc.useMongo", "true");
        conf.setBoolean(".useMockInstance", useMock);
        if (user != null) {
            conf.set(MongoDBRdfConfiguration.MONGO_USER, user);
        }
        if (pass != null) {
            conf.set(MongoDBRdfConfiguration.MONGO_USER_PASSWORD, pass);
        }
        conf.setMongoDBName(mongoDBName);
        conf.setCollectionName(mongoCollectionPrefix);
        conf.setTablePrefix(mongoCollectionPrefix);
        conf.setMongoInstance(host);
        conf.setMongoPort(port);

        return conf;
    }

}
