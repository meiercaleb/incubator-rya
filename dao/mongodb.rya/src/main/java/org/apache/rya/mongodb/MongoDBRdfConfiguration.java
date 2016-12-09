package org.apache.rya.mongodb;

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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;

public class MongoDBRdfConfiguration extends RdfCloudTripleStoreConfiguration {
	public static final String MONGO_INSTANCE = "mongo.db.instance";
	public static final String MONGO_INSTANCE_PORT = "mongo.db.port";
	public static final String MONGO_GEO_MAXDISTANCE = "mongo.geo.maxdist";
	public static final String MONGO_DB_NAME = "mongo.db.name";
	public static final String MONGO_COLLECTION_PREFIX = "mongo.db.collectionprefix";
	public static final String MONGO_USER = "mongo.db.user";
	public static final String MONGO_USER_PASSWORD = "mongo.db.userpassword";
	public static final String CONF_ADDITIONAL_INDEXERS = "ac.additional.indexers";
	private MongoClient mongoClient;

	public MongoDBRdfConfiguration() {
		super();
	}

	public MongoDBRdfConfiguration(Configuration other) {
		super(other);
	}

	/**
	 * Creates a MongoRdfConfiguration object from a Properties file. This
	 * method assumes that all values in the Properties file are Strings and
	 * that the Properties file uses the keys below.
	 * 
	 * <br>
	 * <ul>
	 * <li>"mongo.auths" - String of Mongo authorizations.  Empty auths used by default.
	 * <li>"mongo.visibilities" - String of Mongo visibilities assigned to ingested triples.
	 * <li>"mongo.user" - Mongo user.  Empty by default.
	 * <li>"mongo.password" - Mongo password.  Empty by default.
	 * <li>"mongo.host" - Mongo host.  Default host is "localhost"
	 * <li>"mongo.port" - Mongo port.  Default port is "27017".
	 * <li>"mongo.db.name" - Name of MongoDB.  Default name is "rya_triples".
	 * <li>"mongo.collection.prefix" - Mongo collection prefix. Default is "rya_".
	 * <li>"mongo.rya.prefix" - Prefix for Mongo Rya instance.  Same as value of "mongo.collection.prefix".
	 * <li>"use.mock" - Use a Embedded Mongo instance as back-end for Rya instance. False by default.
	 * <li>"use.display.plan" - Display query plan during evaluation. Useful for debugging.  True by default.
	 * <li>"use.inference" - Use backward chaining inference during query.  False by default.
	 * </ul>
	 * <br>
	 * 
	 * @param props
	 *            - Properties file containing Mongo specific configuration
	 *            parameters
	 * @return MongoRdfConfiguration with properties set
	 */
	public static MongoDBRdfConfiguration fromProperties(Properties props) {
		return MongoDBRdfConfigurationBuilder.fromProperties(props);
	}

	public MongoDBRdfConfigurationBuilder getBuilder() {
		return new MongoDBRdfConfigurationBuilder();
	}

	@Override
	public MongoDBRdfConfiguration clone() {
		return new MongoDBRdfConfiguration(this);
	}

	public String getTriplesCollectionName() {
		return this.get(MONGO_COLLECTION_PREFIX, "rya") + "_triples";
	}

	public String getCollectionName() {
		return this.get(MONGO_COLLECTION_PREFIX, "rya");
	}

	public void setCollectionName(String name) {
		this.set(MONGO_COLLECTION_PREFIX, name);
	}

	public String getMongoInstance() {
		return this.get(MONGO_INSTANCE, "localhost");
	}

	public void setMongoInstance(String name) {
		this.set(MONGO_INSTANCE, name);
	}

	public String getMongoPort() {
		return this.get(MONGO_INSTANCE_PORT, "27017");
	}

	public void setMongoPort(String name) {
		this.set(MONGO_INSTANCE_PORT, name);
	}

	public String getMongoDBName() {
		return this.get(MONGO_DB_NAME, "rya");
	}

	public void setMongoDBName(String name) {
		this.set(MONGO_DB_NAME, name);
	}

	public String getNameSpacesCollectionName() {
		return this.get(MONGO_COLLECTION_PREFIX, "rya") + "_ns";
	}

	public void setAdditionalIndexers(Class<? extends MongoSecondaryIndex>... indexers) {
		List<String> strs = Lists.newArrayList();
		for (Class<?> ai : indexers) {
			strs.add(ai.getName());
		}

		setStrings(CONF_ADDITIONAL_INDEXERS, strs.toArray(new String[] {}));
	}

	public List<MongoSecondaryIndex> getAdditionalIndexers() {
		return getInstances(CONF_ADDITIONAL_INDEXERS, MongoSecondaryIndex.class);
	}

	public void setMongoClient(MongoClient client) {
		this.mongoClient = client;
	}

	public MongoClient getMongoClient() {
		return mongoClient;
	}

}
