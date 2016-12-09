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
package org.apache.rya.api;

import com.google.common.base.Preconditions;

/**
 * This is a base class meant to be extended by Rya configuration builders. Any
 * class extending this class inherits setter methods that should be common to
 * all Rya implementations.
 * 
 * @param <B>
 *            the configuration builder returned by each of the setter methods
 * @param <C>
 *            the configuration object returned by the builder when build is
 *            called
 */
public abstract class RdfCloudTripleStoreConfigurationBuilder<B extends RdfCloudTripleStoreConfigurationBuilder<B, C>, C extends RdfCloudTripleStoreConfiguration> {

    private String prefix = "rya_";
    private String auths;
    private String visibilities;
    private boolean useInference = false;
    private boolean displayPlan = false;

    /**
     * Auxiliary method to return specified Builder in a type safe way
     * 
     * @return specified builder
     */
    protected abstract B confBuilder();

    /**
     * Auxiliary method to return type of configuration object constructed by
     * builder in a type safe way
     * 
     * @return specified configuration
     */
    protected abstract C createConf();

    /**
     * Set whether to use backwards chaining inferencing during query
     * evaluation. The default value is false.
     * 
     * @param useInference
     *            - turn inferencing on and off in Rya
     * @return B - concrete builder class
     */
    public B useInference(boolean useInference) {
        this.useInference = useInference;
        return confBuilder();
    }

    /**
     * 
     * Sets the authorization for querying the underlying data store.
     * 
     * @param auths
     *            - authorizations for querying underlying datastore
     * @return B - concrete builder class
     */
    public B auths(String auths) {
        Preconditions.checkNotNull(auths, "User auths cannot be null.");
        this.auths = auths;
        return confBuilder();
    }

    /**
     * Set the column visibities for ingested triples. If no value is set,
     * triples won't have a visibility.
     * 
     * @param visibilities
     *            - visibilities assigned to any triples inserted into Rya
     * @return B - concrete builder class
     */
    public B visibilities(String visibilites) {
        Preconditions.checkNotNull(visibilites, "Visibilities cannot be null.");
        this.visibilities = visibilites;
        return confBuilder();
    }

    /**
     *
     * Sets the prefix for the Rya instance to connect to. This parameter is
     * required and the default value is "rya_"
     * 
     * @param prefix
     *            - the prefix for the Rya instance
     * @return B - concrete builder class
     */
    public B setRyaPrefix(String prefix) {
        Preconditions.checkNotNull(prefix, "Rya prefix cannot be null.");
        this.prefix = prefix;
        return confBuilder();
    }

    /**
     * Set whether to display query plan during optimization. The default value
     * is false.
     * 
     * @param displayPlan
     *            - display the parsed query plan during query evaluation
     *            (useful for debugging)
     * @return B - concrete builder class
     */
    public B displayQueryPlan(boolean displayPlan) {
        this.displayPlan = displayPlan;
        return confBuilder();
    }

    public C build() {
        return getConf(createConf());
    }

    private C getConf(C conf) {

        Preconditions.checkNotNull(auths, "Authorizations cannot be null");

        conf.setInfer(useInference);
        conf.setTablePrefix(prefix);
        conf.setInt("sc.cloudbase.numPartitions", 3);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, auths);
        if (visibilities != null) {
            conf.set(RdfCloudTripleStoreConfiguration.CONF_CV, visibilities);
        }
        conf.setDisplayQueryPlan(displayPlan);

        return conf;
    }

    protected static void checkBooleanString(String boolString) {
        Preconditions.checkNotNull(boolString, "Boolean string cannot be null.");
        Preconditions.checkArgument(boolString.equalsIgnoreCase("true") || boolString.equalsIgnoreCase("false"),
                "Invalid boolean string");
    }

    protected static boolean getBoolean(String boolString) {
        checkBooleanString(boolString);
        return boolString.equalsIgnoreCase("true");
    }

}
