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
package org.apache.rya.accumulo;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfigurationBuilder;

import com.google.common.base.Preconditions;

/**
 * This builder class will set all of the core Accumulo-backed Rya configuration
 * parameters. Any builder extending this class will have setter methods for all
 * of the necessary parameters to connect to an Accumulo backed Rya instance.
 *
 * @param <B>
 *            - builder returned by setter methods extending this class
 * @param <C>
 *            - configuration object constructed by the builder extending this
 *            class
 */
public abstract class AbstractAccumuloRdfConfigurationBuilder<B extends AbstractAccumuloRdfConfigurationBuilder<B, C>, C extends AccumuloRdfConfiguration>
        extends RdfCloudTripleStoreConfigurationBuilder<B, C> {

    private String user;
    private String pass;
    private String zoo = "";
    private String instance;
    private boolean useMock = false;
    private boolean usePrefixHashing = false;
    private boolean useComposite = false;
    private boolean useSelectivity = false;

    protected static final String ACCUMULO_USER = "accumulo.user";
    protected static final String ACCUMULO_PASSWORD = "accumulo.password";
    protected static final String ACCUMULO_INSTANCE = "accumulo.instance";
    protected static final String ACCUMULO_AUTHS = "accumulo.auths";
    protected static final String ACCUMULO_VISIBILITIES = "accumulo.visibilities";
    protected static final String ACCUMULO_ZOOKEEPERS = "accumulo.zookeepers";
    protected static final String ACCUMULO_RYA_PREFIX = "accumulo.rya.prefix";
    protected static final String USE_INFERENCE = "use.inference";
    protected static final String USE_DISPLAY_QUERY_PLAN = "use.display.plan";
    protected static final String USE_MOCK_ACCUMULO = "use.mock";
    protected static final String USE_PREFIX_HASHING = "use.prefix.hashing";
    protected static final String USE_COUNT_STATS = "use.count.stats";
    protected static final String USE_JOIN_SELECTIVITY = "use.join.selectivity";

    /**
     * Sets Accumulo user. This is a required parameter to connect to an
     * Accumulo Rya Instance
     * 
     * @param user
     *            - Accumulo user
     */
    public B setAccumuloUser(String user) {
        Preconditions.checkNotNull(user, "User cannot be null.");
        this.user = user;
        return confBuilder();
    }

    /**
     * Sets Accumulo user's password. This is a required parameter to connect to
     * an Accumulo Rya Instance
     * 
     * @param password
     *            - password for Accumulo user specified by
     *            {@link AbstractAccumuloRdfConfigurationBuilder#setAccumuloUser(String)}
     * @return specified builder
     */
    public B setAccumuloPassword(String password) {
        Preconditions.checkNotNull(password, "Password cannot be null.");
        this.pass = password;
        return confBuilder();
    }

    /**
     * Sets name of Accumulo instance containing Rya tables. This is a required
     * parameter to connect to an Accumulo Rya instance.
     * 
     * @param instance
     *            - Accumulo instance name
     * @return specified builder
     */
    public B setAccumuloInstance(String instance) {
        Preconditions.checkNotNull(instance, "Instance cannot be null.");
        this.instance = instance;
        return confBuilder();
    }

    /**
     * Sets Accumulo Zookeepers for instance specified by
     * {@link AbstractAccumuloRdfConfigurationBuilder#setAccumuloInstance(String)}
     * . This is a required parameter if
     * {@link AbstractAccumuloRdfConfigurationBuilder#useMock} is false.
     * 
     * @param zoo
     *            - Accumuo Zookeepers
     * @return specified builder
     */
    public B setAccumuloZooKeepers(String zoo) {
        Preconditions.checkNotNull(zoo, "Zookeepers cannot be null.");
        this.zoo = zoo;
        return confBuilder();
    }

    /**
     * Specifies whether or not to use a mock version of Accumulo for the Rya
     * Instance. The default value is false. If
     * {@link AbstractAccumuloRdfConfigurationBuilder#useMock} is false, then
     * Accumulo Zookeepers must be specified.
     * 
     * @param useMock
     * @return specified builder
     */
    public B useMockAccumulo(boolean useMock) {
        this.useMock = useMock;
        return confBuilder();
    }

    /**
     * Specifies whether to use prefix hashing as a table design optimization to
     * prevent hot spotting. The default value is false.
     * 
     * @param useHash
     *            - use prefix hashing to prevent hot spotting
     * @return specified builder
     */
    public B useAccumuloPrefixHashing(boolean useHash) {
        this.usePrefixHashing = useHash;
        return confBuilder();
    }

    /**
     * Specifies whether or not to use triple cardinalities for query
     * optimization. This feature can only be used if the prospects table has
     * been created by the prospector and the default value is false.
     * 
     * @param useCompositeCardinality
     *            - use prospects statistics table for query optimization
     * @return specified builder
     */
    public B useCompositeCardinality(boolean useCompositeCardinality) {
        this.useComposite = useCompositeCardinality;
        return confBuilder();
    }

    /**
     * Specifies whether or not to use the join selectivity for query
     * optimization. This feature can only be used if the selectivity table has
     * been created and the default value is false.
     * 
     * @param useJoinSelectivity
     *            - use join selectivity statistics table for query optimization
     * @return specified builder
     */
    public B useJoinSelectivity(boolean useJoinSelectivity) {
        this.useSelectivity = useJoinSelectivity;
        return confBuilder();
    }

    public C build() {
        return getConf(super.build());
    }

    private C getConf(C conf) {

        Preconditions.checkNotNull(user, "Accumulo user cannot be null");
        Preconditions.checkNotNull(pass, "Accumulo user password cannot be null");
        Preconditions.checkNotNull(instance, "Accumulo instance cannot be null");

        if (!useMock) {
            Preconditions.checkArgument(zoo.length() > 0,
                    "Accumulo zookeepers cannot be null when not using a MockInstance");
        }

        conf.setBoolean(".useMockInstance", useMock);
        conf.setBoolean(RdfCloudTripleStoreConfiguration.CONF_PREFIX_ROW_WITH_HASH, usePrefixHashing);
        conf.set("sc.cloudbase.username", user);
        conf.set("sc.cloudbase.password", pass);
        conf.set("sc.cloudbase.instancename", instance);
        conf.set("sc.cloudbase.zookeepers", zoo);
        if (useSelectivity) {
            conf.setUseStats(true);
            conf.setCompositeCardinality(true);
            conf.setUseSelectivity(useSelectivity);
        } else if (useComposite) {
            conf.setUseStats(true);
            conf.setCompositeCardinality(useComposite);
        }

        return conf;
    }

}
