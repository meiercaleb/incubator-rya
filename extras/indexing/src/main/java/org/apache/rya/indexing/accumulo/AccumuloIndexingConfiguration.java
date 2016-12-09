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
package org.apache.rya.indexing.accumulo;

import java.util.Properties;

import org.apache.rya.accumulo.AbstractAccumuloRdfConfigurationBuilder;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRdfConfigurationBuilder;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;
import org.apache.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import org.apache.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import org.apache.rya.indexing.external.PrecomputedJoinIndexer;
import org.openrdf.sail.Sail;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * This class is an extension of the AccumuloRdfConfiguration object used to to
 * create a {@link Sail} connection to an Accumulo backed instance of Rya. This
 * configuration object is designed to create Accumulo Rya Sail connections
 * where one or more of the Accumulo Rya Indexes are enabled. These indexes
 * include the {@link AccumuloFreeTextIndexer}, {@link AccumuloTemporalIndexer},
 * {@link EntityCentricIndex}, and the {@link PrecomputedJoinIndexer}.
 *
 */
public class AccumuloIndexingConfiguration extends AccumuloRdfConfiguration {

    private AccumuloIndexingConfiguration() {
    }

    public static AccumuloIndexingConfigBuilder builder() {
        return new AccumuloIndexingConfigBuilder();
    }

    /**
     * Creates an AccumuloIndexingConfiguration object from a Properties file.
     * This method assumes that all values in the Properties file are Strings
     * and that the Properties file uses the keys below.
     * 
     * <br>
     * <ul>
     * <li>"accumulo.auths" - String of Accumulo authorizations. Default is
     * empty String.
     * <li>"accumulo.visibilities" - String of Accumulo visibilities assigned to
     * ingested triples.
     * <li>"accumulo.instance" - Accumulo instance name (required)
     * <li>"accumulo.user" - Accumulo user (required)
     * <li>"accumulo.password" - Accumulo password (required)
     * <li>"accumulo.rya.prefix" - Prefix for Accumulo backed Rya instance.
     * Default is "rya_"
     * <li>"accumulo.zookeepers" - Zookeepers for underlying Accumulo instance
     * (required if not using Mock)
     * <li>"use.mock" - Use a MockAccumulo instance as back-end for Rya
     * instance. Default is false.
     * <li>"use.prefix.hashing" - Use prefix hashing for triples. Helps avoid
     * hot-spotting. Default is false.
     * <li>"use.count.stats" - Use triple pattern cardinalities for query
     * optimization. Default is false.
     * <li>"use.join.selectivity" - Use join selectivity for query optimization.
     * Default is false.
     * <li>"use.display.plan" - Display query plan during evaluation. Useful for
     * debugging. Default is true.
     * <li>"use.inference" - Use backward chaining inference during query
     * evaluation. Default is false.
     * <li>"use.freetext" - Use Accumulo Freetext Indexer for querying and
     * ingest. Default is false.
     * <li>"use.temporal" - Use Accumulo Temporal Indexer for querying and
     * ingest. Default is false.
     * <li>"use.entity" - Use Accumulo Entity Index for querying and ingest.
     * Default is false.
     * <li>"fluo.app.name" - Set name of Fluo App to update PCJs.
     * <li>"use.pcj" - Use PCJs for query optimization. Default is false.
     * <li>"use.optimal.pcj" - Use optimal PCJ for query optimization. Default
     * is false.
     * <li>"pcj.tables" - PCJ tables to be used, specified as comma delimited
     * Strings with no spaces between. If no tables are specified, all
     * registered tables are used.
     * <li>"freetext.predicates" - Freetext predicates used for ingest. Specify
     * as comma delimited Strings with no spaces between. Empty by default.
     * <li>"temporal.predicates" - Temporal predicates used for ingest. Specify
     * as comma delimited Strings with no spaces between. Empty by default.
     * </ul>
     * <br>
     * 
     * @param props
     *            - Properties file containing Accumulo specific configuration
     *            parameters
     * @return AccumumuloIndexingConfiguration with properties set
     */
    public static AccumuloIndexingConfiguration fromProperties(Properties props) {
        return AccumuloIndexingConfigBuilder.fromProperties(props);
    }

    /**
     * 
     * Specify whether to use use {@link EntitCentricIndex} for ingest and at
     * query time. The default value is false, and if useEntity is set to true
     * and the EntityIndex does not exist, then useEntity will default to false.
     * 
     * @param useEntity
     *            - use entity indexing
     */
    public void setUseEntity(boolean useEntity) {
        setBoolean(ConfigUtils.USE_ENTITY, useEntity);
    }

    public boolean getUseEntity() {
        return getBoolean(ConfigUtils.USE_ENTITY, false);
    }

    /**
     * 
     * Specify whether to use use {@link AccumuloTemproalIndexer} for ingest and
     * at query time. The default value is false, and if useTemporal is set to
     * true and the TemporalIndex does not exist, then useTemporal will default
     * to false.
     * 
     * @param useTemporal
     *            - use temporal indexing
     */
    public void setUseTemporal(boolean useTemporal) {
        setBoolean(ConfigUtils.USE_TEMPORAL, useTemporal);
    }

    public boolean getUseTemporal() {
        return getBoolean(ConfigUtils.USE_TEMPORAL, false);
    }

    public boolean getUseFreetext() {
        return getBoolean(ConfigUtils.USE_FREETEXT, false);
    }

    /**
     * 
     * Specify whether to use use {@link AccumuloFreeTextIndexer} for ingest and
     * at query time. The default value is false, and if useFreeText is set to
     * true and the FreeTextIndex does not exist, then useFreeText will default
     * to false.
     * 
     * @param useFreeText
     *            - use freetext indexing
     */
    public void setUseFreetext(boolean useFreetext) {
        setBoolean(ConfigUtils.USE_FREETEXT, useFreetext);
    }

    public boolean getUsePCJUpdater() {
        return getBoolean(ConfigUtils.USE_PCJ_UPDATER_INDEX, false);
    }

    public void setUsePCJUpdater(boolean usePCJUpdater) {
        setBoolean(ConfigUtils.USE_PCJ_UPDATER_INDEX, usePCJUpdater);
        if (usePCJUpdater) {
            set(ConfigUtils.PCJ_STORAGE_TYPE, "ACCUMULO");
            set(ConfigUtils.PCJ_UPDATER_TYPE, "FLUO");
        }
    }

    /**
     * 
     * Specify the name of the PCJ Fluo updater application. A non-null
     * application results in the {@link PrecomputedJoinIndexer} being activated
     * so that all triples ingested into Rya are also ingested into Fluo to
     * update any registered PCJs. PreomputedJoinIndexer is turned off by
     * default. If no fluo application of the specified name exists, a
     * RuntimeException will occur.
     * 
     * @param fluoAppName
     *            - use entity indexing
     */
    public void setFluoAppUpdaterName(String fluoAppName) {
        Preconditions.checkNotNull(fluoAppName, "Fluo app name cannot be null.");
        setUsePCJUpdater(true);
        set(ConfigUtils.FLUO_APP_NAME, fluoAppName);
    }

    public String getFluoAppUpdaterName() {
        return get(ConfigUtils.FLUO_APP_NAME);
    }

    /**
     * Use Precomputed Joins as a query optimization.
     * 
     * @param usePcj
     *            - use PCJ
     */
    public void setUsePCJ(boolean usePCJ) {
        setBoolean(ConfigUtils.USE_PCJ, usePCJ);
    }

    public boolean getUsePCJ() {
        return getBoolean(ConfigUtils.USE_PCJ, false);
    }

    /**
     * Use Precomputed Joins as a query optimization and attempt to find the
     * best combination of PCJ in the query plan
     * 
     * @param useOptimalPcj
     *            - use optimal pcj plan
     */
    public void setUseOptimalPCJ(boolean useOptimalPCJ) {
        setBoolean(ConfigUtils.USE_OPTIMAL_PCJ, useOptimalPCJ);
    }

    public boolean getUseOptimalPCJ() {
        return getBoolean(ConfigUtils.USE_OPTIMAL_PCJ, false);
    }

    public void setAccumuloFreeTextPredicates(String[] predicates) {
        Preconditions.checkNotNull(predicates, "Freetext predicates cannot be null.");
        setStrings(ConfigUtils.FREETEXT_PREDICATES_LIST, predicates);
    }

    public String[] getAccumuloFreeTextPredicates() {
        return getStrings(ConfigUtils.FREETEXT_PREDICATES_LIST);
    }

    public void setAccumuloTemporalPredicates(String[] predicates) {
        Preconditions.checkNotNull(predicates, "Freetext predicates cannot be null.");
        setStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST, predicates);
    }

    public String[] getAccumuloTemporalPredicates() {
        return getStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST);
    }

    /**
     * Concrete extension of {@link AbstractAccumuloRdfConfigurationBuilder}
     * that adds setter methods to configure Accumulo Rya Indexers in addition
     * the core Accumulo Rya configuration. This builder should be used instead
     * of {@link AccumuloRdfConfigurationBuilder} to configure a query client to
     * use one or more Accumulo Indexers.
     *
     */
    public static class AccumuloIndexingConfigBuilder extends
            AbstractAccumuloRdfConfigurationBuilder<AccumuloIndexingConfigBuilder, AccumuloIndexingConfiguration> {

        private String fluoAppName;
        private boolean useFreetext = false;
        private boolean useTemporal = false;
        private boolean useEntity = false;
        private String[] freetextPredicates;
        private String[] temporalPredicates;
        private boolean usePcj = false;
        private boolean useOptimalPcj = false;
        private String[] pcjs = new String[0];

        private static final String USE_FREETEXT = "use.freetext";
        private static final String USE_TEMPORAL = "use.temporal";
        private static final String USE_ENTITY = "use.entity";
        private static final String FLUO_APP_NAME = "fluo.app.name";
        private static final String USE_PCJ = "use.pcj";
        private static final String USE_OPTIMAL_PCJ = "use.optimal.pcj";
        private static final String TEMPORAL_PREDICATES = "temporal.predicates";
        private static final String FREETEXT_PREDICATES = "freetext.predicates";
        private static final String PCJ_TABLES = "pcj.tables";

        /**
         * Creates an AccumuloIndexingConfiguration object from a Properties
         * file. This method assumes that all values in the Properties file are
         * Strings and that the Properties file uses the keys below.
         * 
         * <br>
         * <ul>
         * <li>"accumulo.auths" - String of Accumulo authorizations. Default is
         * empty String.
         * <li>"accumulo.visibilities" - String of Accumulo visibilities
         * assigned to ingested triples.
         * <li>"accumulo.instance" - Accumulo instance name (required)
         * <li>"accumulo.user" - Accumulo user (required)
         * <li>"accumulo.password" - Accumulo password (required)
         * <li>"accumulo.rya.prefix" - Prefix for Accumulo backed Rya instance.
         * Default is "rya_"
         * <li>"accumulo.zookeepers" - Zookeepers for underlying Accumulo
         * instance (required if not using Mock)
         * <li>"use.mock" - Use a MockAccumulo instance as back-end for Rya
         * instance. Default is false.
         * <li>"use.prefix.hashing" - Use prefix hashing for triples. Helps
         * avoid hot-spotting. Default is false.
         * <li>"use.count.stats" - Use triple pattern cardinalities for query
         * optimization. Default is false.
         * <li>"use.join.selectivity" - Use join selectivity for query
         * optimization. Default is false.
         * <li>"use.display.plan" - Display query plan during evaluation. Useful
         * for debugging. Default is true.
         * <li>"use.inference" - Use backward chaining inference during query
         * evaluation. Default is false.
         * <li>"use.freetext" - Use Accumulo Freetext Indexer for querying and
         * ingest. Default is false.
         * <li>"use.temporal" - Use Accumulo Temporal Indexer for querying and
         * ingest. Default is false.
         * <li>"use.entity" - Use Accumulo Entity Index for querying and ingest.
         * Default is false.
         * <li>"fluo.app.name" - Set name of Fluo App to update PCJs
         * <li>"use.pcj" - Use PCJs for query optimization. Default is false.
         * <li>"use.optimal.pcj" - Use optimal PCJ for query optimization.
         * Default is false.
         * <li>"pcj.tables" - PCJ tables to be used, specified as comma
         * delimited Strings with no spaces between. If no tables are specified,
         * all registered tables are used.
         * <li>"freetext.predicates" - Freetext predicates used for ingest.
         * Specify as comma delimited Strings with no spaces between. Empty by
         * default.
         * <li>"temporal.predicates" - Temporal predicates used for ingest.
         * Specify as comma delimited Strings with no spaces between. Empty by
         * default.
         * </ul>
         * <br>
         * 
         * @param props
         *            - Properties file containing Accumulo specific
         *            configuration parameters
         * @return AccumumuloIndexingConfiguration with properties set
         */
        public static AccumuloIndexingConfiguration fromProperties(Properties props) {
            try {
                AccumuloIndexingConfigBuilder builder = new AccumuloIndexingConfigBuilder() //
                        .auths(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_AUTHS, "")) //
                        .setRyaPrefix(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_RYA_PREFIX, "rya_"))//
                        .visibilities(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_VISIBILITIES, ""))
                        .useInference(getBoolean(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_INFERENCE, "false")))//
                        .displayQueryPlan(getBoolean(props
                                .getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_DISPLAY_QUERY_PLAN, "true")))//
                        .setAccumuloUser(props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_USER)) //
                        .setAccumuloInstance(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_INSTANCE))//
                        .setAccumuloZooKeepers(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_ZOOKEEPERS))//
                        .setAccumuloPassword(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.ACCUMULO_PASSWORD))//
                        .useMockAccumulo(getBoolean(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_MOCK_ACCUMULO, "false")))//
                        .useAccumuloPrefixHashing(getBoolean(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_PREFIX_HASHING, "false")))//
                        .useCompositeCardinality(getBoolean(
                                props.getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_COUNT_STATS, "false")))//
                        .useJoinSelectivity(getBoolean(props
                                .getProperty(AbstractAccumuloRdfConfigurationBuilder.USE_JOIN_SELECTIVITY, "false")))//
                        .useAccumuloFreetextIndex(getBoolean(props.getProperty(USE_FREETEXT, "false")))//
                        .useAccumuloTemporalIndex(getBoolean(props.getProperty(USE_TEMPORAL, "false")))//
                        .useAccumuloEntityIndex(getBoolean(props.getProperty(USE_ENTITY, "false")))//
                        .setAccumuloFreeTextPredicates(props.getProperty(FREETEXT_PREDICATES))//
                        .setAccumuloTemporalPredicates(props.getProperty(TEMPORAL_PREDICATES))//
                        .usePcj(getBoolean(props.getProperty(USE_PCJ, "false")))//
                        .useOptimalPcj(getBoolean(props.getProperty(USE_OPTIMAL_PCJ, "false")))//
                        .pcjTables(props.getProperty(PCJ_TABLES))//
                        .pcjUpdaterFluoAppName(props.getProperty(FLUO_APP_NAME));

                return builder.build();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 
         * Specify whether to use use {@link AccumuloFreeTextIndexer} for ingest
         * and at query time. The default value is false, and if useFreeText is
         * set to true and the FreeTextIndex does not exist, then useFreeText
         * will default to false.
         * 
         * @param useFreeText
         *            - use freetext indexing
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder useAccumuloFreetextIndex(boolean useFreeText) {
            this.useFreetext = useFreeText;
            return this;
        }

        /**
         * 
         * Specify whether to use use {@link AccumuloTemporalIndexer} for ingest
         * and at query time. The default value is false, and if useTemporal is
         * set to true and the TemporalIndex does not exist, then useTemporal
         * will default to false.
         * 
         * @param useTemporal
         *            - use temporal indexing
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder useAccumuloTemporalIndex(boolean useTemporal) {
            this.useTemporal = useTemporal;
            return this;
        }

        /**
         * 
         * Specify whether to use use {@link EntitCentricIndex} for ingest and
         * at query time. The default value is false, and if useEntity is set to
         * true and the EntityIndex does not exist, then useEntity will default
         * to false.
         * 
         * @param useEntity
         *            - use entity indexing
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder useAccumuloEntityIndex(boolean useEntity) {
            this.useEntity = useEntity;
            return this;
        }

        /**
         * 
         * Specify the name of the PCJ Fluo updater application. A non-null
         * application results in the {@link PrecomputedJoinIndexer} being
         * activated so that all triples ingested into Rya are also ingested
         * into Fluo to update any registered PCJs. PreomputedJoinIndexer is
         * turned off by default. If no fluo application of the specified name
         * exists, a RuntimeException will be thrown.
         * 
         * @param fluoAppName
         *            - use entity indexing
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder pcjUpdaterFluoAppName(String fluoAppName) {
            this.fluoAppName = fluoAppName;
            return this;
        }

        /**
         * 
         * @param predicates
         *            - String of comma delimited predicates used by the
         *            FreetextIndexer to determine which triples to index
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder setAccumuloFreeTextPredicates(String... predicates) {
            this.freetextPredicates = predicates;
            return this;
        }

        /**
         * 
         * @param predicates
         *            - String of comma delimited predicates used by the
         *            TemporalIndexer to determine which triples to index
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder setAccumuloTemporalPredicates(String... predicates) {
            this.temporalPredicates = predicates;
            return this;
        }

        /**
         * Use Precomputed Joins as a query optimization.
         * 
         * @param usePcj
         *            - use PCJ
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder usePcj(boolean usePcj) {
            this.usePcj = usePcj;
            return this;
        }

        /**
         * Use Precomputed Joins as a query optimization and attempt to find the
         * best combination of PCJs in the query plan
         * 
         * @param useOptimalPcj
         *            - use optimal pcj plan
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder useOptimalPcj(boolean useOptimalPcj) {
            this.useOptimalPcj = useOptimalPcj;
            return this;
        }

        /**
         * Specify a collection of PCJ tables to use for query optimization. If
         * no tables are specified and PCJs are enabled for query evaluation,
         * then all registered PCJs will be considered when optimizing the
         * query.
         * 
         * @param pcjs
         *            - array of PCJs to be used for query evaluation
         * @return AccumuloIndexingConfigBuilder
         */
        public AccumuloIndexingConfigBuilder pcjTables(String... pcjs) {
            this.pcjs = pcjs;
            return this;
        }

        public AccumuloIndexingConfiguration build() {
            AccumuloIndexingConfiguration conf = getConf(super.build());

            return conf;
        }

        private AccumuloIndexingConfiguration getConf(AccumuloIndexingConfiguration conf) {

            Preconditions.checkNotNull(conf);

            if (fluoAppName != null) {
                conf.setFluoAppUpdaterName(fluoAppName);
            }
            if (useFreetext) {
                conf.setUseFreetext(useFreetext);
                if (freetextPredicates != null) {
                    conf.setAccumuloFreeTextPredicates(freetextPredicates);
                }
            }
            if (useTemporal) {
                conf.setUseTemporal(useTemporal);
                if (temporalPredicates != null) {
                    conf.setAccumuloTemporalPredicates(temporalPredicates);
                }
            }

            if (usePcj || useOptimalPcj) {
                conf.setUsePCJ(usePcj);
                conf.setUseOptimalPCJ(useOptimalPcj);
                if (pcjs.length > 1 || (pcjs.length == 1 && pcjs[0] != null)) {
                    conf.setPcjTables(Lists.newArrayList(pcjs));
                }
            }

            conf.setBoolean(ConfigUtils.USE_ENTITY, useEntity);

            return conf;
        }

        @Override
        protected AccumuloIndexingConfigBuilder confBuilder() {
            return this;
        }

        @Override
        protected AccumuloIndexingConfiguration createConf() {
            return new AccumuloIndexingConfiguration();
        }

    }

}
