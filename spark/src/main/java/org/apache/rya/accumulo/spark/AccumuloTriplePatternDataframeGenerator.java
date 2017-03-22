package org.apache.rya.accumulo.spark;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.rya.accumulo.AccumuloRdfConstants;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.TriplePatternInputFormat;
import org.apache.rya.accumulo.spark.TriplePatternRowInputFormat.StatementReturnFields;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.spark.query.SparkRyaQueryException;
import org.apache.rya.spark.query.TriplePatternDataframeGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import com.google.common.base.Preconditions;

public class AccumuloTriplePatternDataframeGenerator implements TriplePatternDataframeGenerator {

    public String tablePrefix;
    private boolean isInit = false;
    private JavaSparkContext sc;
    private SparkSession spark;
    private Configuration conf;
    private Job job;

    public AccumuloTriplePatternDataframeGenerator(SparkSession spark, Configuration conf) {
        this.spark = spark;
        this.sc = new JavaSparkContext(spark.sparkContext());
        this.conf = conf;
        tablePrefix = MRUtils.getTablePrefix(conf);
    }
    
    @Override
    public SparkSession getSparkSession() {
        return spark;
    }

    private void init() throws AccumuloSecurityException, IOException {

        String zoo = conf.get(ConfigUtils.CLOUDBASE_ZOOKEEPERS);
        String instance = conf.get(ConfigUtils.CLOUDBASE_INSTANCE);
        String user = conf.get(ConfigUtils.CLOUDBASE_USER);
        String password = conf.get(ConfigUtils.CLOUDBASE_PASSWORD);
        boolean mock = ConfigUtils.useMockInstance(conf);
        String auths = conf.get(ConfigUtils.CLOUDBASE_AUTHS);
        tablePrefix = conf.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX);

        Authorizations authorizations = null;
        if (auths != null && !auths.isEmpty()) {
            authorizations = new Authorizations(auths.split(","));
        } else {
            authorizations = AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        }

        // Check for required configuration parameters
        Preconditions.checkNotNull(instance, "Accumulo instance name [" + ConfigUtils.CLOUDBASE_INSTANCE + "] not set.");
        Preconditions.checkNotNull(user, "Accumulo user [" + ConfigUtils.CLOUDBASE_USER + "] not set.");
        Preconditions.checkNotNull(password, "Accumulo password [" + ConfigUtils.CLOUDBASE_PASSWORD + "] not set.");
        Preconditions.checkNotNull(tablePrefix, "Table prefix [" + RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX + "] not set.");
        Preconditions.checkNotNull(authorizations, "Authorization String [" + ConfigUtils.CLOUDBASE_AUTHS + "] not set.");

        job = Job.getInstance(conf, sc.appName());

        if (mock) {
            TriplePatternInputFormat.setMockInstance(job, instance);
        } else {
            Preconditions.checkNotNull(zoo, "Accumulo Zookeeper connect string [" + ConfigUtils.CLOUDBASE_ZOOKEEPERS + "] not set.");
            ClientConfiguration clientConfig = new ClientConfiguration().with(ClientProperty.INSTANCE_NAME, instance)
                    .with(ClientProperty.INSTANCE_ZK_HOST, zoo);
            TriplePatternInputFormat.setZooKeeperInstance(job, clientConfig);
        }
        TriplePatternInputFormat.setConnectorInfo(job, user, new PasswordToken(password));
        TriplePatternInputFormat.setScanAuthorizations(job, authorizations);

        isInit = true;
    }

    @Override
    public Dataset<Row> getTriplePatternDataFrame(RyaStatement pattern) throws SparkRyaQueryException {
        try {
            if (!isInit) {
                init();
            }
            TriplePatternRowInputFormat.setTriplePattern(job, pattern, tablePrefix, StatementReturnFields.All);
            JavaPairRDD<Text, StringRowWritable> rdd = sc.newAPIHadoopRDD(job.getConfiguration(), TriplePatternRowInputFormat.class,
                    Text.class, StringRowWritable.class);
            JavaRDD<Row> ryaRDD = rdd.map(x -> x._2.getRow());
            return spark.createDataFrame(ryaRDD, createRyaStatementSchema());
        } catch (Exception e) {
            throw new SparkRyaQueryException(e);
        }
    }

    @Override
    public Dataset<Row> getTriplePatternDataFrame(StatementPattern pattern) throws SparkRyaQueryException {
        try {
            if (!isInit) {
                init();
            }
            TriplePatternRowInputFormat.setTriplePattern(job, getRyaStatementFromStatementPattern(pattern), tablePrefix, StatementReturnFields.Empty);
            JavaPairRDD<Text, StringRowWritable> rdd = sc.newAPIHadoopRDD(job.getConfiguration(), TriplePatternRowInputFormat.class,
                    Text.class, StringRowWritable.class);
            JavaRDD<Row> ryaRDD = rdd.map(x -> x._2.getRow());
            return spark.createDataFrame(ryaRDD, createSpSchema(pattern, StatementReturnFields.Empty));
        } catch (Exception e) {
            throw new SparkRyaQueryException(e);
        }
    }

    private StructType createRyaStatementSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("subject", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("predicate", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("object", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("context", DataTypes.StringType, false));
        return DataTypes.createStructType(fields);
    }
    
    private StructType createSpSchema(StatementPattern pattern, StatementReturnFields fieldsToReturn) {
        List<StructField> fields = new ArrayList<>();

        Var spSubj = pattern.getSubjectVar();
        Var spPred = pattern.getPredicateVar();
        Var spObj = pattern.getObjectVar();
        Var spContext = pattern.getContextVar();

        if (!spSubj.isConstant()) {
            StructField field = DataTypes.createStructField(spSubj.getName(), DataTypes.StringType, false);
            fields.add(field);
        }

        if (!spPred.isConstant()) {
            StructField field = DataTypes.createStructField(spPred.getName(), DataTypes.StringType, false);
            fields.add(field);
        }

        if (!spObj.isConstant()) {
            StructField field = DataTypes.createStructField(spObj.getName(), DataTypes.StringType, false);
            fields.add(field);
        }

        if (spContext != null) {
            if (!spContext.isConstant()) {
                StructField field = DataTypes.createStructField(spContext.getName(), DataTypes.StringType, false);
                fields.add(field);
            }
        } else {
            StructField field = DataTypes.createStructField("context", DataTypes.StringType, false);
            fields.add(field);
        }

        return DataTypes.createStructType(fields);

    }

    private RyaStatement getRyaStatementFromStatementPattern(StatementPattern pattern) {

        RyaURI subj = null;
        RyaURI pred = null;
        RyaType obj = null;
        RyaURI context = null;

        Var spSubj = pattern.getSubjectVar();
        Var spPred = pattern.getPredicateVar();
        Var spObj = pattern.getObjectVar();
        Var spContext = pattern.getContextVar();

        if (spSubj.getValue() != null) {
            subj = RdfToRyaConversions.convertURI((URI) spSubj.getValue());
        }

        if (spPred.getValue() != null) {
            pred = RdfToRyaConversions.convertURI((URI) spPred.getValue());
        }

        if (spObj.getValue() != null) {
            obj = RdfToRyaConversions.convertValue(spObj.getValue());
        }

        if (spContext != null && spContext.getValue() != null) {
            context = RdfToRyaConversions.convertURI((URI) spContext.getValue());
        }

        return new RyaStatement(subj, pred, obj, context);

    }

}
