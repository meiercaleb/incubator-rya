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
import java.io.Serializable;
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
import org.apache.rya.accumulo.mr.RyaStatementWritable;
import org.apache.rya.accumulo.mr.TriplePatternInputFormat;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.openrdf.model.URI;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import com.google.common.base.Preconditions;

public class TriplePatternDataframeGenerator {

    public String tablePrefix;
    private boolean isInit = false;
    private JavaSparkContext sc;
    private SQLContext sqlContext;
    private Configuration conf;
    private Job job;

    public TriplePatternDataframeGenerator(SQLContext sqlContext, Configuration conf) {
        this.sqlContext = sqlContext;
        this.sc = new JavaSparkContext(sqlContext.sparkContext());
        this.conf = conf;
        tablePrefix = MRUtils.getTablePrefix(conf);
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
        Preconditions.checkNotNull(instance,
                "Accumulo instance name [" + ConfigUtils.CLOUDBASE_INSTANCE + "] not set.");
        Preconditions.checkNotNull(user, "Accumulo user [" + ConfigUtils.CLOUDBASE_USER + "] not set.");
        Preconditions.checkNotNull(password, "Accumulo password [" + ConfigUtils.CLOUDBASE_PASSWORD + "] not set.");
        Preconditions.checkNotNull(tablePrefix,
                "Table prefix [" + RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX + "] not set.");
        Preconditions.checkNotNull(authorizations,
                "Authorization String [" + ConfigUtils.CLOUDBASE_AUTHS + "] not set.");

        job = Job.getInstance(conf, sc.appName());

        if (mock) {
            TriplePatternInputFormat.setMockInstance(job, instance);
        } else {
            Preconditions.checkNotNull(zoo,
                    "Accumulo Zookeeper connect string [" + ConfigUtils.CLOUDBASE_ZOOKEEPERS + "] not set.");
            ClientConfiguration clientConfig = new ClientConfiguration().with(ClientProperty.INSTANCE_NAME, instance)
                    .with(ClientProperty.INSTANCE_ZK_HOST, zoo);
            TriplePatternInputFormat.setZooKeeperInstance(job, clientConfig);
        }
        TriplePatternInputFormat.setConnectorInfo(job, user, new PasswordToken(password));
        TriplePatternInputFormat.setScanAuthorizations(job, authorizations);

        isInit = true;
    }

    public DataFrame getTriplePatternDataFrame(RyaStatement pattern) throws IOException, AccumuloSecurityException {
        if (!isInit) {
            init();
        }
        TriplePatternInputFormat.setTriplePattern(job, pattern, tablePrefix);
        JavaPairRDD<Text, RyaStatementWritable> rdd = sc.newAPIHadoopRDD(job.getConfiguration(),
                TriplePatternInputFormat.class, Text.class, RyaStatementWritable.class);
        // JavaRDD<RyaStatementBean> ryaRDD = rdd.map(x -> new
        // RyaStatementBean(x._2.getRyaStatement()));
        JavaRDD<RyaStatementStringBean> ryaRDD = rdd.map(x -> new RyaStatementStringBean(x._2.getRyaStatement()));
        // return sqlContext.createDataFrame(ryaRDD, RyaStatementBean.class);
        return sqlContext.createDataFrame(ryaRDD, RyaStatementStringBean.class);
    }

    public DataFrame getTriplePatternDataFrame(StatementPattern pattern) throws IOException, AccumuloSecurityException {
        if (!isInit) {
            init();
        }
        TriplePatternRowInputFormat.setTriplePattern(job, getRyaStatementFromStatementPattern(pattern), tablePrefix);
        JavaPairRDD<Text, StringRowWritable> rdd = sc.newAPIHadoopRDD(job.getConfiguration(),
                TriplePatternRowInputFormat.class, Text.class, StringRowWritable.class);
        JavaRDD<Row> ryaRDD = rdd.map(x -> x._2.getRow());
        return sqlContext.createDataFrame(ryaRDD, createSchema(pattern));
    }

    private StructType createSchema(StatementPattern pattern) {
        List<StructField> fields = new ArrayList<>();

        Var spSubj = pattern.getSubjectVar();
        Var spPred = pattern.getPredicateVar();
        Var spObj = pattern.getObjectVar();
        Var spContext = pattern.getContextVar();

        if (spSubj.getValue() == null) {
            StructField field = DataTypes.createStructField(spSubj.getName(), DataTypes.StringType, false);
            fields.add(field);
        }

        if (spPred.getValue() == null) {
            StructField field = DataTypes.createStructField(spPred.getName(), DataTypes.StringType, false);
            fields.add(field);
        }

        if (spObj.getValue() == null) {
            StructField field = DataTypes.createStructField(spObj.getName(), DataTypes.StringType, false);
            fields.add(field);
        }

        if (spContext != null) {
            if (spContext.getValue() == null) {
                StructField field = DataTypes.createStructField(spContext.getName(), DataTypes.StringType, true);
                fields.add(field);
            }
        } else {
            StructField field = DataTypes.createStructField("context", DataTypes.StringType, true);
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

    public static class RyaStatementStringBean implements Serializable {
        private static final long serialVersionUID = 8991406778259942740L;
        private String subject;
        private String predicate;
        private String object;
        private static final String DELIM = "^^";

        public RyaStatementStringBean(RyaStatement statement) {
            subject = statement.getSubject().getData();
            predicate = statement.getPredicate().getData();
            RyaType type = statement.getObject();
            object = type.getData() + DELIM + type.getDataType();
        }

        public String getSubject() {
            return subject;
        }

        public String getPredicate() {
            return predicate;
        }

        public String getObject() {
            return object;
        }

    }

    public static class RyaStatementBean implements Serializable {

        private static final long serialVersionUID = -1550967738422947980L;
        private RyaURI subject;
        private RyaURI predicate;
        private RyaType object;
        private RyaURI context;

        public RyaStatementBean(RyaStatement statement) {
            subject = statement.getSubject();
            predicate = statement.getPredicate();
            object = statement.getObject();
            if (context != null) {
                context = statement.getContext();
            }
        }

        public RyaURI getSubject() {
            return subject;
        }

        public void setSubject(RyaURI subject) {
            this.subject = subject;
        }

        public RyaURI getPredicate() {
            return predicate;
        }

        public void setPredicate(RyaURI predicate) {
            this.predicate = predicate;
        }

        public RyaType getObject() {
            return object;
        }

        public void setObject(RyaType object) {
            this.object = object;
        }

        public RyaURI getContext() {
            return context;
        }

        public void setContext(RyaURI context) {
            this.context = context;
        }
    }

}
