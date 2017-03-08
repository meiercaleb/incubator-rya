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
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.spark.TriplePatternRowInputFormat.StatementToRowReader;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TriplePatternRowInputFormatTest {

    private String username = "root"; 
    private String tablePrefix ="rya_";
    private PasswordToken password = new PasswordToken("");
    private Instance instance;
    private AccumuloRyaDAO dao;

    @Before
    public void init() throws Exception {
        instance = new MockInstance("instance");
        Connector connector = instance.getConnector(username, password);
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(tablePrefix);
        dao = new AccumuloRyaDAO();
        dao.setConf(conf);
        dao.setConnector(connector);
        dao.init();
    }

    @After
    public void after() throws Exception {
        dao.dropAndDestroy();
    }

    @Test
    public void testInputFormatSPO() throws Exception {
        RyaStatement input1 = RyaStatement.builder()
            .setSubject(new RyaURI("http://Bob"))
            .setPredicate(new RyaURI("http://worksAt"))
            .setObject(new RyaURI("http://www.google.com"))
            .setContext(new RyaURI("http://context"))
            .setColumnVisibility(new byte[0])
            .setMetadata(new StatementMetadata())
            .build();
        
        RyaStatement input2 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Joe"))
                .setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com"))
                .setContext(new RyaURI("http://context"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();
        
        RyaStatement input3 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Joe"))
                .setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();
        
        RyaStatement input4 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Evan"))
                .setPredicate(new RyaURI("http://studiesAt"))
                .setObject(new RyaURI("http://University123"))
                .setContext(new RyaURI("http://context"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);

        Job job = Job.getInstance();

        TriplePatternRowInputFormat.setTriplePattern(job, new RyaStatement(new RyaURI("http://Joe"),new RyaURI("http://worksAt"), null, null, null), tablePrefix);
        TriplePatternRowInputFormat.setMockInstance(job, instance.getInstanceName());
        TriplePatternRowInputFormat.setConnectorInfo(job, username, password);
        TriplePatternRowInputFormat inputFormat = new TriplePatternRowInputFormat();
        
        JobContext context = new JobContextImpl(job.getConfiguration(), job.getJobID());
        List<InputSplit> splits = inputFormat.getSplits(context);
        Assert.assertEquals(1, splits.size());
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(context.getConfiguration(), new TaskAttemptID(new TaskID(), 1));

        RecordReader<Text, StringRowWritable> reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);
        StatementToRowReader StatementToRowReader = (StatementToRowReader)reader;
        StatementToRowReader.initialize(splits.get(0), taskAttemptContext);

        List<Row> results = new ArrayList<Row>();
        while(StatementToRowReader.nextKeyValue()) {
            StringRowWritable writable = StatementToRowReader.getCurrentValue();
            Row value = writable.getRow();
            Text text = StatementToRowReader.getCurrentKey();
            results.add(value);

            System.out.println("Row: " + value);
            System.out.println("Key: " + text);
        }

        System.out.println(results.size());
        Assert.assertEquals(1, results.size());
    }
    
    @Test
    public void testInputFormatSPO2() throws Exception {
        RyaStatement input1 = RyaStatement.builder()
            .setSubject(new RyaURI("http://Bob"))
            .setPredicate(new RyaURI("http://worksAt"))
            .setObject(new RyaURI("http://www.google.com"))
            .setContext(new RyaURI("http://context"))
            .setColumnVisibility(new byte[0])
            .setMetadata(new StatementMetadata())
            .build();
        
        RyaStatement input2 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Joe"))
                .setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com"))
                .setContext(new RyaURI("http://context"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();
        
        RyaStatement input3 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Joe"))
                .setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();
        
        RyaStatement input4 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Evan"))
                .setPredicate(new RyaURI("http://studiesAt"))
                .setObject(new RyaURI("http://University123"))
                .setContext(new RyaURI("http://context"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);

        Job job = Job.getInstance();

        TriplePatternRowInputFormat.setTriplePattern(job, new RyaStatement(new RyaURI("http://Joe"),null, null, null, null), tablePrefix);
        TriplePatternRowInputFormat.setMockInstance(job, instance.getInstanceName());
        TriplePatternRowInputFormat.setConnectorInfo(job, username, password);
        TriplePatternRowInputFormat inputFormat = new TriplePatternRowInputFormat();
        
        JobContext context = new JobContextImpl(job.getConfiguration(), job.getJobID());
        List<InputSplit> splits = inputFormat.getSplits(context);
        Assert.assertEquals(1, splits.size());
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(context.getConfiguration(), new TaskAttemptID(new TaskID(), 1));

        RecordReader<Text, StringRowWritable> reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);
        StatementToRowReader StatementToRowReader = (StatementToRowReader)reader;
        StatementToRowReader.initialize(splits.get(0), taskAttemptContext);

        List<Row> results = new ArrayList<Row>();
        while(StatementToRowReader.nextKeyValue()) {
            StringRowWritable writable = StatementToRowReader.getCurrentValue();
            Row value = writable.getRow();
            Text text = StatementToRowReader.getCurrentKey();
            results.add(value);

            System.out.println("Row: " + value);
            System.out.println("Key: " + text);
        }

        System.out.println(results.size());
        Assert.assertEquals(2, results.size());
    }
    
    @Test
    public void testInputFormatOSP() throws Exception {
        RyaStatement input1 = RyaStatement.builder()
            .setSubject(new RyaURI("http://Bob"))
            .setPredicate(new RyaURI("http://worksAt"))
            .setObject(new RyaURI("http://www.google.com"))
            .setContext(new RyaURI("http://context"))
            .setColumnVisibility(new byte[0])
            .setMetadata(new StatementMetadata())
            .build();
        
        RyaStatement input2 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Joe"))
                .setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com"))
                .setContext(new RyaURI("http://context"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();
        
        RyaStatement input3 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Joe"))
                .setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia"))
                .setContext(new RyaURI("http://context"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();
        
        RyaStatement input4 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Evan"))
                .setPredicate(new RyaURI("http://browsesWebWith"))
                .setObject(new RyaURI("http://www.google.com"))
                .setContext(new RyaURI("http://context"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();

        dao.add(input1);
        dao.add(input2);
        dao.add(input3);
        dao.add(input4);

        Job job = Job.getInstance();

        TriplePatternRowInputFormat.setTriplePattern(job, new RyaStatement(null, null, new RyaURI("http://www.google.com"), null, null), tablePrefix);
        TriplePatternRowInputFormat.setMockInstance(job, instance.getInstanceName());
        TriplePatternRowInputFormat.setConnectorInfo(job, username, password);
        TriplePatternRowInputFormat inputFormat = new TriplePatternRowInputFormat();
        
        JobContext context = new JobContextImpl(job.getConfiguration(), job.getJobID());
        List<InputSplit> splits = inputFormat.getSplits(context);
        Assert.assertEquals(1, splits.size());
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(context.getConfiguration(), new TaskAttemptID(new TaskID(), 1));

        RecordReader<Text, StringRowWritable> reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);
        StatementToRowReader StatementToRowReader = (StatementToRowReader)reader;
        StatementToRowReader.initialize(splits.get(0), taskAttemptContext);

        List<Row> results = new ArrayList<Row>();
        while(StatementToRowReader.nextKeyValue()) {
            StringRowWritable writable = StatementToRowReader.getCurrentValue();
            Row value = writable.getRow();
            Text text = StatementToRowReader.getCurrentKey();
            results.add(value);

            System.out.println("Row: " + value);
            System.out.println("Key: " + text);
        }

        System.out.println(results.size());
        Assert.assertEquals(2, results.size());
    }
    
    
    @Test
    public void testInputFormatContext() throws Exception {
        RyaStatement input1 = RyaStatement.builder()
            .setSubject(new RyaURI("http://Bob"))
            .setPredicate(new RyaURI("http://worksAt"))
            .setObject(new RyaURI("http://www.google.com"))
            .setContext(new RyaURI("http://context_1"))
            .setColumnVisibility(new byte[0])
            .setMetadata(new StatementMetadata())
            .build();
        
        RyaStatement input2 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Joe"))
                .setPredicate(new RyaURI("http://worksAt"))
                .setObject(new RyaURI("http://www.yahoo.com"))
                .setContext(new RyaURI("http://context_1"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();
        
        RyaStatement input3 = RyaStatement.builder()
                .setSubject(new RyaURI("http://Joe"))
                .setPredicate(new RyaURI("http://livesIn"))
                .setObject(new RyaURI("http://Virginia"))
                .setContext(new RyaURI("http://context_2"))
                .setColumnVisibility(new byte[0])
                .setMetadata(new StatementMetadata())
                .build();
        
        dao.add(input1);
        dao.add(input2);
        dao.add(input3);

        Job job = Job.getInstance();

        TriplePatternRowInputFormat.setTriplePattern(job, new RyaStatement(null, null, null, new RyaURI("http://context_1")), tablePrefix);
        TriplePatternRowInputFormat.setMockInstance(job, instance.getInstanceName());
        TriplePatternRowInputFormat.setConnectorInfo(job, username, password);
        TriplePatternRowInputFormat inputFormat = new TriplePatternRowInputFormat();
        
        JobContext context = new JobContextImpl(job.getConfiguration(), job.getJobID());
        List<InputSplit> splits = inputFormat.getSplits(context);
        Assert.assertEquals(1, splits.size());
        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(context.getConfiguration(), new TaskAttemptID(new TaskID(), 1));

        RecordReader<Text, StringRowWritable> reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);
        StatementToRowReader StatementToRowReader = (StatementToRowReader)reader;
        StatementToRowReader.initialize(splits.get(0), taskAttemptContext);

        List<Row> results = new ArrayList<Row>();
        while(StatementToRowReader.nextKeyValue()) {
            StringRowWritable writable = StatementToRowReader.getCurrentValue();
            Row value = writable.getRow();
            Text text = StatementToRowReader.getCurrentKey();
            results.add(value);

            System.out.println("Row: " + value);
            System.out.println("Key: " + text);
        }

        System.out.println(results.size());
        Assert.assertEquals(2, results.size());
    }
}

