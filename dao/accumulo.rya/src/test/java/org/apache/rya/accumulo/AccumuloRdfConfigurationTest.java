package org.apache.rya.accumulo;

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
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;

public class AccumuloRdfConfigurationTest {
    private static final Logger logger = LoggerFactory.getLogger(AccumuloRdfConfigurationTest.class);

    @Test
    public void testAuths() {
        String[] arr = { "U", "FOUO" };
        String str = "U,FOUO";
        Authorizations auths = new Authorizations(arr);

        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        conf.setAuths(arr);
        assertTrue(Arrays.equals(arr, conf.getAuths()));
        assertEquals(str, conf.getAuth());
        assertEquals(auths, conf.getAuthorizations());

        conf.setAuth(str);
        assertTrue(Arrays.equals(arr, conf.getAuths()));
        assertEquals(str, conf.getAuth());
        assertEquals(auths, conf.getAuthorizations());
    }

    @Test
    public void testIterators() {
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        Map<String, String> options = new HashMap<String, String>();
        options.put("key1", "value1");
        options.put("key2", "value2");
        IteratorSetting setting = new IteratorSetting(1, "test", "test2", options);

        conf.setAdditionalIterators(setting);
        IteratorSetting[] iteratorSettings = conf.getAdditionalIterators();
        assertTrue(iteratorSettings.length == 1);

        assertEquals(setting, iteratorSettings[0]);

    }

    @Test
    public void testBuilder() {
        String prefix = "rya_";
        String auth = "U";
        String visibility = "U";
        String user = "user";
        String password = "password";
        String instance = "instance";
        String zookeeper = "zookeeper";
        boolean useMock = true;
        boolean useComposite = true;
        boolean usePrefixHash = true;
        boolean useInference = true;
        boolean displayPlan = false;

        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration().getBuilder()//
                .auths(auth)//
                .visibilities(visibility)//
                .setRyaPrefix(prefix)//
                .useInference(useInference)//
                .useCompositeCardinality(useComposite)//
                .displayQueryPlan(displayPlan)//
                .setAccumuloInstance(instance)//
                .setAccumuloPassword(password)//
                .setAccumuloUser(user)//
                .setAccumuloZooKeepers(zookeeper)//
                .useMockAccumulo(useMock)//
                .useAccumuloPrefixHashing(usePrefixHash)//
                .build();

        org.junit.Assert.assertEquals(conf.getTablePrefix(), prefix);
        org.junit.Assert.assertEquals(conf.getCv(), visibility);
        org.junit.Assert.assertEquals(conf.getAuthorizations(), new Authorizations(auth));
        org.junit.Assert.assertEquals(conf.isInfer(), useInference);
        org.junit.Assert.assertEquals(conf.isUseCompositeCardinality(), useComposite);
        org.junit.Assert.assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        org.junit.Assert.assertEquals(conf.getAccumuloInstance(), instance);
        org.junit.Assert.assertEquals(conf.getAccumuloPassword(), password);
        org.junit.Assert.assertEquals(conf.getAccumuloUser(), user);
        org.junit.Assert.assertEquals(conf.getAccumuloZookeepers(), zookeeper);
        org.junit.Assert.assertEquals(conf.getUseMockAccumulo(), useMock);
        org.junit.Assert.assertEquals(conf.isPrefixRowsWithHash(), usePrefixHash);

    }

    @Test
    public void testBuilderFromProperties() throws FileNotFoundException, IOException {
        String prefix = "rya_";
        String auth = "U";
        String visibility = "U";
        String user = "user";
        String password = "password";
        String instance = "instance";
        String zookeeper = "zookeeper";
        boolean useMock = true;
        boolean useComposite = true;
        boolean usePrefixHash = true;
        boolean useInference = true;
        boolean displayPlan = false;

        Properties props = new Properties();
        props.load(new FileInputStream("src/test/resources/properties/rya.properties"));

        AccumuloRdfConfiguration conf = AccumuloRdfConfiguration.fromProperties(props);

        org.junit.Assert.assertEquals(conf.getTablePrefix(), prefix);
        org.junit.Assert.assertEquals(conf.getCv(), visibility);
        org.junit.Assert.assertEquals(conf.getAuthorizations(), new Authorizations(auth));
        org.junit.Assert.assertEquals(conf.isInfer(), useInference);
        org.junit.Assert.assertEquals(conf.isUseCompositeCardinality(), useComposite);
        org.junit.Assert.assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        org.junit.Assert.assertEquals(conf.getAccumuloInstance(), instance);
        org.junit.Assert.assertEquals(conf.getAccumuloPassword(), password);
        org.junit.Assert.assertEquals(conf.getAccumuloUser(), user);
        org.junit.Assert.assertEquals(conf.getAccumuloZookeepers(), zookeeper);
        org.junit.Assert.assertEquals(conf.getUseMockAccumulo(), useMock);
        org.junit.Assert.assertEquals(conf.isPrefixRowsWithHash(), usePrefixHash);

    }

}
