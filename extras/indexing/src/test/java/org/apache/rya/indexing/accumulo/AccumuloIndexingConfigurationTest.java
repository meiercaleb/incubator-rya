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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloIndexingConfigurationTest {

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

        AccumuloIndexingConfiguration conf = AccumuloIndexingConfiguration.builder()//
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
                .setAccumuloFreeTextPredicates("http://pred1", "http://pred2")//
                .setAccumuloTemporalPredicates("http://pred3", "http://pred4")//
                .useAccumuloTemporalIndex(true)//
                .useAccumuloEntityIndex(true)//
                .useAccumuloFreetextIndex(true)//
                .pcjUpdaterFluoAppName("fluo")//
                .useOptimalPcj(true)//
                .pcjTables("table1", "table2").build();

        Assert.assertEquals(conf.getTablePrefix(), prefix);
        Assert.assertEquals(conf.getCv(), visibility);
        Assert.assertEquals(conf.getAuthorizations(), new Authorizations(auth));
        Assert.assertEquals(conf.isInfer(), useInference);
        Assert.assertEquals(conf.isUseCompositeCardinality(), useComposite);
        Assert.assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        Assert.assertEquals(conf.getAccumuloInstance(), instance);
        Assert.assertEquals(conf.getAccumuloPassword(), password);
        Assert.assertEquals(conf.getAccumuloUser(), user);
        Assert.assertEquals(conf.getAccumuloZookeepers(), zookeeper);
        Assert.assertEquals(conf.getUseMockAccumulo(), useMock);
        Assert.assertEquals(conf.isPrefixRowsWithHash(), usePrefixHash);
        Assert.assertTrue(
                Arrays.equals(conf.getAccumuloFreeTextPredicates(), new String[] { "http://pred1", "http://pred2" }));
        Assert.assertTrue(
                Arrays.equals(conf.getAccumuloTemporalPredicates(), new String[] { "http://pred3", "http://pred4" }));
        Assert.assertEquals(conf.getPcjTables(), Arrays.asList("table1", "table2"));
        Assert.assertEquals(conf.getUsePCJ(), false);
        Assert.assertEquals(conf.getUseOptimalPCJ(), true);
        Assert.assertEquals(conf.getUseEntity(), true);
        Assert.assertEquals(conf.getUseFreetext(), true);
        Assert.assertEquals(conf.getUseTemporal(), true);
        Assert.assertEquals(conf.getUsePCJUpdater(), true);
        Assert.assertEquals(conf.getFluoAppUpdaterName(), "fluo");

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
        props.load(new FileInputStream("src/test/resources/accumulo_rya_indexing.properties"));

        AccumuloIndexingConfiguration conf = AccumuloIndexingConfiguration.fromProperties(props);

        Assert.assertEquals(conf.getTablePrefix(), prefix);
        Assert.assertEquals(conf.getCv(), visibility);
        Assert.assertEquals(conf.getAuthorizations(), new Authorizations(auth));
        Assert.assertEquals(conf.isInfer(), useInference);
        Assert.assertEquals(conf.isUseCompositeCardinality(), useComposite);
        Assert.assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        Assert.assertEquals(conf.getAccumuloInstance(), instance);
        Assert.assertEquals(conf.getAccumuloPassword(), password);
        Assert.assertEquals(conf.getAccumuloUser(), user);
        Assert.assertEquals(conf.getAccumuloZookeepers(), zookeeper);
        Assert.assertEquals(conf.getUseMockAccumulo(), useMock);
        Assert.assertEquals(conf.isPrefixRowsWithHash(), usePrefixHash);
        Assert.assertTrue(
                Arrays.equals(conf.getAccumuloFreeTextPredicates(), new String[] { "http://pred1", "http://pred2" }));
        Assert.assertTrue(
                Arrays.equals(conf.getAccumuloTemporalPredicates(), new String[] { "http://pred3", "http://pred4" }));
        Assert.assertEquals(conf.getPcjTables(), Arrays.asList("table1", "table2"));
        Assert.assertEquals(conf.getUsePCJ(), false);
        Assert.assertEquals(conf.getUseOptimalPCJ(), true);
        Assert.assertEquals(conf.getUseEntity(), true);
        Assert.assertEquals(conf.getUseFreetext(), true);
        Assert.assertEquals(conf.getUseTemporal(), true);
        Assert.assertEquals(conf.getUsePCJUpdater(), true);
        Assert.assertEquals(conf.getFluoAppUpdaterName(), "fluo");

    }

}
