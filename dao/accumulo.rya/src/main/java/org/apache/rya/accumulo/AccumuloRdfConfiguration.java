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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.experimental.AccumuloIndexer;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;


/**
 * Created by IntelliJ IDEA.
 * Date: 4/25/12
 * Time: 3:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccumuloRdfConfiguration extends RdfCloudTripleStoreConfiguration {

    public static final String MAXRANGES_SCANNER = "ac.query.maxranges";
    
    public static final String CONF_ADDITIONAL_INDEXERS = "ac.additional.indexers";
    
    public static final String CONF_FLUSH_EACH_UPDATE = "ac.dao.flush";

    public static final String ITERATOR_SETTINGS_SIZE = "ac.iterators.size";
    public static final String ITERATOR_SETTINGS_BASE = "ac.iterators.%d.";
    public static final String ITERATOR_SETTINGS_NAME = ITERATOR_SETTINGS_BASE + "name";
    public static final String ITERATOR_SETTINGS_CLASS = ITERATOR_SETTINGS_BASE + "iteratorClass";
    public static final String ITERATOR_SETTINGS_PRIORITY = ITERATOR_SETTINGS_BASE + "priority";
    public static final String ITERATOR_SETTINGS_OPTIONS_SIZE = ITERATOR_SETTINGS_BASE + "optionsSize";
    public static final String ITERATOR_SETTINGS_OPTIONS_KEY = ITERATOR_SETTINGS_BASE + "option.%d.name";
    public static final String ITERATOR_SETTINGS_OPTIONS_VALUE = ITERATOR_SETTINGS_BASE + "option.%d.value";

    public AccumuloRdfConfiguration() {
        super();
    }

    public AccumuloRdfConfiguration(Configuration other) {
        super(other);
    }

    public AccumuloRdfConfigurationBuilder getBuilder() {
    	return new AccumuloRdfConfigurationBuilder();
    }
    
	/**
	 * Creates an AccumuloRdfConfiguration object from a Properties file.  This method assumes
	 * that all values in the Properties file are Strings and that the Properties file uses the keys below.
	 * See accumulo/rya/src/test/resources/properties/rya.properties for an example.
	 * <br>
	 * <ul>
	 * <li>"accumulo.auths" - String of Accumulo authorizations. Default is empty String.
	 * <li>"accumulo.visibilities" - String of Accumulo visibilities assigned to ingested triples.
	 * <li>"accumulo.instance" - Accumulo instance name (required)
	 * <li>"accumulo.user" - Accumulo user (required)
	 * <li>"accumulo.password" - Accumulo password (required)
	 * <li>"accumulo.rya.prefix" - Prefix for Accumulo backed Rya instance.  Default is "rya_"
     * <li>"accumulo.zookeepers" - Zookeepers for underlying Accumulo instance (required if not using Mock)
     * <li>"use.mock" - Use a MockAccumulo instance as back-end for Rya instance.  Default is false.
     * <li>"use.prefix.hashing" - Use prefix hashing for triples.  Helps avoid hot-spotting.  Default is false.
     * <li>"use.count.stats" - Use triple pattern cardinalities for query optimization.   Default is false.
     * <li>"use.join.selectivity" - Use join selectivity for query optimization.  Default is false.
     * <li>"use.display.plan" - Display query plan during evaluation.  Useful for debugging.   Default is true.
     * <li>"use.inference" - Use backward chaining inference during query evaluation.   Default is false.
     * </ul>
	 * <br>
	 * @param props - Properties file containing Accumulo specific configuration parameters
	 * @return AccumumuloRdfConfiguration with properties set
	 */
    
    public static AccumuloRdfConfiguration fromProperties(Properties props) {
    	return AccumuloRdfConfigurationBuilder.fromProperties(props).build();
    }
    
    @Override
    public AccumuloRdfConfiguration clone() {
        return new AccumuloRdfConfiguration(this);
    }
    
    public void setAccumuloUser(String user) {
    	Preconditions.checkNotNull(user);
    	set("sc.cloudbase.username", user);
    }
    
    public String getAccumuloUser(){
    	return get("sc.cloudbase.username"); 
    }
    
    public void setAccumuloPassword(String password) {
    	Preconditions.checkNotNull(password);
    	set("sc.cloudbase.password", password);
    }
    
    public String getAccumuloPassword() {
    	return get("sc.cloudbase.password");
    }
    
    public void setAccumuloZookeepers(String zookeepers) {
    	Preconditions.checkNotNull(zookeepers);
    	set("sc.cloudbase.zookeepers", zookeepers);
    }
    
    public String getAccumuloZookeepers() {
    	return get("sc.cloudbase.zookeepers");
    }
    
    public void setAccumuloInstance(String instance) {
    	Preconditions.checkNotNull(instance);
    	set("sc.cloudbase.instancename", instance);
    }
    
    public String getAccumuloInstance() {
    	return get("sc.cloudbase.instancename");
    }
    
    public void setUseMockAccumulo(boolean useMock) {
    	setBoolean(".useMockInstance", useMock);
    }
    
    public boolean getUseMockAccumulo() {
    	return getBoolean(".useMockInstance", false);
    }
    

    public Authorizations getAuthorizations() {
        String[] auths = getAuths();
        if (auths == null || auths.length == 0)
            return AccumuloRdfConstants.ALL_AUTHORIZATIONS;
        return new Authorizations(auths);
    }

    public void setMaxRangesForScanner(Integer max) {
        setInt(MAXRANGES_SCANNER, max);
    }

    public Integer getMaxRangesForScanner() {
        return getInt(MAXRANGES_SCANNER, 2);
    }

    public void setAdditionalIndexers(Class<? extends AccumuloIndexer>... indexers) {
        List<String> strs = Lists.newArrayList();
        for (Class<? extends AccumuloIndexer> ai : indexers){
            strs.add(ai.getName());
        }
        
        setStrings(CONF_ADDITIONAL_INDEXERS, strs.toArray(new String[]{}));
    }

    public List<AccumuloIndexer> getAdditionalIndexers() {
        return getInstances(CONF_ADDITIONAL_INDEXERS, AccumuloIndexer.class);
    }
    public boolean flushEachUpdate(){
        return getBoolean(CONF_FLUSH_EACH_UPDATE, true);
    }

    public void setFlush(boolean flush){
        setBoolean(CONF_FLUSH_EACH_UPDATE, flush);
    }

    public void setAdditionalIterators(IteratorSetting... additionalIterators){
        //TODO do we need to worry about cleaning up
        this.set(ITERATOR_SETTINGS_SIZE, Integer.toString(additionalIterators.length));
        int i = 0;
        for(IteratorSetting iterator : additionalIterators) {
            this.set(String.format(ITERATOR_SETTINGS_NAME, i), iterator.getName());
            this.set(String.format(ITERATOR_SETTINGS_CLASS, i), iterator.getIteratorClass());
            this.set(String.format(ITERATOR_SETTINGS_PRIORITY, i), Integer.toString(iterator.getPriority()));
            Map<String, String> options = iterator.getOptions();

            this.set(String.format(ITERATOR_SETTINGS_OPTIONS_SIZE, i), Integer.toString(options.size()));
            Iterator<Entry<String, String>> it = options.entrySet().iterator();
            int j = 0;
            while(it.hasNext()) {
                Entry<String, String> item = it.next();
                this.set(String.format(ITERATOR_SETTINGS_OPTIONS_KEY, i, j), item.getKey());
                this.set(String.format(ITERATOR_SETTINGS_OPTIONS_VALUE, i, j), item.getValue());
                j++;
            }
            i++;
        }
    }

    public IteratorSetting[] getAdditionalIterators(){
        int size = Integer.valueOf(this.get(ITERATOR_SETTINGS_SIZE, "0"));
        if(size == 0) {
            return new IteratorSetting[0];
        }

        IteratorSetting[] settings = new IteratorSetting[size];
        for(int i = 0; i < size; i++) {
            String name = this.get(String.format(ITERATOR_SETTINGS_NAME, i));
            String iteratorClass = this.get(String.format(ITERATOR_SETTINGS_CLASS, i));
            int priority = Integer.valueOf(this.get(String.format(ITERATOR_SETTINGS_PRIORITY, i)));

            int optionsSize = Integer.valueOf(this.get(String.format(ITERATOR_SETTINGS_OPTIONS_SIZE, i)));
            Map<String, String> options = new HashMap<String, String>(optionsSize);
            for(int j = 0; j < optionsSize; j++) {
                String key = this.get(String.format(ITERATOR_SETTINGS_OPTIONS_KEY, i, j));
                String value = this.get(String.format(ITERATOR_SETTINGS_OPTIONS_VALUE, i, j));
                options.put(key, value);
            }
            settings[i] = new IteratorSetting(priority, name, iteratorClass, options);
        }

        return settings;
    }
}
