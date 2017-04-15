package org.apache.rya.indexing.pcj.fluo.app.export.rya;
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
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.fluo.api.observer.Observer.Context;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.ConfigurationException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.IncrementalExporterFactoryException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaStatementExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaStatementExporterFactory;

import com.google.common.base.Optional;

public class RyaStatementExporterFactory implements IncrementalRyaStatementExporterFactory {

    private static final Logger log = Logger.getLogger(RyaStatementExporterFactory.class);
    
    @Override
    public Optional<IncrementalRyaStatementExporter> build(final Context context)
            throws IncrementalExporterFactoryException, ConfigurationException {
        checkNotNull(context);

        // Wrap the context's parameters for parsing.
        final RyaExportParameters params = new RyaExportParameters(context.getObserverConfiguration().toMap());

        if (params.isExportToRya()) {
            // Setup Zookeeper connection info.
            final String accumuloInstance = params.getAccumuloInstanceName().get();
            final String zookeeperServers = params.getZookeeperServers().get().replaceAll(";", ",");
            final String exporterUsername = params.getExporterUsername().get();
            final String exporterPassword = params.getExporterPassword().get();
            final String ryaInstanceName = params.getRyaInstanceName().get();
            final String fluoAppName = params.getFluoApplicationName().get();

            AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration().getBuilder().setAccumuloInstance(accumuloInstance)
                    .setAccumuloZooKeepers(zookeeperServers).setAccumuloUser(exporterUsername).setAccumuloPassword(exporterPassword)
                    .setRyaPrefix(ryaInstanceName).build();
           AccumuloRyaDAO dao = new AccumuloRyaDAO();
           dao.setConf(conf);
           try {
            dao.init();
        } catch (RyaDAOException e) {
            log.trace("Unable to initialize AccumuloRyaDAO.  Returning empty Optional.");
            throw new ConfigurationException(e.getMessage());
        }
           return Optional.of(new RyaStatementExporter(dao));
        } else {
            return Optional.absent();
        }
    }

}
