/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package mvm.rya.console;

import java.io.FileInputStream;
import java.io.StringReader;
import java.util.Formatter;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.ntriples.NTriplesParserFactory;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.query.RyaQueryEngine;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaContext;

@Component
public class RyaConsoleCommands implements CommandMarker {

    private static final NTriplesParserFactory N_TRIPLES_PARSER_FACTORY = new NTriplesParserFactory();

    protected final Logger LOG = Logger.getLogger(getClass().getName());

    private final RyaContext ryaContext = RyaContext.getInstance();
    private RyaDAO ryaDAO;
    private RDFParser ntrips_parser = null;

    public RyaConsoleCommands() {
        ntrips_parser = N_TRIPLES_PARSER_FACTORY.getParser();
        ntrips_parser.setRDFHandler(new RDFHandler() {

            @Override
            public void startRDF() throws RDFHandlerException {

            }

            @Override
            public void endRDF() throws RDFHandlerException {

            }

            @Override
            public void handleNamespace(final String s, final String s1) throws RDFHandlerException {

            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                try {
                    final RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
                    ryaDAO.add(ryaStatement);
                } catch (final Exception e) {
                    throw new RDFHandlerException(e);
                }
            }

            @Override
            public void handleComment(final String s) throws RDFHandlerException {

            }
        });
    }

    /**
     * commands:
     * 1. connect(instance, user, password, zk)
     * 1.a. disconnect
     * 2. query
     * 3. add
     */

    @CliAvailabilityIndicator({"connect"})
    public boolean isConnectAvailable() {
        return true;
    }

    @CliAvailabilityIndicator({"qt", "add", "load", "disconnect"})
    public boolean isCommandAvailable() {
        return ryaDAO != null;
    }

    @CliCommand(value = "qt", help = "Query with Triple Pattern")
    public String queryTriple(
            @CliOption(key = {"subject"}, mandatory = false, help = "Subject") final String subject,
            @CliOption(key = {"predicate"}, mandatory = false, help = "Predicate") final String predicate,
            @CliOption(key = {"object"}, mandatory = false, help = "Object") final String object,
            @CliOption(key = {"context"}, mandatory = false, help = "Context") final String context,
            @CliOption(key = {"maxResults"}, mandatory = false, help = "Maximum Number of Results", unspecifiedDefaultValue = "100") final String maxResults
    ) {
        try {
            final RdfCloudTripleStoreConfiguration conf = ryaDAO.getConf().clone();
            if (maxResults != null) {
                conf.setLimit(Long.parseLong(maxResults));
            }
            final RyaQueryEngine queryEngine = ryaDAO.getQueryEngine();
            final CloseableIteration<RyaStatement, RyaDAOException> query =
                    queryEngine.query(new RyaStatement(
                            (subject != null) ? (new RyaURI(subject)) : null,
                            (predicate != null) ? (new RyaURI(predicate)) : null,
                            (object != null) ? (new RyaURI(object)) : null,
                            (context != null) ? (new RyaURI(context)) : null), conf);
            final StringBuilder sb = new StringBuilder();
            final Formatter formatter = new Formatter(sb, Locale.US);
            final String format = "%-40s %-40s %-40s %-40s\n";
            formatter.format(format, "Subject", "Predicate",
                    "Object", "Context");
            while (query.hasNext()) {
                final RyaStatement next = query.next();
                formatter.format(format, next.getSubject().getData(), next.getPredicate().getData(),
                        next.getObject().getData(), (next.getContext() != null) ? (next.getContext().getData()) : (null));
                sb.append("\n");
            }
            return sb.toString();
        } catch (final Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
        return "";
    }

    @CliCommand(value = "load", help = "Load file")
    public void load(
            @CliOption(key = {"", "file"}, mandatory = true, help = "File of ntriples rdf to load") final String file
    ) {
        //diff formats?
        //diff types of urls
        try {
            ntrips_parser.parse(new FileInputStream(file), "");
        } catch (final Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
    }

    @CliCommand(value = "add", help = "Add Statement")
    public void add(
            @CliOption(key = {"", "statement"}, mandatory = true, help = "Statement in NTriples format") final String statement) {
        try {
            ntrips_parser.parse(new StringReader(statement), "");
        } catch (final Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
    }

    @CliCommand(value = "connect", help = "Connect to Rya Triple Store")
    public String connect(
            @CliOption(key = {"instance"}, mandatory = true, help = "Accumulo Instance") final String instance,
            @CliOption(key = {"user"}, mandatory = true, help = "Accumulo User") final String user,
            @CliOption(key = {"pwd"}, mandatory = true, help = "Accumulo Pwd") final String pwd,
            @CliOption(key = {"zk"}, mandatory = true, help = "Accumulo Zk (zk=mock for the mock instance)") final String zk,
            @CliOption(key = {"pre"}, mandatory = false, help = "Accumulo table prefix", unspecifiedDefaultValue = "rya_") final String pre) {
        try {
            //using Cloudbase
            Connector connector = null;
            final AccumuloRyaDAO cryaDao = new AccumuloRyaDAO();
            if ("mock".equals(zk)) {
                //mock instance
                connector = new MockInstance(instance).getConnector(user, pwd);
            } else {
                connector = new ZooKeeperInstance(instance, zk).getConnector(user, pwd);
            }

            cryaDao.setConnector(connector);
            final AccumuloRdfConfiguration configuration = new AccumuloRdfConfiguration();
            configuration.setTablePrefix(pre);
            cryaDao.setConf(configuration);
            cryaDao.init();
            this.ryaDAO = cryaDao;
            return "Connected to Accumulo";
        } catch (final Exception e) {
            LOG.log(Level.SEVERE, "", e);
        }
        return "";
    }

    @CliCommand(value = "disconnect", help = "Disconnect from Rya Store")
    public String disconnect() {
        if (ryaDAO == null) {
            return "Command is not available because Rya is not connected. Please 'connect' first.";
        }
        try {
            this.ryaDAO.destroy();
            this.ryaDAO = null;
        } catch (final RyaDAOException e) {
            LOG.log(Level.SEVERE, "", e);
        }
        return "";
    }

    public RyaDAO getRyaDAO() {
        return ryaDAO;
    }

    public void setRyaDAO(final RyaDAO ryaDAO) {
        this.ryaDAO = ryaDAO;
    }
}
