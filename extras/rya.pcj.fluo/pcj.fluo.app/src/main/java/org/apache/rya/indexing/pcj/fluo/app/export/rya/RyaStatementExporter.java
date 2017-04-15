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
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter.ResultExportException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaStatementExporter;

/**
 * Exports RyaStatements to Accumulo backed Rya instances using the underlying AccumuloRyaDAO.
 *
 */
public class RyaStatementExporter implements IncrementalRyaStatementExporter {

    private AccumuloRyaDAO dao;
    
    public RyaStatementExporter(AccumuloRyaDAO dao) {
        this.dao = dao;
    }
    
    @Override
    public void export(String contructID, RyaStatement statement) throws ResultExportException {
        try {
            dao.add(statement);
        } catch (RyaDAOException e) {
            throw new ResultExportException(e.getMessage());
        }
    }

}
