package org.apache.rya.spark.query;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;

public class SparqlQueryConversionUtils {

    public static Map<StatementPattern, String> buildSpTableNameMap(TupleExpr te) throws MalformedQueryException {
        List<StatementPattern> patterns = StatementPatternCollector.process(te);
        AtomicInteger counter = new AtomicInteger(1);
        return patterns.stream().collect(Collectors.toMap(Function.identity(), x -> "table" + counter.getAndIncrement()));
    }

    public static void createAndRegisterSpTables(Map<StatementPattern, String> spTableNames, TriplePatternDataframeGenerator gen)
            throws SparkRyaQueryException {
        for (StatementPattern sp : spTableNames.keySet()) {
            gen.getTriplePatternDataFrame(sp).createOrReplaceTempView(spTableNames.get(sp));

        }
    }

    public static Map<StatementPattern, String> createAndRegisterSpTables(TupleExpr te, TriplePatternDataframeGenerator gen)
            throws MalformedQueryException, SparkRyaQueryException {
        Map<StatementPattern, String> spTableNameMap = buildSpTableNameMap(te);
        createAndRegisterSpTables(spTableNameMap, gen);
        return spTableNameMap;
    }

}
