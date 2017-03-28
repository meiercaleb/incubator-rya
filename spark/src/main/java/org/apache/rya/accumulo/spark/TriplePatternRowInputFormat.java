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
import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.query.strategy.TriplePatternStrategy;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.openrdf.model.vocabulary.XMLSchema;

import com.clearspring.analytics.util.Preconditions;
import com.google.common.primitives.Bytes;

public class TriplePatternRowInputFormat extends InputFormatBase<Text, StringRowWritable> {

    public static final String EMPTY_STATEMENT_FIELDS = "empty_fields";
    public static final String DELIM = ",";

    public static enum StatementReturnFields {
        Empty, All
    };

    public static void setTriplePattern(Job job, RyaStatement statement, String tablePrefix, StatementReturnFields fieldsToReturn) {
        RyaTripleContext context = new RyaTripleContext(false);
        TriplePatternStrategy strategy = context.retrieveStrategy(statement);
        try {
            Entry<TABLE_LAYOUT, ByteRange> rangeEntry = strategy.defineRange(statement.getSubject(), statement.getPredicate(),
                    statement.getObject(), statement.getContext(), new AccumuloRdfConfiguration());
            ByteRange byteRange = rangeEntry.getValue();
            Range range = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
            setRanges(job, Collections.singleton(range));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (statement.getContext() != null) {
            fetchColumns(job, Collections.singleton(new Pair<Text, Text>(new Text(statement.getContext().getData()), new Text())));
        }

        TABLE_LAYOUT layout = strategy.getLayout();
        setTableLayout(job, layout);
        TablePrefixLayoutStrategy prefixStrategy = new TablePrefixLayoutStrategy(tablePrefix);
        setInputTableName(job, prefixStrategy.getTableName(layout));
        setEmptyFields(job, statement, fieldsToReturn);
    }

    private static void setEmptyFields(Job conf, RyaStatement statement, StatementReturnFields fieldsToReturn) {

        StringBuffer emptyFieldsBuffer = new StringBuffer();

        if (fieldsToReturn == StatementReturnFields.All) {
            emptyFieldsBuffer.append(1).append(DELIM).append(1).append(DELIM).append(1).append(DELIM).append(1);
        } else {
            if (statement.getSubject() == null) {
                emptyFieldsBuffer.append(1).append(DELIM);
            } else {
                emptyFieldsBuffer.append(0).append(DELIM);
            }

            if (statement.getPredicate() == null) {
                emptyFieldsBuffer.append(1).append(DELIM);
            } else {
                emptyFieldsBuffer.append(0).append(DELIM);
            }

            if (statement.getObject() == null) {
                emptyFieldsBuffer.append(1).append(DELIM);
            } else {
                emptyFieldsBuffer.append(0).append(DELIM);
            }

            if (statement.getContext() == null) {
                emptyFieldsBuffer.append(1).append(DELIM);
            } else {
                emptyFieldsBuffer.append(0).append(DELIM);
            }

            emptyFieldsBuffer.delete(emptyFieldsBuffer.length() - 1, emptyFieldsBuffer.length());
        }
        conf.getConfiguration().set(EMPTY_STATEMENT_FIELDS, emptyFieldsBuffer.toString());

    }

    private static void setTableLayout(Job conf, TABLE_LAYOUT layout) {
        conf.getConfiguration().set(MRUtils.TABLE_LAYOUT_PROP, layout.name());
    }

    @Override
    public RecordReader<Text, StringRowWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new StatementToRowReader();
    }

    protected static class StatementToRowReader extends RecordReaderBase<Text, StringRowWritable> {

        private TABLE_LAYOUT tableLayout;
        private List<Integer> rowPosToInclude;

        @Override
        public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
            super.initialize(inSplit, attempt);
            Configuration conf = attempt.getConfiguration();
            this.tableLayout = MRUtils.getTableLayout(conf, TABLE_LAYOUT.OSP);
            int[] nullFieldArray = conf.getInts(EMPTY_STATEMENT_FIELDS);

            rowPosToInclude = new ArrayList<>();
            for (int i = 0; i < nullFieldArray.length; i++) {
                if (convertIntToBoolean(nullFieldArray[i])) {
                    rowPosToInclude.add(i);
                }
            }

        }

        private boolean convertIntToBoolean(int boolInt) {
            if (boolInt == 0) {
                return false;
            } else if (boolInt == 1) {
                return true;
            } else {
                throw new IllegalArgumentException("Invalid Integer");
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!scannerIterator.hasNext())
                return false;
            Entry<Key, Value> entry = scannerIterator.next();
            ++numKeysRead;
            currentKey = entry.getKey();
            try {
                currentK = currentKey.getRow();
                byte[] row = entry.getKey().getRow().getBytes();
                int firstIndex = Bytes.indexOf(row, DELIM_BYTE);
                int secondIndex = Bytes.lastIndexOf(row, DELIM_BYTE);
                int typeIndex = Bytes.indexOf(row, TYPE_DELIM_BYTE);
                byte[] first = Arrays.copyOf(row, firstIndex);
                byte[] second = Arrays.copyOfRange(row, firstIndex + 1, secondIndex);
                byte[] third = Arrays.copyOfRange(row, secondIndex + 1, typeIndex);
                byte[] type = Arrays.copyOfRange(row, typeIndex, row.length);
                String context = currentKey.getColumnFamily().toString();
                List<byte[]> byteList = getByteListFromLayout(first, second, third, type);

                StringRowWritable writable = new StringRowWritable(getRow(byteList, context));
                currentV = writable;
            } catch (RyaTypeResolverException e) {
                throw new IOException(e);
            }
            return true;
        }

        private List<byte[]> getByteListFromLayout(byte[] first, byte[] second, byte[] third, byte[] type) {
            List<byte[]> byteList = null;
            switch (tableLayout) {
            case SPO:
                byteList = Arrays.asList(first, second, Bytes.concat(third, type));
                break;
            case PO:
                byteList = Arrays.asList(third, first, Bytes.concat(second, type));
                break;
            case OSP:
                byteList = Arrays.asList(second, third, Bytes.concat(first, type));
                break;
            }
            return byteList;
        }

        private Row getRow(List<byte[]> byteList, String context) throws RyaTypeResolverException {

            Object[] entries = new String[rowPosToInclude.size()];
            for (int i = 0; i < rowPosToInclude.size(); i++) {
                int j = rowPosToInclude.get(i);
                Preconditions.checkArgument(j < 4 && j > -1);
                if (j == 2) {
                    RyaType type = RyaContext.getInstance().deserialize(byteList.get(j));
                    String data = type.getData();
                    String dataType = type.getDataType().toString();
                    entries[i] = data + "^^" + dataType;
                } else if (j == 3) {
                    if (context != null && !context.isEmpty()) {
                        entries[rowPosToInclude.size() - 1] = context;
                    } else {
                        entries[rowPosToInclude.size() - 1] = "NA";
                    }
                } else {
                    entries[i] = new String(byteList.get(j)) + "^^" + XMLSchema.ANYURI.toString();
                }
            }

            return RowFactory.create(entries);
        }
    }

}
