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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class StringRowWritable implements Writable {

    private Row row;
    
    public StringRowWritable() {};
    
    public StringRowWritable(Row row) {
        this.row = row;
    }
    
    public Row getRow() {
        return row;
    }
    
    public void setRow(Row row) {
        this.row = row;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        IntWritable size = new IntWritable();
        size.readFields(in);
        int n = size.get();
        Object[] entries = new String[n];
        for(int i = 0; i < n; i++) {
            Text text = new Text();
            text.readFields(in);
            entries[i] = text.toString();
        }
        
        setRow(RowFactory.create(entries));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        IntWritable size = new IntWritable(row.size());
        size.write(out);
        for(int i = 0; i < row.size(); i++) {
            Text text = new Text(row.getString(i));
            text.write(out);
        }
    }
    

}
