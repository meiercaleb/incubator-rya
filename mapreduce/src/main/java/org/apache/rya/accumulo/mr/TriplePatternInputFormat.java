package org.apache.rya.accumulo.mr;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapred.RangeInputSplit;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.HadoopCompatUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.query.strategy.TriplePatternStrategy;
import org.apache.rya.api.resolver.RyaTripleContext;

public class TriplePatternInputFormat extends RyaInputFormat {
    
    
    public static void setTriplePattern(Job job, RyaStatement statement, String tablePrefix) {
        RyaTripleContext context = new RyaTripleContext(false);
        TriplePatternStrategy strategy = context.retrieveStrategy(statement);
        try {
            Entry<TABLE_LAYOUT, ByteRange> rangeEntry = strategy.defineRange(statement.getSubject(),
                    statement.getPredicate(), statement.getObject(), statement.getContext(), new AccumuloRdfConfiguration());
            ByteRange byteRange = rangeEntry.getValue();
            Range range = new Range(new Text(byteRange.getStart()), new Text(byteRange.getEnd()));
            setRange(job.getConfiguration(), range);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        TABLE_LAYOUT layout = strategy.getLayout();
        setTableLayout(job, layout);
        TablePrefixLayoutStrategy prefixStrategy = new TablePrefixLayoutStrategy(tablePrefix);
        setInputTableName(job, prefixStrategy.getTableName(layout));
    }
    
    private static void setInputTableName(Job job, String table) {
        AccumuloInputFormat.setInputTableName(job, table);
    }
    
    @Override
    public RecordReader<Text, RyaStatementWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RangeBasedRyaStatementReader();
    }

    private static void setRange(Configuration conf, Range range) {
        InputConfigurator.setRanges(CLASS, conf, Collections.singleton(range));
    }

    protected static class RangeBasedRyaStatementReader extends RyaStatementRecordReader {

        protected void setupIterators(TaskAttemptContext context, Scanner scanner, String tableName,
                RangeInputSplit split) {
            List<IteratorSetting> iterators = null;

            if (null == split) {
                iterators = getIterators(context);
            } else {
                iterators = split.getIterators();
                if (null == iterators) {
                    iterators = getIterators(context);
                }
            }

            setupIterators(iterators, scanner);
        }
        
        protected void setupIterators(List<IteratorSetting> iterators, Scanner scanner) {
            for (IteratorSetting iterator : iterators) {
                scanner.addScanIterator(iterator);
            }
        }

        protected static List<IteratorSetting> getIterators(JobContext context) {
            return InputConfigurator.getIterators(CLASS, getConfiguration(context));
        }

        static Configuration getConfiguration(JobContext context) {
            return HadoopCompatUtil.getConfiguration(context);
        }
    }

}
