package org.apache.rya.indexing.pcj.fluo.integration;

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.recipes.accumulo.ops.TableOperations;
import org.apache.fluo.recipes.core.common.RowRange;
import org.apache.fluo.recipes.core.common.TransientRegistry;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Assert;
import org.junit.Test;

public class TransientIT extends RyaExportITBase {

    @Override
    protected void preFluoInitHook() throws Exception {
        TransientRegistry transientRegistry = new TransientRegistry(fluoConfig.getAppConfiguration());

        BytesBuilder builder = Bytes.builder();
        builder.append(Bytes.of("T")).append(Bytes.of(NODEID_BS_DELIM)).append((byte) 0xff);

        transientRegistry.addTransientRange("triples",
                new RowRange(Bytes.of("T" + IncrementalUpdateConstants.NODEID_BS_DELIM), builder.toBytes()));



        builder = Bytes.builder();
        builder.append(Bytes.of("SP")).append(Bytes.of(NODEID_BS_DELIM)).append((byte) 0xff);

        transientRegistry.addTransientRange("statementPattern",
                new RowRange(Bytes.of("SP" + IncrementalUpdateConstants.NODEID_BS_DELIM), builder.toBytes()));



        builder = Bytes.builder();
        builder.append(Bytes.of("J")).append(Bytes.of(NODEID_BS_DELIM)).append((byte) 0xff);

        transientRegistry.addTransientRange("join",
                new RowRange(Bytes.of("J" + IncrementalUpdateConstants.NODEID_BS_DELIM), builder.toBytes()));


        builder = Bytes.builder();
        builder.append(Bytes.of("A")).append(Bytes.of(NODEID_BS_DELIM)).append((byte) 0xff);

        transientRegistry.addTransientRange("aggregation",
                new RowRange(Bytes.of("A" + IncrementalUpdateConstants.NODEID_BS_DELIM), builder.toBytes()));


        builder = Bytes.builder();
        builder.append(Bytes.of("PR")).append(Bytes.of(NODEID_BS_DELIM)).append((byte) 0xff);

        transientRegistry.addTransientRange("projection",
                new RowRange(Bytes.of("PR" + IncrementalUpdateConstants.NODEID_BS_DELIM), builder.toBytes()));

        builder = Bytes.builder();
        builder.append(Bytes.of("PR")).append(Bytes.of(NODEID_BS_DELIM)).append((byte) 0xff);

        transientRegistry.addTransientRange("fullTable",
                new RowRange(Bytes.of(new byte[0]), Bytes.of(new byte[]{(byte) 0xff})));
    }

    @Test
    public void transientRegister() throws Exception {
        FluoConfiguration conf = fluoConfig;
        TableOperations.compactTransient(conf);

        BytesBuilder builder = Bytes.builder();
        builder.append(Bytes.of("T")).append(Bytes.of(NODEID_BS_DELIM)).append((byte) 0xff);
        Bytes b = builder.toBytes();
        Assert.assertTrue(b.compareTo(Bytes.of("T" + NODEID_BS_DELIM + "zzzz")) > 0);
        Assert.assertTrue(b.compareTo(Bytes.of("T" + NODEID_BS_DELIM)) > 0);
    }

}
