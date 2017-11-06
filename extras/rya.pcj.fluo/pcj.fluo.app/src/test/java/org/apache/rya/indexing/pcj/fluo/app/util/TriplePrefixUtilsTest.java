package org.apache.rya.indexing.pcj.fluo.app.util;

import java.util.Arrays;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.junit.Assert;
import org.junit.Test;

public class TriplePrefixUtilsTest {

    @Test
    public void testAddRemovePrefix() throws TripleRowResolverException {
        byte[] expected = Bytes.of("triple").toArray();
        Bytes fluoBytes = TriplePrefixUtils.addTriplePrefixAndConvertToBytes(expected);
        byte[] returned = TriplePrefixUtils.removeTriplePrefixAndConvertToByteArray(fluoBytes);
        Assert.assertEquals(true, Arrays.equals(expected, returned));
    }
}
