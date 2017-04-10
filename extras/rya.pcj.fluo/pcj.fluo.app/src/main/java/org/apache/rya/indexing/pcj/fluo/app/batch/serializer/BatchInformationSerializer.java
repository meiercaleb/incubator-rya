package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;

import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class BatchInformationSerializer {

    private static Logger log = Logger.getLogger(BatchInformationSerializer.class);
    private static Gson gson = new GsonBuilder().registerTypeHierarchyAdapter(BatchInformation.class, new BatchInformationTypeAdapter())
            .create();

    public static byte[] toBytes(BatchInformation arg0) {
        try {
            return gson.toJson(arg0).getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.info("Unable to serialize BatchInformation: " + arg0);
            throw new RuntimeException(e);
        }
    }

    public static BatchInformation fromBytes(byte[] arg0) {
        try {
            String json = new String(arg0, "UTF-8");
            return gson.fromJson(json, BatchInformation.class);
        } catch (UnsupportedEncodingException e) {
            log.info("Invalid String encoding.");
            throw new RuntimeException(e);
        }
    }
}
