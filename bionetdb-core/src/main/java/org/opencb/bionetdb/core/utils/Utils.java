package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opencb.commons.utils.StringUtils;

import java.io.IOException;

public class Utils {

    public static String compress(Object obj, ObjectMapper objMapper) throws IOException {
        String json = objMapper.writer().writeValueAsString(obj);
        return compressString(json, objMapper);
    }

    public static <T> T uncompress(String input, Class<T> valueType, ObjectMapper objMapper) throws IOException {
        String json = uncompressString(input, objMapper);
        return objMapper.readValue(json, valueType);
    }

    public static String compressString(String input, ObjectMapper objMapper) throws IOException {
        byte[] compressed = org.opencb.commons.utils.StringUtils.gzip(input);
        String output = objMapper.writer().writeValueAsString(compressed);
        return output;
    }

    public static String uncompressString(String input, ObjectMapper objMapper) throws IOException {
        byte[] bytes = objMapper.readValue(input, byte[].class);
        String output = StringUtils.gunzip(bytes);
        return output;
    }
}
