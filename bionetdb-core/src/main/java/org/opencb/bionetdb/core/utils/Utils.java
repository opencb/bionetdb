package org.opencb.bionetdb.core.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.opencb.commons.utils.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static final String PREFIX_ATTRIBUTES = "attr_";
    public static final int PREFIX_ATTRIBUTES_LENGTH = PREFIX_ATTRIBUTES.length();

    public static String compress(Object obj, ObjectMapper objMapper) throws IOException {
        String json = objMapper.writer().writeValueAsString(obj);
        return compressString(json, objMapper);
    }

    public static <T> T uncompress(String input, Class<T> valueType, ObjectMapper objMapper) throws IOException {
        String json = uncompressString(input, objMapper);
        return objMapper.readValue(json, valueType);
    }

    public static <T> List<T> uncompressList(String input, Class<T> valueType, ObjectMapper objMapper) throws IOException {
        String json = uncompressString(input, objMapper);
        CollectionType listType = objMapper.getTypeFactory().constructCollectionType(ArrayList.class, valueType);
        return objMapper.readValue(json, listType);
    }

    public static String compressString(String input, ObjectMapper objMapper) throws IOException {
        byte[] compressed = StringUtils.gzip(input);
        String output = objMapper.writer().writeValueAsString(compressed);
        return output;
    }

    public static String uncompressString(String input, ObjectMapper objMapper) throws IOException {
        byte[] bytes = objMapper.readValue(input, byte[].class);
        String output = StringUtils.gunzip(bytes);
        return output;
    }
}
