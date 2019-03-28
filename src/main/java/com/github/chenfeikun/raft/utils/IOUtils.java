package com.github.chenfeikun.raft.utils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;

/**
 * @desciption: IOUtils
 * @CreateTime: 2019-03-18
 * @author: chenfeikun
 */
public class IOUtils {

    public static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

    public static String file2String(String fileName) throws IOException {
        File file = new File(fileName);
        return file2String(file);
    }

    public static String file2String(File file) throws IOException {
        if (file.exists()) {
            byte[] data = new byte[(int)file.length()];
            boolean result;
            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(file);
                int len = fileInputStream.read(data);
                result = len == data.length;
            } finally {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            }
            if (result) {
                return new String(data);
            }
        }
        return null;
    }

    public static void string2File(final String data, final String fileName) throws Exception {
        String tmpFile = fileName + ".tmp";
        string2FileNotSafe(data, tmpFile);
        String bakFile = fileName + ".bak";
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            string2FileNotSafe(prevContent, bakFile);
        }

        File file = new File(fileName);
        file.delete();
        file = new File(tmpFile);
        file.renameTo(new File(fileName));
    }

    public static void string2FileNotSafe(String data, String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(file);
            fileWriter.write(data);
        } catch (IOException e) {
            throw e;
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    public static Properties string2Properties(String data) {
        Properties properties = new Properties();
        try {
            InputStream in = new ByteArrayInputStream(data.getBytes(DEFAULT_CHARSET));
            properties.load(in);
        } catch (Exception e) {
            return null;
        }
        return properties;
    }

    public static String properties2String(Properties properties) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getValue() != null) {
                sb.append(entry.getKey().toString() + "=" + entry.getValue() + "\n");
            }
        }
        return sb.toString();
    }


}
