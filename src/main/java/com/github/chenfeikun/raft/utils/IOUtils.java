package com.github.chenfeikun.raft.utils;

import java.io.*;
import java.nio.charset.Charset;
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

}
