package com.flink.cache;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class DistributedCacheTemplate {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String cacheUrl = "./src/main/resources/aduit.csv";
//        env.registerCachedFile("file:///home/intsmaze/flink/cache/local.txt", "localFile");
        env.registerCachedFile(cacheUrl, "localFile");

        DataStream<Long> input = env.generateSequence(1, 20);

        input.map(new MyMapper()).setParallelism(5).print();
        env.execute();
    }

    public static class MyMapper extends RichMapFunction<Long, String> {

        private String cacheStr;

        @Override
        public void open(Configuration config) {
            RuntimeContext runtimeContext = getRuntimeContext();
            DistributedCache distributedCache = runtimeContext.getDistributedCache();
            File myFile = distributedCache.getFile("localFile");
            cacheStr = readFile(myFile);
        }


        @Override
        public String map(Long value) throws Exception {
            Thread.sleep(600);
            return StringUtils.join(value, "---", cacheStr);
        }


        public String readFile(File myFile) {
            BufferedReader reader = null;
            StringBuffer sbf = new StringBuffer();
            try {
                reader = new BufferedReader(new FileReader(myFile));
                String tempStr;
                while ((tempStr = reader.readLine()) != null) {
                    sbf.append(tempStr);
                }
                reader.close();
                return sbf.toString();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
            return sbf.toString();
        }
    }

}
