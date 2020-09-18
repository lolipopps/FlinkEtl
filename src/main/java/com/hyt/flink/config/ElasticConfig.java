package com.hyt.flink.config;


import com.hyt.flink.sink.es.RetryRequestFailureHandler;
import org.apache.calcite.avatica.org.apache.http.message.BasicHeader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClientBuilder;
import org.apache.http.Header;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static com.hyt.flink.config.PropertiesConstants.*;
import static com.hyt.flink.util.ExecutionEnvUtil.PARAMETER_TOOL;


/**
 * name：Huyt
 */
public class ElasticConfig {
    static ParameterTool parameter = PARAMETER_TOOL;

    public static <T> ElasticsearchSink<T> buildSink(ElasticsearchSinkFunction func) throws MalformedURLException {

        List<HttpHost> esAddresses = getEsAddresses(parameter.get(ELASTICSEARCH_HOSTS));
        int bulkSize = parameter.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(esAddresses, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkSize);
        esSinkBuilder.setRestClientFactory(new RestClientParamFactoryImpl());
        esSinkBuilder.setFailureHandler(new RetryRequestFailureHandler());
        return esSinkBuilder.build();
    }

    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

    static class RestClientParamFactoryImpl implements RestClientFactory {
        @Override
        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            String user = parameter.get(ES_SECURITY_USERNAME);
            String password = parameter.get(ES_SECURITY_PASSWORD);
            byte[] tokenByte = (user + ":" + password).getBytes();
            String tokenStr = "";
            try {
                tokenStr = new String(tokenByte, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            BasicHeader authHead = new BasicHeader("Authorization", "Basic " + tokenStr);
            BasicHeader typeHead = new BasicHeader("Content-Type", "application/json");
            Header[] headers = {(Header) authHead, (Header) typeHead};
            restClientBuilder.setDefaultHeaders(headers); //以数组的形式可以添加多个header
            restClientBuilder.setMaxRetryTimeoutMillis(90000);
        }
    }
}
