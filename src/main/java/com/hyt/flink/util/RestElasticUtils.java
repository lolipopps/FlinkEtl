package com.hyt.flink.util;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.hyt.flink.config.PropertiesConstants;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.*;

/**
 * @Author: hyt
 * @License: (C) Copyright 2020-2020, xxx Corporation Limited.
 * @Contact: xxx@xxx.com
 * @Version: 1.0
 * @Description: 往kafka中写数据, 可以使用这个main函数进行测试
 */
@Slf4j
public class RestElasticUtils {


    ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool();

    private RestHighLevelClient client = null;

    public RestElasticUtils() {
        if (client == null) {
            synchronized (RestHighLevelClient.class) {
                if (client == null) {
                    client = getClient();
                }
            }
        }
    }

    private RestHighLevelClient getClient() {
        String hostName = parameterTool.get(PropertiesConstants.ELASTIC_HOSTNAME, "172.18.1.20");
        int port = parameterTool.getInt(PropertiesConstants.ELASTIC_PORT, 9810);
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostName, port, "http")));

        return client;
    }

    public void closeClient() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /*------------------------------------------------ document Api start --------------------------------------------*/

    /**
     * 增，改数据
     *
     * @param indexName index名字
     * @param typeName  type名字
     * @param id        id
     * @param jsonStr   增加或修改的数据json字符串格式
     * @throws Exception
     */
    public void index(String indexName, String typeName, String id, String jsonStr) throws Exception {
        IndexRequest request = new IndexRequest(indexName, typeName, id);

        request.source(jsonStr, XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);//同步
        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String ID = indexResponse.getId();
        long version = indexResponse.getVersion();
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            //      System.out.println("index: " + index);
            //      System.out.println("type: " + type);
            //      System.out.println("id: " + ID);
            //      System.out.println("version: " + version);
            //      System.out.println("status: " + "CREATED");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            //      System.out.println("index: " + index);
            //      System.out.println("type: " + type);
            //      System.out.println("id: " + ID);
            //      System.out.println("version: " + version);
            //      System.out.println("status: " + "UPDATED");
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }
    }

    /**
     * 根据 id 获取数据
     *
     * @throws Exception
     */
    public void get(String indexName, String typeName, String id) throws Exception {
        GetRequest request = new GetRequest(indexName, typeName, id);
        GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);

        //Get Response
        String index = getResponse.getIndex();
        String type = getResponse.getType();
        String ID = getResponse.getId();
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            //      System.out.println("index: " + index);
            //      System.out.println("type: " + type);
            //      System.out.println("id: " + ID);
            //      System.out.println("version: " + version);
            //      System.out.println(sourceAsString);
        } else {
            //      System.out.println("没有查询到结果");
        }
    }

    /**
     * 存在
     *
     * @throws Exception
     */
    public boolean exists(String indexName, String typeName, String id) throws Exception {
        GetRequest getRequest = new GetRequest(indexName, typeName, id);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        //Synchronous Execution
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        return exists;
    }

    public void delete(String indexName, String typeName, String id) throws Exception {
        DeleteRequest request = new DeleteRequest(indexName, typeName, id);

        //Synchronous Execution
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        // document was not found
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            //      System.out.println("要删除的数据不存在");
        } else {
            //Delete Response
            String index = deleteResponse.getIndex();
            String type = deleteResponse.getType();
            String ID = deleteResponse.getId();
            long version = deleteResponse.getVersion();
            //      System.out.println("index: " + index);
            //      System.out.println("type: " + type);
            //      System.out.println("id: " + ID);
            //      System.out.println("version: " + version);
            //      System.out.println("status: " + "DELETE");
            ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                }
            }
        }
    }

    public void update(String indexName, String typeName, String id, String jsonStr) throws Exception {
        UpdateRequest request = new UpdateRequest(indexName, typeName, id);

        request.doc(jsonStr, XContentType.JSON);
        try {
            //Synchronous Execution
            UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);
            //update Response
            String index = updateResponse.getIndex();
            String type = updateResponse.getType();
            String ID = updateResponse.getId();
            long version = updateResponse.getVersion();
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                //      System.out.println("index: " + index);
                //      System.out.println("type: " + type);
                //      System.out.println("id: " + ID);
                //      System.out.println("version: " + version);
                //      System.out.println("status: " + "CREATED");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                //      System.out.println("index: " + index);
                //      System.out.println("type: " + type);
                //      System.out.println("id: " + ID);
                //      System.out.println("version: " + version);
                //      System.out.println("status: " + "UPDATED");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
                //      System.out.println("index: " + index);
                //      System.out.println("type: " + type);
                //      System.out.println("id: " + ID);
                //      System.out.println("version: " + version);
                //      System.out.println("status: " + "DELETED");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
                //      System.out.println("index: " + index);
                //      System.out.println("type: " + type);
                //      System.out.println("id: " + ID);
                //      System.out.println("version: " + version);
                //      System.out.println("status: " + "NOOP");
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                //      System.out.println("要修改的内容不存在");
            }
        }
    }

    /**
     * 根据id批量获取数据
     *
     * @throws Exception
     */
    public void multiGet(String indexName, String typeName, String... ids) throws Exception {
        MultiGetRequest request = new MultiGetRequest();

        for (String str : ids) {
            request.add(new MultiGetRequest.Item(indexName, typeName, str));
        }

        //Synchronous Execution
        MultiGetResponse responses = client.mget(request, RequestOptions.DEFAULT);
        //Multi Get Response
        MultiGetItemResponse[] Items = responses.getResponses();
        for (MultiGetItemResponse Item : Items) {
            GetResponse response = Item.getResponse();
            String index = response.getIndex();
            String type = response.getType();
            String id = response.getId();
            if (response.isExists()) {
                long version = response.getVersion();
                String sourceAsString = response.getSourceAsString();
                //      System.out.println("index: " + index);
                //      System.out.println("type: " + type);
                //      System.out.println("id: " + id);
                //      System.out.println("version: " + version);
                //      System.out.println(sourceAsString + "\n");
            } else {
                //      System.out.println("不存在");
            }
        }
    }

    /**
     * 单次请求批量处理数据,可以同时操作增删改
     * 注意：bulk方法操作增删改时只能用json格式，其他类似map方式会报错
     *
     * @throws Exception
     */
    public void bulk() throws Exception {
        BulkRequest request = new BulkRequest();
        String jsonString = "{\n" +
                "  \"name\":\"dior chengyi\",\n" +
                "  \"desc\":\"shishang gaodang\",\n" +
                "  \"price\":7000,\n" +
                "  \"producer\":\"dior producer\",\n" +
                "  \"tags\":[\"shishang\",\"shechi\"]\n" +
                "}";
        request.add(new IndexRequest("lyh_index", "user", "1")
                .source(jsonString, XContentType.JSON));
        String updateJson = "{\n" +
                "  \"other\":\"test1\"\n" +
                "}";
        request.add(new UpdateRequest("lyh_index", "user", "2")
                .doc(updateJson, XContentType.JSON));
        request.add(new DeleteRequest("lyh_index", "user", "3"));

        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                    || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                String index = indexResponse.getIndex();
                String type = indexResponse.getType();
                String id = indexResponse.getId();
                long version = indexResponse.getVersion();
                //      System.out.println("index: " + index);
                //      System.out.println("type: " + type);
                //      System.out.println("id: " + id);
                //      System.out.println("version: " + version);
                //      System.out.println("status: " + "CREATED" + "\n");
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                String index = updateResponse.getIndex();
                String type = updateResponse.getType();
                String id = updateResponse.getId();
                long version = updateResponse.getVersion();
                //      System.out.println("index: " + index);
                //      System.out.println("type: " + type);
                //      System.out.println("id: " + id);
                //      System.out.println("version: " + version);
                //      System.out.println("status: " + "UPDATE" + "\n");
            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                String index = deleteResponse.getIndex();
                String type = deleteResponse.getType();
                String id = deleteResponse.getId();
                long version = deleteResponse.getVersion();
            }
        }
    }

    public List<String> selectAll(String sourceIndex) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
            searchSourceBuilder.query(queryBuilder);
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest);
            SearchHit[] hits = response.getHits().getHits();
            List<String> res = new ArrayList<>();
            for (SearchHit user : hits) {
                String user1 = user.getSourceAsString();
                res.add(user1);
            }
            return res;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String selectOne(String sourceIndex) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(1);
            QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
            searchSourceBuilder.query(queryBuilder);
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(searchSourceBuilder);
            SearchResponse response = client.search(searchRequest);
            SearchHit[] hits = response.getHits().getHits();
            String res = "";
            for (SearchHit user : hits) {
                res = user.getSourceAsString();
                break;
            }
            return res;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public ArrayList<String> getAllIndex() {
        ArrayList<String> res = new ArrayList<>();
        try {
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesResponse getAliasesResponse = client.indices().getAlias(request, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> map = getAliasesResponse.getAliases();
            Set<String> indices = map.keySet();
            for (String key : indices) {
                res.add(key);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameterTool, client);
    }

    public ArrayList<String> getIndexData(String sourceIndex) {
        ArrayList<String> res = new ArrayList<>();
        try {
            SearchRequest searchRequest = new SearchRequest(sourceIndex);
            //请求设置
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.fetchSource(true);
            searchSourceBuilder.size(500);
            searchRequest.scroll(TimeValue.timeValueMinutes(5L));
            searchRequest.source(searchSourceBuilder);
//            请求发送
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            SearchHit[] searchHits = searchResponse.getHits().getHits();
            for (SearchHit searchHit : searchHits) {
                res.add(searchHit.getSourceAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    public HashMap<String, String> getIndexSql(String sourceIndex) throws IOException {
        HashMap<String, String> resss = new HashMap<>();
        GetMappingsRequest request = new GetMappingsRequest();
        GetMappingsRequest res = request.indices(sourceIndex);
        GetMappingsResponse getMappingResponse = client.indices().getMapping(res, RequestOptions.DEFAULT);
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> maps = getMappingResponse.mappings();

        for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> map : maps) {
            String key = map.key;
            ObjectContainer<MappingMetaData> values = map.value.values();
            for (ObjectCursor<MappingMetaData> value : values) {
                CompressedXContent mapss = value.value.source();
                JsonParser parser = new JsonParser();
                // 2.获得 根节点元素
                JsonElement element = parser.parse(mapss.toString());
                // 3.根据 文档判断根节点属于 什么类型的 Gson节点对象
                JsonObject root = element.getAsJsonObject();
                JsonElement properties = root.get("properties");
                JsonObject node = properties.getAsJsonObject();
                Set<String> ele = node.keySet();
                for (String keyss : ele) {
                    JsonElement aa = node.get(keyss).getAsJsonObject().get("type");
                    String type = node.get(keyss).getAsJsonObject().get("type").toString().toLowerCase().replaceAll("\"", "");
                    switch (type) {
                        case "short":
                            type = "smallint";
                            break;
                        case "int":
                            type = "int";
                            break;
                        case "long":
                            type = "bigint";
                            break;
                        case "map":
                            type = "map";
                            break;
                        case "double":
                            type = "double";
                            break;
                        case "float":
                            type = "float";
                            break;
                        case "string":
                            type = "string";
                            break;
                        case "binary":
                            type = "binary";
                            break;
                        case "date":
                            type = "timestamp";
                            break;
                        case "keyword":
                            type = "string";
                            break;
                        default:
                            type = "string";

                    }
                    resss.put(keyss, type);
                }
            }

        }
        return resss;

    }

    public String genDmlHiveSql(HashMap<String, String> paras, String indexName) {
        String after = indexName.toLowerCase().replaceAll("-", "_");
        StringBuilder dmlSql = new StringBuilder("CREATE EXTERNAL TABLE stg." + after + "\n(");
        StringBuilder partDmlsql = new StringBuilder("CREATE EXTERNAL TABLE ods." + after + "\n(");
        for (Map.Entry<String, String> para : paras.entrySet()) {
            if (dmlSql.toString().equalsIgnoreCase("CREATE EXTERNAL TABLE stg." + after + "\n(")) {
                dmlSql.append(" " + para.getKey() + " " + para.getValue());
                partDmlsql.append(" " + para.getKey() + " " + para.getValue());
            } else {
                dmlSql.append(" ," + para.getKey() + " " + para.getValue() + "\n");
                partDmlsql.append(" ," + para.getKey() + " " + para.getValue() + "\n");
            }
        }

        dmlSql.append(")\n" +
                "STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'\n" +
                "TBLPROPERTIES(\n" +
                "'es.resource'='" + indexName + "*/doc',\n" +
                "'es.nodes'='kafka:9810'\n" +
                ");\n");
        partDmlsql.append(")" +
                "partitioned by(stat_hour string ) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY ',';");
        System.out.println(dmlSql.toString());
        System.out.println(partDmlsql.toString());
        return dmlSql.toString();

    }

    public String genOdsHiveSql(HashMap<String, String> paras, String indexName) {
        String after = indexName.toLowerCase().replaceAll("-", "_");
        StringBuilder dmlSql = new StringBuilder("INSERT OVERWRITE TABLE ods." + after + " partition(stat_hour='${hiveconf:stat_hour}')\n select");
        for (Map.Entry<String, String> para : paras.entrySet()) {
            if (dmlSql.toString().equalsIgnoreCase("INSERT OVERWRITE TABLE ods.\"+after +\" partition(stat_hour='${hiveconf:stat_hour}')\\n select")) {
                dmlSql.append(" " + para.getKey());
            } else {
                dmlSql.append(" ," + para.getKey() + "\n");
            }
        }

        dmlSql.append(")\n from stg." + after + " \nWHERE date_format(substr(regexp_replace(center_time,\"T\",\" \"),1,19),\"yyyyMMddHH\") = '${hiveconf:stat_hour}'; ");
        System.out.println(dmlSql.toString());
        return dmlSql.toString();

    }


    public String genFlinkSql(HashMap<String, String> paras, String indexName) {
        String after = indexName.toLowerCase().replaceAll("-", "_");

        StringBuilder sql = new StringBuilder("create view " + after + " as select ");
        for (Map.Entry<String, String> para : paras.entrySet()) {
            if (sql.toString().equalsIgnoreCase("create view " + after + " as select ")) {
                sql.append(" getJsonObject(content,'$." + para.getKey() + "') as " + para.getKey() + "\n");
            } else {
                sql.append(" ,getJsonObject(content,'$." + para.getKey() + "') as " + para.getKey() + "\n");
            }
        }
        sql.append("from all_table where log_type='process';");
        return sql.toString();

    }

    public static void main(String[] args) throws Exception {

        RestElasticUtils restElasticUtils = new RestElasticUtils();
        ArrayList<String> allIndex = restElasticUtils.getAllIndex();
        Collections.sort(allIndex);
        ArrayList<String> allIndexs = new ArrayList<>();
        allIndexs.add("system-safe-operation_auth-2020-06-17");
        allIndexs.add("system-safe-attack_ips-2020-06-17");
        allIndexs.add("system-safe-attack_cc-2020-06-03");
        allIndexs.add("system-safe-attack_waf-2020-06-22");
        allIndexs.add("audit-app-otp_user-2020-07-07");
        allIndexs.add("audit-linuxserver-abnormalprogress-2020-06-28");
        allIndexs.add("audit-linuxserver-address-2020-06-24");
        allIndexs.add("audit-linuxserver-file-2020-06-30");
        allIndexs.add("audit-linuxserver-network-2020-06");
        allIndexs.add("audit-linuxserver-process-2020-06-30");
        allIndexs.add("audit-linuxserver-property-2020-06-30");
        allIndexs.add("audit-linuxserver-soft-2020-07");
        allIndexs.add("audit-linuxserver-user-2020-06-30");


        for (String index : allIndexs) {
//            if (index.startsWith("audit-linuxserver-soft")) {
            String sql = restElasticUtils.genDmlHiveSql(restElasticUtils.getIndexSql(index), index);

            //   String sql = restElasticUtils.genFlinkSql(restElasticUtils.getIndexSql(index),index);
            //  System.out.println(sql);
            //    HashMap<String, String> tableCols = restElasticUtils.getIndexSql(index);
//            String col = "";
//            for (String table : tableCols.keySet()) {
//                if (col.equalsIgnoreCase("")) {
//                    col = table;
//                } else {
//                    col = col + table+",";
//                }
//            }
//            System.out.println(index+"="+col.substring(0,col.length()-1));
//            }
        }
    }
}
