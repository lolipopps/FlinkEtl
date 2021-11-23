package com.flink.connector;
import com.flink.bean.Person;
import com.flink.sql.PrepareData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Avro;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.List;


public class KafkaConnector {



    @Test
    public void testTableSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        StreamTableDescriptor connect = tableEnv.connect(new Kafka()
                .version("universal")
                .topic("flink-intsmaze")
                .property("zookeeper.connect", "192.168.19.201:2181")
                .property("bootstrap.servers", "192.168.19.201:9092")
        );
//        connect = connect.withFormat(new Avro()
//                .avroSchema(
//                        "{" +
//                                "  \"type\": \"record\"," +
//                                "  \"name\": \"test\"," +
//                                "  \"fields\" : [" +
//                                "    {\"name\": \"name\", \"type\": \"string\"}," +
//                                "    {\"name\": \"city\", \"type\": \"string\"}" +
//                                "  ]" +
//                                "}"
//                )
//        );

        connect = connect.withSchema(new Schema()
                .field("name", "VARCHAR")
                .field("city", "VARCHAR")
        );

        connect.inAppendMode();

        connect.createTemporaryTable("CsvTable");


        List<Person> clicksData = PrepareData.getPersonData();
        DataStream<Person> dataStream = env.fromCollection(clicksData);

        tableEnv.registerDataStream("Person", dataStream, "name,age,city");
        tableEnv.sqlUpdate("INSERT INTO CsvTable SELECT name,city FROM Person WHERE age <20 ");

        env.execute();
    }



}