package com.flink.source;

import com.flink.bean.Personnel;
import com.flink.bean.Trade;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;

import java.util.ArrayList;
import java.util.List;

public class CollectionSource {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String filePath = "file:///intsmaze/to/textfile";
        TextInputFormat format = new TextInputFormat(new Path(filePath));
        TypeInformation<String> typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
        DataSet<String> textInputFormat = env.readFile(format, filePath);

//        DataSet<String> localLines = env.readTextFile("file:///intsmaze/to/textfile");
//
//        DataSet<String> hdfsLines = env.readTextFile("hdfs://name-node-Host:Port/intsmaze/to/textfile");
//
//        DataSet<Tuple2<String, Double>> csvInput = env.readCsvFile("hdfs:///intsmaze/CSV/file")
//                .includeFields("10010")
//                .includeFields(true, false, false, true, false)
//                .types(String.class, Double.class);

        DataSet<Personnel> csvInput = env.readCsvFile("/Users/yth/code/java/FlinkEtl/src/main/resources/aduit.csv")
                .pojoType(Personnel.class, "name", "age", "city");

        DataSet<String> value = env.fromElements("flink", "strom", "spark", "stream");

        List<String> list = new ArrayList<>();
        list.add("HashMap");
        list.add("List");

        csvInput.print();

        env.fromCollection(list).print();

        env.fromElements("Flink", "intsmaze").print();

        env.generateSequence(100, 101).print();
    }
}
