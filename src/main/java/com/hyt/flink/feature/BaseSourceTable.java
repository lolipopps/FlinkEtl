
package com.hyt.flink.feature;

import com.hyt.flink.config.KafkaConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.List;


public class BaseSourceTable implements StreamTableSource<BaseSource> , DefinedRowtimeAttributes {


    @Override
    public DataType getProducedDataType() {
        return DataTypes.STRING();  //不能少，否则报错
    }

    @Override
    public DataStream<BaseSource> getDataStream(StreamExecutionEnvironment env) {
        DataStreamSource<String> stream = KafkaConfig.buildSource(env);
        DataStream<BaseSource> tempData = stream.map(new FlinkFeatureService());
        return tempData;
    }


    @Override
    public TableSchema getTableSchema() {
//       String[] names = new String[]{"logType","content","eventTime"};
//        DataType[] types = new DataType[]{DataTypes.STRING(), DataTypes.STRING(),DataTypes.TIMESTAMP()};
        return TableSchema.builder()
                .field("logType",DataTypes.STRING())

                .field("eventTime",DataTypes.TIMESTAMP()).build();
    }

    @Override
    public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
        RowtimeAttributeDescriptor rowTime = new RowtimeAttributeDescriptor("eventTime", new ExistingField(("eventTime")), new AscendingTimestamps());
        ArrayList<RowtimeAttributeDescriptor> res = new ArrayList<RowtimeAttributeDescriptor>();
          res.add(rowTime);
        return res;
    }
}
