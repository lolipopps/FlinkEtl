package com.flink.udf.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


public class SplitTableTypeInfor extends TableFunction<Row> {

    private String separator;


    public SplitTableTypeInfor(String separator) {
        this.separator = separator;
    }


    public void eval(String str) {
        if (str.indexOf("flink") < 0) {
            for (String s : str.split(separator)) {
                Row row = new Row(2);
                row.setField(0, s);
                row.setField(1, s.length());
                collect(row);
            }
        }
    }


    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING(), Types.INT());
    }
}
