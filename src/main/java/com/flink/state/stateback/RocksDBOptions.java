package com.flink.state.stateback;


import org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;




public class RocksDBOptions extends RocksDBStateBackendFactory {



    public DBOptions createDBOptions(DBOptions currentOptions) {
        return currentOptions.setIncreaseParallelism(5)
                .setUseFsync(false);
    }

  

    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
        return currentOptions.setTableFormatConfig(
                new BlockBasedTableConfig()
                        .setBlockCacheSize(256 * 1024 * 1024)
                        .setBlockSize(128 * 1024));
    }


}
