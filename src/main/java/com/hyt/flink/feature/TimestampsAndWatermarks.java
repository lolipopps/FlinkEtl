package com.hyt.flink.feature;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

@Data
@Slf4j
public class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<BaseSource> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(BaseSource baseSource, long previousElementTimestamp) {
        long timestamp = baseSource.getRowTime();
        currentTimestamp = Math.max(timestamp, currentTimestamp);
//        log.info("event timestamp = {}, {}, CurrentWatermark = {}, {}", baseSource.getEventTime(),
//                DateUtil.format(baseSource.getEventTime(), YYYY_MM_DD_HH_MM_SS),
//                getCurrentWatermark().getTimestamp(),
//                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return baseSource.getRowTime();
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 5000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }


}