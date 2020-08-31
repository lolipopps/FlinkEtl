package com.flink.feature;

import com.flink.util.DateUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

import static com.flink.util.DateUtil.YYYY_MM_DD_HH_MM_SS;

@Data
@Slf4j
public class TimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<BaseSource> {

    private long currentTimestamp = Long.MIN_VALUE;

    @Override
    public long extractTimestamp(BaseSource baseSource, long previousElementTimestamp) {
        long timestamp = baseSource.getEventTime();
        currentTimestamp = Math.max(timestamp, currentTimestamp);
//        log.info("event timestamp = {}, {}, CurrentWatermark = {}, {}", baseSource.getEventTime(),
//                DateUtil.format(baseSource.getEventTime(), YYYY_MM_DD_HH_MM_SS),
//                getCurrentWatermark().getTimestamp(),
//                DateUtil.format(getCurrentWatermark().getTimestamp(), YYYY_MM_DD_HH_MM_SS));
        return baseSource.getEventTime();
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 500000000;
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag);
    }


}