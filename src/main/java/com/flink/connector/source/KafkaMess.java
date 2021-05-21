package com.flink.connector.source;

import java.io.Serializable;


public class KafkaMess implements Serializable {

private String content;

private long time;

public String getContent() {
   return content;
}

public void setContent(String content) {
   this.content = content;
}

public long getTime() {
   return time;
}

public void setTime(long time) {
   this.time = time;
}
}
