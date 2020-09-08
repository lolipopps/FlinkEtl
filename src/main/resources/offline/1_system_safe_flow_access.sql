Timestamp,   //日志产生的时间	本次http请求结束的时间
remote_addr,  //请求客户端ip地址	访客的出口IP
server_addr, //处理该请求的cdn防护节点IP
body_bytes_sent, //发送给客户端的字节数，不包括响应头
request_time, //节点处理请求的总时间	从节点接收到请求开始计算, 到节点处理请求结束
server_identity, //网站主域名
http_host, //客户端访问的域名
method, //访客的请求方法	GET、POST、HEAD、PUT等
status, //响应状态码	请求的最终结果
upstream_status,//源站响应的状态码	502状态码即有可能是源站主动返回502, 也有可能是没有接收到源的响应
request_url, //访客请求的完整URL
http_user_agent";//http请求的User-Agent字段
