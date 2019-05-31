package com.jp.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 拦截器
 * 将数据转化成json格式
 * 先调用Builder，由Builder创建JsonInterceptor
 */
public class JsonInterceptor implements Interceptor {

    private String[] schema; //id,name,age,fv  ，字段分隔符默认使用,
    private String separator; //数据的分隔符,不是字段分割符  ,

    public JsonInterceptor(String schema,String separator){
        this.schema = schema.split("[,]");
        this.separator = separator;
    }

    @Override
    public void initialize() {
        //no-op
    }

    @Override
    public Event intercept(Event event) {
        Map<String,String> tuple = new LinkedHashMap<String,String>();
        String line = new String(event.getBody());
        System.out.println("Interceptor processing data:" + line);
        String[] fields = line.split(separator);
        for(int i=0;i<schema.length;i++){
            String key = schema[i];
            String value = fields[i];
            tuple.put(key,value);
        }
        String json = JSONObject.toJSONString(tuple);
        //将转换好的josn，再放到Event中
        event.setBody(json.getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for(Event e : events){
            intercept(e);
        }
        return events;
    }

    @Override
    public void close() {
        //no-op
    }

    /**
     * Interceptor.Builder的生命周期
     * 构造器->configure->build
     */
    public static class Builder implements Interceptor.Builder{
        private String fields;
        private String separator;

        @Override
        public Interceptor build() {
            return new JsonInterceptor(fields,separator);
        }

        /**
         * 配置文件中应该有哪些属性
         * 1.数据的分隔符
         * 2.字段名称
         * 3.schema字段的分隔符 默认用逗号
         * @param context
         */
        @Override
        public void configure(Context context) {
            fields = context.getString("fields");
            separator = context.getString("separator");
        }
    }
}
