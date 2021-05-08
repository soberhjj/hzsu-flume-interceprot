package com.newland.hzsy.flume.interceptor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.newland.hzsy.flume.common.Constant.*;

/**
 * @Author: huangJunJie  2020-12-25 11:27
 */
public class ReportDataParseInterceptor implements Interceptor {
    Logger logger;
    SimpleDateFormat format;
    List<Event> events;

    @Override
    public void initialize() {
        format = new SimpleDateFormat(DATE_FORMATE);
        logger = LoggerFactory.getLogger(ReportDataParseInterceptor.class);
        events = new ArrayList<>();

    }

    @Override
    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        events.clear();
        for (Event event : list) {
            String eventData = null;
            try {
                eventData = new String(event.getBody(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                logger.error("转码失败 ", e);
            }
            List<Object> res = Splice(eventData);
            List<StringBuilder> spliceStirngs = (List<StringBuilder>) res.get(0);
            String dataString = (String) res.get(1);
            String reportType = (String) res.get(2);

            for (StringBuilder sb : spliceStirngs) {
                events.add(new Event() {
                    @Override
                    public Map<String, String> getHeaders() {
                        Map<String, String> map = new HashMap<>();
                        map.put("hdfspath", dataString);
                        map.put("reportType", reportType);
                        return map;
                    }

                    @Override
                    public void setHeaders(Map<String, String> map) {

                    }

                    @Override
                    public byte[] getBody() {
                        return sb.toString().getBytes();
                    }

                    @Override
                    public void setBody(byte[] bytes) {

                    }
                });
            }
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public void configure(Context arg0) {
        }

        @Override
        public Interceptor build() {
            return new ReportDataParseInterceptor();
        }
    }

    /**
     * 返回的是List<Object>，该List中包含两个元素。
     * 第一个元素是List<StringBuilder>，该List中的元素就是拼接好的字符串。
     * 第二个元素是根据时间戳转换的"yyyy-MM-dd"格式的日期字符串，用于按日期对消息分目录存储。
     * 第三个元素是上报数据类型，用于后续可能需要根据上报数据类型对消息分目录存储时该值将作为channel选择器的判断值
     */
    private List<Object> Splice(String jsonStr) {
        List<Object> res = new LinkedList<>();
        JSONObject jsonObject = null;
        JSONArray reportData = null;
        try {
            jsonObject = JSONObject.parseObject(jsonStr);
            reportData = jsonObject.getJSONArray("reportData");
        } catch (Exception e) {
            logger.error("解析失败 ", e);
        }
        //生成"yyyy-MM-dd"格式的日期字符串
        String dataString = format.format(new Date(jsonObject.getLong("receiveTime")));

        List<StringBuilder> list = new LinkedList<>();
        for (int i = 0; i < reportData.size(); i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(jsonObject.getString("appId")).append(SeparatorA)
                    .append(jsonObject.getString("rtt")).append(SeparatorA)
                    .append(jsonObject.getString("signMethod")).append(SeparatorA)
                    .append(jsonObject.getString("sign")).append(SeparatorA)
                    .append(jsonObject.getString("reportType")).append(SeparatorA)
                    .append(jsonObject.getString("receiveTime")).append(SeparatorA)
                    .append(jsonObject.getString("requestId")).append(SeparatorA);

            //添加reportData字段
            JSONObject jsonReportData = JSONObject.parseObject(reportData.getString(i));
            Iterator<String> iterator = jsonReportData.keySet().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                sb.append(key).append(SeparatorC).append(jsonReportData.getString(key)).append(SeparatorB);
            }
            //删除sb的最后一个字符（上面循环结束后末尾的SeparatorB分隔符要删掉）
            sb.deleteCharAt(sb.length() - 1);

            //加入dataString，用于数据导入hive时自动分区
            sb.append(SeparatorA).append(dataString);
            list.add(sb);
        }
        res.add(list);
        res.add(dataString);
        res.add(jsonObject.getString("reportType"));
        return res;
    }
}
