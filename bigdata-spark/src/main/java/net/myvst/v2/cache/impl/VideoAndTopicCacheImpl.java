package net.myvst.v2.cache.impl;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import net.myvst.v2.cache.IVideoAndTopicCache;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@Slf4j
public class VideoAndTopicCacheImpl implements IVideoAndTopicCache, Runnable {
    private Map<String, JSONObject> video = new HashMap<>();        //影片
    private Map<String, JSONObject> videoTopic = new HashMap<>();   //专题
    private String url;

    public VideoAndTopicCacheImpl(String url) {
        this.url = url;
        load();
        new Thread(this).start();
    }

    @Override
    public JSONObject getVideo(String k) {
        return video.get(k);
    }

    @Override
    public JSONObject getVideoTopic(String k) {
        return videoTopic.get(k);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(60 * 1000 * 10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            load();
        }
    }

    private void load() {
        try {
            log.info("flush movie to memory");
            String movieUrl = url + "?filename=movie.dat";
            video = readURLContent(movieUrl, "uuid");
            String topicUrl = url + "?filename=topic.dat";
            videoTopic = readURLContent(topicUrl, "topicId");
            log.info("flush movie completed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, JSONObject> readURLContent(String u, String key) throws IOException {
        GZIPInputStream gzipInput = null;
        HttpURLConnection conn = null;
        StringBuilder lines = new StringBuilder();
        try {
            if (StringUtils.isEmpty(u)) {
                return null;
            }

            URL requestUrl = new URL(u);
            conn = (HttpURLConnection) requestUrl.openConnection();

            gzipInput = new GZIPInputStream(conn.getInputStream());
            byte[] buffer = new byte[4 * 1024];
            int offset;
            while ((offset = gzipInput.read(buffer)) != -1) {
                lines.append(new String(buffer, 0, offset));
            }
        } finally {
            IOUtils.closeQuietly(gzipInput);
            IOUtils.close(conn);
        }

        Map<String, JSONObject> context = new HashMap<>();
        for (String line : lines.toString().split("\n")) {
            JSONObject jsonObject = JSONObject.parseObject(line);
            String k = jsonObject.getString(key);
            context.put(k, jsonObject);
        }
        return context;
    }
}
