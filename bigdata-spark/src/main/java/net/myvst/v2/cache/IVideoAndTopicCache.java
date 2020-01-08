package net.myvst.v2.cache;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

public interface IVideoAndTopicCache extends Serializable {

    JSONObject getVideo(String k);

    JSONObject getVideoTopic(String k);
}
