package net.myvst.v2.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/**
 * 存储用户记录，
 * firstTime：用户注册时间
 * lastTime：用户最后活跃时间
 * activeDates: 用户活跃日期，逗号拼接
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class UserRecord implements Serializable {
    private String pkg;
    private String uuid;
    private String channel;
    private String ip;
    private String bdModel;
    private String eth0Mac;
    private long dpi;
    private long cpuCnt;
    private String cpuName;
    private String country;
    private String region;
    private String city;
    private boolean touch;
    private long largeMem;
    private long limitMem;
    private String screen;
    private String verCode;
    private String verName;
    private boolean isVip;
    private String bdCpu;
    private String deviceName;
    private long firstTime;
    private long lastTime;
    private String activeDates;
    private long count;
}

/**
 root
 |-- actor: string (nullable = true)
 |-- adType: string (nullable = true)
 |-- analyticId: string (nullable = true)
 |-- androidId: string (nullable = true)
 |-- anylstVer: long (nullable = true)
 |-- area: string (nullable = true)
 |-- bdCpu: string (nullable = true)
 |-- bdModel: string (nullable = true)
 |-- blockId: string (nullable = true)
 |-- blockPos: string (nullable = true)
 |-- buildBoard: string (nullable = true)
 |-- cat: string (nullable = true)
 |-- channel: string (nullable = true)
 |-- cid: string (nullable = true)
 |-- city: string (nullable = true)
 |-- country: string (nullable = true)
 |-- cpuCnt: long (nullable = true)
 |-- cpuName: string (nullable = true)
 |-- date: string (nullable = true)
 |-- decorationTitle: string (nullable = true)
 |-- definition: string (nullable = true)
 |-- deviceId: string (nullable = true)
 |-- deviceName: string (nullable = true)
 |-- director: string (nullable = true)
 |-- dpi: long (nullable = true)
 |-- duration: long (nullable = true)
 |-- endTime: string (nullable = true)
 |-- entry1: string (nullable = true)
 |-- entry1Id: string (nullable = true)
 |-- entry2: string (nullable = true)
 |-- entry2Id: string (nullable = true)
 |-- eth0Mac: string (nullable = true)
 |-- eventKey: string (nullable = true)
 |-- eventName: string (nullable = true)
 |-- eventType: string (nullable = true)
 |-- eventValue: string (nullable = true)
 |-- index: string (nullable = true)
 |-- ip: string (nullable = true)
 |-- ipaddr: string (nullable = true)
 |-- isVip: boolean (nullable = true)
 |-- kafkaTopic: string (nullable = true)
 |-- keyAction: string (nullable = true)
 |-- largeMem: long (nullable = true)
 |-- limitMem: long (nullable = true)
 |-- loginType: string (nullable = true)
 |-- loginType : string (nullable = true)
 |-- mark: string (nullable = true)
 |-- name: string (nullable = true)
 |-- nameId: string (nullable = true)
 |-- openId: string (nullable = true)
 |-- openId2: string (nullable = true)
 |-- optType: string (nullable = true)
 |-- osVersion: long (nullable = true)
 |-- page: string (nullable = true)
 |-- pkg: string (nullable = true)
 |-- playPerc: long (nullable = true)
 |-- pluginPkg: string (nullable = true)
 |-- pluginVercode: long (nullable = true)
 |-- pos: string (nullable = true)
 |-- prePage: string (nullable = true)
 |-- prevPage: string (nullable = true)
 |-- prevue: string (nullable = true)
 |-- rectime: long (nullable = true)
 |-- refreshType: string (nullable = true)
 |-- region: string (nullable = true)
 |-- screen: string (nullable = true)
 |-- serial: string (nullable = true)
 |-- session: string (nullable = true)
 |-- shop: long (nullable = true)
 |-- site: string (nullable = true)
 |-- specId: long (nullable = true)
 |-- subName: string (nullable = true)
 |-- time: string (nullable = true)
 |-- topic: string (nullable = true)
 |-- topicCid: string (nullable = true)
 |-- topicId: string (nullable = true)
 |-- topicType: string (nullable = true)
 |-- touch: boolean (nullable = true)
 |-- uid: string (nullable = true)
 |-- url: string (nullable = true)
 |-- userStatus: long (nullable = true)
 |-- uuid: string (nullable = true)
 |-- verCode: string (nullable = true)
 |-- verName: string (nullable = true)
 |-- vip: boolean (nullable = true)
 |-- wlan0Mac: string (nullable = true)
 |-- year: long (nullable = true)
 * */
