package com.bigdaf.sparkmask.javautil;

import com.bigdaf.sparkmask.vo.MaskVo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class JsonUtil {
    public static MaskVo parseJson(String json){
        if(null!=json | "".equals(json)){
            Gson gson=new GsonBuilder().setDateFormat("yyyyMMddHHmmssSSS").create();
            MaskVo mv=gson.fromJson(json,MaskVo.class);
            return mv;
        }else{
            return null;
        }

    }
}
