package com.bigdaf.sparkmask.controller

import com.bigdaf.sparkmask.javautil.JsonUtil
import com.bigdaf.sparkmask.service.RuleBasedSparkMask
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.ComponentScan
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.{PathVariable, RequestMapping, RequestMethod, ResponseBody}


@ComponentScan
@Controller
@ResponseBody
//@RequestMapping(value = Array("/find"))
/**
  * 前端传来的请求信息格式：/localhost:8086/mask/{dbName:whp,tables:payes}
  */
class HiveController @Autowired()(private val ruleBaseMask:RuleBasedSparkMask){
  @RequestMapping(value = Array("/mask/{jsonStr}"), method = Array(RequestMethod.GET))
   def testMask(@PathVariable(value = "jsonStr") str: String):String={
       //解析json，转为对象
       val maskVo=JsonUtil.parseJson(str)
       val ss=ruleBaseMask.MaskTask(maskVo)
         println(ss.toString)
         ss
   }


}
