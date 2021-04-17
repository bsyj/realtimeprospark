package com.atguigu.utils
import java.io.InputStreamReader
import java.util.Properties
object PropertiesUtil {
  def load(propertiesName : String) : Properties = {
    val properties = new Properties()
    /**
     * 加载配置文件路径相对与classPath加载
     * 推荐使用Thread.currentThread().getContextClassLoader().getResourceAsStream("")
     * 来得到当前的classpath的绝对路径的URI表示法
     */
    properties.load(
      new InputStreamReader(
        Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName)
      )
    )
    properties
  }
}
