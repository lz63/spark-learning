package com.erongda.bigdata.project.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.erongda.bigdata.project.common.EventLogConstants;
import com.erongda.bigdata.project.util.IPSeekerExt.RegionInfo;
import com.erongda.bigdata.project.util.UserAgentUtil.UserAgentInfo;

/**
 * 用于解析日志文件，对文件进行字段的提取和补全
	-》以^A进行分割
		-》ip
		-》时间戳
		-》主机名
		-》URI
	-》对URI进行解析，以？进行分割
		-》请求的资源
		-》事件所收集的所有字段
	-》对事件进行解析，以&进行分割
		-》每一条记录的每一个字段的名称和值
	-》对key=value进行分割，以=进行分割
		-》字段的名称
		-》字段的值
		-》对某些字段进行解码
	-》通过ip地址分析
		-》国家
		-》省份
		-》城市
	-》通过客户端字段分析
		-》浏览器的类型
		-》浏览器的版本
		-》操作系统名称
		-》操作系统版本
-》将所有的字段的名称和值，存储到map集合中
 * @author 江城子
 *
 */

public class LogParser {
	
	private Logger logger = LoggerFactory.getLogger(LogParser.class); 

	public Map<String, String> handleLogParser(String logText) throws UnsupportedEncodingException{
		
		//new map instance
		Map<String, String> logInfo = new HashMap<String,String>();
		//analysis the log of line
		if(StringUtils.isNotBlank(logText)){
			String[] splits = logText.split("\\^A");
			if(splits.length == 4){
				//define ip,stime,hostname,uri
				String ip = splits[0];
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_IP, ip);
				String s_time = splits[1];
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, s_time);
				String hostname = splits[2];
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_HTTP_HOST, hostname);
				String uri = splits[3];
				//解析URI
				this.AnalysisURI(uri,logInfo);
				//解析IP
				this.AnalysisIP(ip,logInfo);
				//解析客户端
				this.AnalysisUserAgent(logInfo);
			}
		}else{
			logger.error("该条日志记录的格式不正确："+logText);
		}
		
		return logInfo;
		
	}

	/**
	 * 解析客户端，得到浏览器和操作系统的信息
	 * @param logInfo
	 */
	public void AnalysisUserAgent(Map<String, String> logInfo) {
		// TODO Auto-generated method stub
		//从集合中获取用户客户端字段
		String agent = logInfo.get(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
		UserAgentInfo userAgent = UserAgentUtil.analyticUserAgent(agent);
		String browsername = userAgent.getBrowserName();
		String browserversion = userAgent.getBrowserVersion();
		String osname = userAgent.getOsName();
		String osversion = userAgent.getOsVersion();
		if(StringUtils.isNotBlank(browsername)){
			logInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, browsername);
		}
		if(StringUtils.isNotBlank(browserversion)){
			logInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, browserversion);
		}
		if(StringUtils.isNotBlank(osname)){
			logInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, osname);
		}
		if(StringUtils.isNotBlank(osversion)){
			logInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, osversion);
		}
	}

	/**
	 * 解析ip地址，得到国家省份城市
	 * @param ip
	 * @param logInfo
	 */
	public void AnalysisIP(String ip, Map<String, String> logInfo) {
		if(StringUtils.isNotBlank(ip)){
			IPSeekerExt ipseek = IPSeekerExt.getInstance();
			RegionInfo regionInfo = ipseek.analyseIp(ip);
			String country = regionInfo.getCountry();
			String province = regionInfo.getProvince();
			String city = regionInfo.getCity();
			if(StringUtils.isNotBlank(country)){
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, country);
			}
			if(StringUtils.isNotBlank(province)){
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, province);
			}
			if(StringUtils.isNotBlank(city)){
				logInfo.put(EventLogConstants.LOG_COLUMN_NAME_CITY, city);
			}
		}
	}
	
	/**
	 * 解析URI
	 * @param uri
	 * @param logInfo
	 * @throws UnsupportedEncodingException
	 */
	public void AnalysisURI(String uri, Map<String, String> logInfo) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		if(StringUtils.isNotBlank(uri)){
			String[] splits = uri.split("\\?");
			if(splits.length == 2){
				String e_source = splits[1];
				String[] keyvalues = e_source.split("&");
				for(String keyvalue : keyvalues){
					String key = keyvalue.split("=")[0];
					String value = keyvalue.split("=")[1];
					String realvalue = URLDecoder.decode(value, "utf-8");
					logInfo.put(key, realvalue);
				}
			}
		}
	}
	
}













