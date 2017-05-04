package com.xcl.hadoop.mr.basestation;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;

/**
 *对map读的一行数据解析
 *输出对应的key:value
 */
public class TableLine {
	//imsi用户id,position基站,time时间,timeFlag时间段
	private String imsi,position,time,timeFlag;
	private Date day;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/**
	 * 
	 * @param line map一行数据
	 * @param source 位置数据还是上网数据
	 * @param date	输入参数指定的日期
	 * @param timepoint 分割后的时间段数组
	 * @throws Exception 
	 */
	public void set(String line, boolean source, String date, String[] timepoint) 
			throws Exception{
		
		String[] lineSplit = line.split("\t");
		if(source){
			//解析位置数据
			this.imsi = lineSplit[0];
			this.position = lineSplit[3];
			this.time = lineSplit[4];
		}else{
			//解析上网数据
			this.imsi = lineSplit[0];
			this.position = lineSplit[2];
			this.time = lineSplit[3];
		}
		
		//检查日期的合法性
		if(!this.time.startsWith(date)){
			System.err.println("本行数据的所在日期不是指定的日期！");
		}
		this.day = this.formatter.parse(this.time);
		
		//计算时间段
		int i=0, n=timepoint.length;
		int hour = Integer.valueOf(this.time.split(" ")[1].split(":")[0]);
		while(i < n && Integer.valueOf(timepoint[i]) <= hour){
			i++;
		}
		if(i < n){
			if(i == 0){
				//设定其他的时间段
				this.timeFlag = "00-" + timepoint[i];
			}else{
				this.timeFlag = timepoint[i-1] + "-" + timepoint[i];
			}
		}else{
			System.err.println("本行数据的所在日期（小时）超出指定时间段！");
		}
	}

	/**
	 * 输出map对应的key
	 * 输出格式：imsi用户id|timeFlag时间段
	 */
	public Text outKey(){
		return new Text(this.imsi + "|" + this.timeFlag);
	}
	
	/**
	 * 输出map对应的value
	 * 输出格式：position基站|t时间偏移量-1970毫秒数
	 */
	public Text outValue(){
		long t = day.getTime()/1000L;
		return new Text(this.position + "|" + String.valueOf(t));
	}
}
