package com.xcl.hadoop.mr.basestation;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




/**   
* 汇总基站数据表 
* 计算每个用户在不同的时间段不同的基站停留的时长 
* 输入参数 < input path > < output path > < date > < timepoint > 
* 参数示例： “/base /output 2012-09-12 00-09-17-24" 
* 意味着以“/base”为输入，"/output"为输出，指定计算2012年09月12日的数据，并分为00-09，09-17，17-24三个时段 
*/  
public class BaseStationDataPreprocess {
	
	/**   
     * 枚举 计数器 
     * 用于计数各种异常数据 
     */    
    enum Counter   
    {  
        TIMESKIP,       //时间格式有误  
        OUTOFTIMESKIP,  //时间不在参数指定的时间段内  
        LINESKIP,       //源文件行有误  
        USERSKIP        //某个用户某个时间段被整个放弃  
    }  
    
    /**
	 * 输出map对应的key:value
	 * key输出格式：imsi用户id|timeFlag时间段
	 * value输出格式：position基站|t时间偏移量-1970毫秒数
	 */
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
    	String date;//输入参数 指定日期
    	String[] timepoint;//分割时间段小时数组
    	boolean source;
    	
    	//初始化，获取输入参数
    	@Override
    	protected void setup(Context context){
    		this.date = context.getConfiguration().get("date");
    		this.timepoint = context.getConfiguration().get("timepoint").split("-");    //读取时间分割点  
            
            //提取文件名  
            FileSplit fs = (FileSplit)context.getInputSplit();  
            String fileName = fs.getPath().getName();  
            if( fileName.startsWith("POS") )  
            	source = true;  
            else if ( fileName.startsWith("NET") )  
            	source = false;  
            else  
            	System.err.println("没有指定格式的文件！");
    	}
    	
    	@Override
    	protected void map(LongWritable key, Text value, Context context)
    			throws IOException, InterruptedException {
    		String line = value.toString();
    		TableLine tableLine = new TableLine();
    		try {
				tableLine.set(line, source, date, timepoint);
			} catch (Exception e) {
				e.printStackTrace();
			}
    		context.write(tableLine.outKey(), tableLine.outValue());
    	}
    	
    	
    }

    public static class Reduce extends Reducer<Text, Text, NullWritable, Text>{
    	private String date;//输入参数-指定日期
    	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	
    	@Override
    	protected void setup(Context context) throws IOException, InterruptedException {
    		this.date = context.getConfiguration().get("date");
    	}
    	
    	@Override
    	protected void reduce(Text key, Iterable<Text> values,Context context)
    			throws IOException, InterruptedException {
    		String imsi = key.toString().split("\\|")[0];
    		String timeFlag = key.toString().split("\\|")[1];
    		
    		//TreeMap记录-排序时间-key时间-value基站
    		TreeMap<Long, String> uploads = new TreeMap<>();
    		String valueString;
    		for (Text value : values) {
				valueString = value.toString();
				uploads.put(Long.valueOf(valueString.split("\\|")[1]), valueString.split("\\|")[0]);
			}
    		//在最后添加OFF
    		try {
				Date tmpDate = this.formatter.parse(this.date + " " + timeFlag.split("-")[1] + ":00:00");
				uploads.put(tmpDate.getTime()/1000L, "OFF");
    		} catch (ParseException e) {
				e.printStackTrace();
			}
    		//计算时间差
    		 HashMap<String, Float> locs = getStayTime(uploads);  
    		 for (Entry<String, Float> entry : locs.entrySet()) {
    			 StringBuilder builder = new StringBuilder();  
                 builder.append(imsi).append("|");  
                 builder.append(entry.getKey()).append("|");  
                 builder.append(timeFlag).append("|");  
                 builder.append(entry.getValue());  
                   
                 context.write( NullWritable.get(), new Text(builder.toString()) );  
             }  
		}
    
    
    	private HashMap<String, Float> getStayTime(TreeMap<Long, String> uploads){
    		Entry<Long, String> upload, nextUpload;
    		HashMap<String, Float> locs = new HashMap<>();
    		Iterator<Entry<Long, String>> iterator = uploads.entrySet().iterator();
    		upload = iterator.next();
    		while(iterator.hasNext()){
    			nextUpload = iterator.next();
    			float diff = (float)(nextUpload.getKey()-upload.getKey())/60.0f;
    			if(diff <= 60.0){
    				if(locs.containsKey(upload.getValue())){
    					locs.put(upload.getValue(), locs.get(upload.getValue()) + diff);
    				}else{
    					locs.put(upload.getValue(), diff);
    				}
    			}
    			upload = nextUpload;
    		}
    		return locs;
    	}
    } 

    
    public static void main(String[] args) throws Exception {
    	
    	String input = "hdfs://192.168.2.123:9000/hadoop/BaseStation/";
        String output = "hdfs://192.168.2.123:9000/hadoop/output/";
        
        Configuration conf = new Configuration();  
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");
        conf.set("date", args[0]);//设置指定的日期  
        conf.set("timepoint", args[1]);//设置指定的时间段  
        
        //检查一下参数所指定的输出路径是否存在，如果已存在，先删除
  		Path path = new Path(output);
  		FileSystem fs = FileSystem.get(conf);
  		if(fs.exists(path)){
  			fs.delete(path, true);
  		}
        
  		Job job = new Job(conf,"BaseStationDataPreprocess");  
  		
  		job.setJarByClass(BaseStationDataPreprocess.class);  
  	    job.setMapperClass(Map.class);  
  	    job.setReducerClass(Reduce.class);  
  	    job.setOutputKeyClass(Text.class);  
  	    job.setOutputValueClass(Text.class);  
  	    FileInputFormat.addInputPath(job,new Path(input));  
  	    FileOutputFormat.setOutputPath(job,new Path(output));  
  	    System.exit(job.waitForCompletion(true) ? 0 : 1);  
  		
    }
    
    
}





