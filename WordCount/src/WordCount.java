import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

/**
 * 
 * @author xiangping
 * @since 2013-12-22
 *
 */

public class WordCount {
	
	
	//实例化日志对象
	private static final Log log=LogFactory.getLog(WordCount.class);
	
	public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable>
	{
		private final static IntWritable one=new IntWritable(1);
		private final Text word =new Text();
		
		public void map(Object key,BSONObject value,Context context) throws IOException, InterruptedException
		{
			
			System.out.println("key:"+key);
			System.out.println("value:"+value);
			
			//对词进行让空格切分
			final StringTokenizer itr=new StringTokenizer(value.get("x").toString());
			while(itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				context.write(word, one);
					
			}
		
		}
	
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		
		private final IntWritable result=new IntWritable();
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			//计算词出现的频率，把相同词的value相加
			int sum=0;
			for(final IntWritable val:values)
			{
				sum+=val.get();
				
				
			}
			result.set(sum);
			context.write(key, result);
			
		}

	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		final Configuration conf=new Configuration();
		//定义Mongodb数据库的输入与输出表名，这里是调用本地的mongodb，默认端口号为2017
		
		

	}

}
