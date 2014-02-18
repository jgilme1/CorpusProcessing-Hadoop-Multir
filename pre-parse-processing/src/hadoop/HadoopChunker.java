package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.knowitall.tool.chunk.ChunkedToken;
import edu.knowitall.tool.chunk.OpenNlpChunker;
import edu.knowitall.tool.postag.PostaggedToken;

public class HadoopChunker implements Tool{

	private Configuration conf;

	public HadoopChunker(){
		conf = new Configuration();
		setConf(conf);
	}
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		conf = arg0;
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,LongWritable,Text>{
		
		private static OpenNlpChunker chunker = new OpenNlpChunker();
		
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> collector, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] values = line.split("\t");
			Integer sentId = Integer.parseInt(values[0]);
			String[] tokens = values[1].split("\\s+");
			String[] offsets = values[2].split("\\s+");
			String[] postags = values[3].split("\\s+");
			List<PostaggedToken> posTokens = new ArrayList<PostaggedToken>();		
			for(int i =0; i < tokens.length; i++){
				String token = tokens[i];
				String pos = postags[i];
				Integer startOffset = Integer.parseInt(offsets[i].split(":")[0]);
				posTokens.add(PostaggedToken.apply(pos,token,startOffset));
			}
			List<ChunkedToken> chunkedTokens =
					scala.collection.JavaConversions.asJavaList(
							chunker.chunkPostagged(scala.collection.JavaConversions.asScalaIterable(posTokens).toSeq()));
			
			StringBuilder chunkBuilder = new StringBuilder();
			for(ChunkedToken ct: chunkedTokens){
				chunkBuilder.append(ct.chunk());
				chunkBuilder.append(" ");
			}
			collector.collect(new LongWritable(sentId), new Text(chunkBuilder.toString().trim()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		JobConf job = new JobConf(conf,HadoopChunker.class);
		
		
		//process command line options
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		job.setJobName("hadoop-chunker");
		job.setInputFormat(TextInputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(Map.class);
		job.set("mapred.child.java.opts", "-Xmx6g");
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setNumMapTasks(Integer.parseInt(args[2]));
		job.setNumReduceTasks(0);
		JobClient.runJob(job);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new HadoopChunker(), args);
	}
	

}
