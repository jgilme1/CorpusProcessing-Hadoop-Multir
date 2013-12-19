package hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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

import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreebankLanguagePack;
import edu.stanford.nlp.trees.TypedDependency;

public class PostParseProcessor implements Tool {
	
	private Configuration configuration;
	
	public PostParseProcessor(){
		configuration = new Configuration();
		setConf(configuration);
	}
	
	
	private static final TreebankLanguagePack tlp = new PennTreebankLanguagePack();
	private static final GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Integer,Text> {	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Integer, Text> output, Reporter reporter)
				throws IOException {
			
			String[] values = value.toString().split("\t");
			
			Integer sentId = Integer.parseInt(values[0]);
			if(values.length > 1){
				String parseString = values[1];	
				
				// we don't allow | since we treat that as a special character
				parseString = parseString.replace("|", " ");
				
				Tree parse = Tree.valueOf(parseString);
				GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
				Collection<TypedDependency> tdl = null;
				try {
					tdl = gs.allTypedDependencies(); 
				} catch (NullPointerException e) {
					// there has to be a bug in EnglishGrammaticalStructure.collapseFlatMWP
					tdl = new ArrayList<TypedDependency>();
				}
				
				StringBuilder sb = new StringBuilder();
				List<TypedDependency> l = new ArrayList<TypedDependency>();
				l.addAll(tdl);
				for (int j=0; j < tdl.size(); j++) {
					TypedDependency td = l.get(j);
					String name = td.reln().getShortName();
					if (td.reln().getSpecific() != null)
						name += "-" + td.reln().getSpecific();				
					sb.append((td.gov().index()) + " ");
					sb.append(name + " ");
					sb.append((td.dep().index()));
					if (j < tdl.size()-1)
						sb.append("|");
				}
				
				output.collect(sentId, new Text(sb.toString()));
			}
			else{
				output.collect(sentId,new Text(""));
			}
		}
	}
	
	
	
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new PostParseProcessor(), args);
	}




	@Override
	public Configuration getConf() {
		return configuration;
	}




	@Override
	public void setConf(Configuration arg0) {
		configuration = arg0;
	}




	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		JobConf job = new JobConf(conf,PostParseProcessor.class);
		
		
		//process command line options
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		job.setJobName("postparseprocessor");
		job.setOutputKeyClass(Integer.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(Map.class);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setNumReduceTasks(0);
		job.setNumMapTasks(Integer.parseInt(args[2]));
		
		JobClient.runJob(job);
		return 0;
	}

}
