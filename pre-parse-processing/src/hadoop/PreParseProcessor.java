package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.stanford.nlp.ling.CoreAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class PreParseProcessor implements Tool {

	private Configuration conf;
	
	
	public PreParseProcessor(){
		conf = new Configuration();
		setConf(conf);
	}
	
    private static final class SentGlobalID implements CoreAnnotation<Integer>{
		@Override
		public Class<Integer> getType() {
			return Integer.class;
		}
    }
	
	
	public static class Map extends MapReduceBase implements Mapper<Text,Text,Text,CoreMapWritable> {

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, CoreMapWritable> collector, Reporter reporter)
				throws IOException {
			String[] values = value.toString().split("\t");
			String docName = values[0];
			String tokenString = values[1];
			String tokenOffsetString = values[2];
			String sentenceOffsetString = values[3];
			String sentenceTextString = values[4];
			
			// pretend nothing can be null
			
			//text value
			CoreMap sentence = new Annotation(sentenceTextString);
			//sentence global id
			sentence.set(SentGlobalID.class, Integer.parseInt(key.toString()));
			//sentence offsets
			String[] sentenceOffsetStringValues = sentenceOffsetString.split("\\s+");
			Integer sentenceStartOffset = Integer.parseInt(sentenceOffsetStringValues[0]);
			Integer sentenceEndOffset = Integer.parseInt(sentenceOffsetStringValues[1]);
			sentence.set(CoreAnnotations.CharacterOffsetBeginAnnotation.class,sentenceStartOffset);
			sentence.set(CoreAnnotations.CharacterOffsetEndAnnotation.class,sentenceEndOffset);
			
			//get tokens
			List<CoreLabel> coreLabelTokens = new ArrayList<CoreLabel>();
			String[] tokens = tokenString.split("\\s+");
			String[] tokenOffsets = tokenOffsetString.split("\\s+");
			for(int i =0; i < tokens.length; i++){
				String token = tokens[i];
				String[] tokenOffsetsValues = tokenOffsets[i].split(":");
				Integer startOffset = Integer.parseInt(tokenOffsetsValues[0]);
				Integer endOffset = Integer.parseInt(tokenOffsetsValues[1]);
				CoreLabel coreLabelToken = new CoreLabel();
				coreLabelToken.setWord(token);
				coreLabelToken.setBeginPosition(startOffset+sentenceStartOffset);
				coreLabelToken.setEndPosition(endOffset+sentenceStartOffset);
				coreLabelTokens.add(coreLabelToken);
			}
			
			if(coreLabelTokens.size() != tokens.length){
				throw new IllegalStateException("CoreLabel Token length should be equivalent to raw string token length");
			}
			
			//add corelabels to sentence Annotation
			sentence.set(CoreAnnotations.TokensAnnotation.class, coreLabelTokens);
			
			collector.collect(new Text(docName), new CoreMapWritable(sentence));
			//System.out.println(docName + "\t" + key.toString() + "\t" + tokenString);
		}
	}
	
	
	public static class Reduce extends MapReduceBase implements Reducer <Text,CoreMapWritable,LongWritable, Text>{
		static{
//			System.err.println("STARTING REDUCE FREEMEMORY = " +Runtime.getRuntime().freeMemory());
//			System.err.println("STARTING REDUCE TOTALMEMORY = " +Runtime.getRuntime().totalMemory());
//			System.err.println("STARTING REDUCE MAXMEMORY = " + Runtime.getRuntime().maxMemory());

		}
		private final static Properties props = new Properties();

		static{
			props.put("annotators", "pos,lemma,ner");
			props.put("sutime.binders","0");
		}

		private final static StanfordCoreNLP pipeline = new StanfordCoreNLP(props,false);

		static{
//			System.err.println("LOADED PIPELINE FREEMEMORY = " +Runtime.getRuntime().freeMemory());
//			System.err.println("LOADED PIPELINE TOTALMEMORY = " +Runtime.getRuntime().totalMemory());
//			System.err.println("LOADED PIPELINE MAXMEMORY = " + Runtime.getRuntime().maxMemory());

		}

		@Override
		public void reduce(Text key, Iterator<CoreMapWritable> values,
				OutputCollector<LongWritable, Text> collector, Reporter reporter)
				throws IOException {
//			System.err.println("BEFORE READ SENTENCES FREEMEMORY = " +Runtime.getRuntime().freeMemory());
//			System.err.println("BEFORE READ SENTENCES TOTALMEMORY = " +Runtime.getRuntime().totalMemory());
//			System.err.println("BEFORE READ SENTENCE MAXMEMORY = " + Runtime.getRuntime().maxMemory());

			
			List<CoreMapWritable> sentences = new ArrayList<CoreMapWritable>();
//			System.out.println("Printing values for the following doc");
//			System.out.println(key.toString());
			while(values.hasNext()){
				CoreMapWritable next = values.next();
				CoreMap cm = next.getCoreMap();
				//System.out.println( next.getID() + "\t" + cm.get(SentGlobalID.class) + "\t" + cm.get(CoreAnnotations.TextAnnotation.class));
				sentences.add(new CoreMapWritable(cm));
			}
			
//			System.out.println("before sort");
//			for(CoreMapWritable sen : sentences){
//				System.out.println(sen.getID() + "\t" + sen.getCoreMap().get(SentGlobalID.class) + "\t" + sen.getCoreMap().get(CoreAnnotations.TextAnnotation.class));
//			}
			
//			System.err.println("READ IN SENTENCES FREEMEMORY = " +Runtime.getRuntime().freeMemory());
//			System.err.println("READ IN SENTENCES TOTALMEMORY = " +Runtime.getRuntime().totalMemory());
//			System.err.println("READ IN SENTENCES MAXMEMORY = " + Runtime.getRuntime().maxMemory());

			
			//sort sentences
			Collections.sort(sentences);
			
//			System.out.println("Printing sorted sentences:");
//			for(CoreMapWritable sen : sentences){
//				System.out.println(sen.getCoreMap().get(SentGlobalID.class) + "\t" + sen.getCoreMap().get(CoreAnnotations.TextAnnotation.class));
//			}
			
			//create Annotation document
			List<CoreMap> coreMapSentences = new ArrayList<CoreMap>();
			for(CoreMapWritable cmw : sentences){
				coreMapSentences.add(cmw.getCoreMap());
			}
			Annotation doc = new Annotation(coreMapSentences);
			
//			System.out.println("Printing document sentences:");
//			for(CoreMap sen : doc.get(CoreAnnotations.SentencesAnnotation.class)){
//				System.out.println(sen.get(SentGlobalID.class) + "\t" + sen.get(CoreAnnotations.TextAnnotation.class));
//			}
//			
			//process document
			pipeline.annotate(doc);
			
			//print document
			
			
			//iterate over sentences outputting results
			//System.out.println("Printing annotated sentences:");
			List<CoreMap> annotatedSentences = doc.get(CoreAnnotations.SentencesAnnotation.class);
			for(CoreMap annotatedSen : annotatedSentences){
				Integer globalID = annotatedSen.get(SentGlobalID.class);
				LongWritable outputKey = new LongWritable(globalID);
				//System.out.println(outputKey + "\t" + annotatedSen.get(CoreAnnotations.TextAnnotation.class));
				StringBuilder outputValueBuilder = new StringBuilder();
				StringBuilder nerBuilder = new StringBuilder();
				StringBuilder posBuilder = new StringBuilder();
				StringBuilder lemmaBuilder = new StringBuilder();
				List<CoreLabel> toks = annotatedSen.get(CoreAnnotations.TokensAnnotation.class);
				for(CoreLabel tok : toks){
					nerBuilder.append(" ");
					nerBuilder.append(tok.ner());
					posBuilder.append(" ");
					posBuilder.append(tok.get(CoreAnnotations.PartOfSpeechAnnotation.class));
					lemmaBuilder.append(" ");
					lemmaBuilder.append(tok.lemma());
				}
				outputValueBuilder.append(posBuilder.toString().trim());
				outputValueBuilder.append("\t");
				outputValueBuilder.append(nerBuilder.toString().trim());
				outputValueBuilder.append("\t");
				outputValueBuilder.append(lemmaBuilder.toString().trim());
				
				collector.collect(outputKey, new Text(outputValueBuilder.toString()));
			}
			
		}
		
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		conf = arg0;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		JobConf job = new JobConf(conf,PreParseProcessor.class);
		
		
		//process command line options
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		job.setJobName("preparseprocessor");
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(CoreMapWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMemoryForReduceTask(8144);
		job.set("mapred.child.java.opts", "-Xmx6g");
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setNumMapTasks(Integer.parseInt(args[2]));
		job.setNumReduceTasks(Integer.parseInt(args[3]));
		
		JobClient.runJob(job);
		return 0;
	}

	
	
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new PreParseProcessor(), args);
	}
	
	
//	private static class SentenceComparator implements Comparator<CoreMap>{
//
//		@Override
//		public int compare(CoreMap arg0, CoreMap arg1) {
//			Integer first = arg0.get(SentGlobalID.class);
//			Integer second = arg1.get(SentGlobalID.class);
//			return (first - second);
//		}
//		
//	}
	
	private static class CoreMapWritable implements Writable , Comparable<CoreMapWritable>{

		private CoreMap cm;
		private Integer id;
		
		
		public CoreMapWritable(CoreMap cm){
			this.cm = new Annotation((Annotation)cm);
			this.id = cm.get(SentGlobalID.class);
		}
		
		public CoreMapWritable(){
			this.id = -1;
			cm = new Annotation("");
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			Integer sentID = in.readInt();
			String senText = in.readUTF();
			Integer sentStartOffset = in.readInt();
			Integer sentEndOffset = in.readInt();
			
			Integer numTokens = in.readInt();
			
			List<CoreLabel> coreLabelTokens = new ArrayList<CoreLabel>();
			for(int i =0; i < numTokens; i ++){
				CoreLabel tok = new CoreLabel();
				
				Integer tokStartOffset = in.readInt();
				Integer tokEndOffset = in.readInt();
				
				String tokWord = in.readUTF();
				
				tok.set(CoreAnnotations.CharacterOffsetBeginAnnotation.class,tokStartOffset);
				tok.set(CoreAnnotations.CharacterOffsetEndAnnotation.class,tokEndOffset);
				tok.setWord(tokWord);
				coreLabelTokens.add(tok);
			}
			
			//update coremap
			this.cm.set(SentGlobalID.class, sentID);
			this.cm.set(CoreAnnotations.TextAnnotation.class, senText);
			this.cm.set(CoreAnnotations.CharacterOffsetBeginAnnotation.class,sentStartOffset);
			this.cm.set(CoreAnnotations.CharacterOffsetEndAnnotation.class,sentEndOffset);
			this.cm.set(CoreAnnotations.TokensAnnotation.class,coreLabelTokens);
			
			this.id = sentID;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			//write sent ID
			out.writeInt(cm.get(SentGlobalID.class));
			//write sent Text
			out.writeUTF(cm.get(CoreAnnotations.TextAnnotation.class));
			//write sent start offset
			out.writeInt(cm.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class));
			out.writeInt(cm.get(CoreAnnotations.CharacterOffsetEndAnnotation.class));
			//write number of tokens
			List<CoreLabel> coreLabelTokens = cm.get(CoreAnnotations.TokensAnnotation.class);
			Integer numCoreLabelTokens = coreLabelTokens.size();
			out.writeInt(numCoreLabelTokens);
			
			//serialize each token's information
			for(int i =0; i < numCoreLabelTokens; i++){
				CoreLabel tok = coreLabelTokens.get(i);
				//serialize offsets
				out.writeInt(tok.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class));
				out.writeInt(tok.get(CoreAnnotations.CharacterOffsetEndAnnotation.class));
				
				//serialize word
				out.writeUTF(tok.word());
			}			
		}
		
		public CoreMap getCoreMap(){
			return cm;
		}
		
		public Integer getID(){
			return id;
		}
		
//		@Override
//		public boolean equals(Object o){
//			CoreMapWritable other = (CoreMapWritable)o;
//			if(other.id.equals(this.id)){
//				return true;
//			}
//			else{
//				return false;
//			}
//		}
//		
//		public int hashCode(){
//			return this.id.hashCode();
//		}


		@Override
		public int compareTo(CoreMapWritable o) {
			Integer thisId = this.id;
			Integer otherId = o.id;
			//System.out.println("COMPARING " + thisId + " and " + otherId);
			return (thisId - otherId);
		}
	}
}
