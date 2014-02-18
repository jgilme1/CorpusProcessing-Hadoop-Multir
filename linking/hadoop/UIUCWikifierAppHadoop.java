package hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration.JNDIConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.security.JniBasedUnixGroupsMapping;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import edu.illinois.cs.cogcomp.edison.sentences.TextAnnotation;
import edu.illinois.cs.cogcomp.wikifier.common.GlobalParameters;
import edu.illinois.cs.cogcomp.wikifier.common.GlobalPaths;
import edu.illinois.cs.cogcomp.wikifier.common.GlobalParameters.SettingManager;
import edu.illinois.cs.cogcomp.wikifier.inference.InferenceEngine;
import edu.illinois.cs.cogcomp.wikifier.models.LinkingProblem;
import edu.illinois.cs.cogcomp.wikifier.models.Mention;
import edu.illinois.cs.cogcomp.wikifier.models.ReferenceInstance;
import edu.illinois.cs.cogcomp.wikifier.utils.io.OutFile;


import LBJ2.infer.GurobiHook;


public class UIUCWikifierAppHadoop implements Tool {

	Configuration conf;
	private static final String pathToDefaultNERConfigFile = "configs/NER.config";
	private static final String pathToDefaultNEConfigFile = "data/NESimdata/config.txt";
	private static final String pathToDefaultJWNLConfigFile = "configs/jwnl_properties.xml";


	public UIUCWikifierAppHadoop(){
		Configuration configuration = new Configuration();
		setConf(configuration);
	}
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		conf = arg0;
	}
	
	public static class Map extends MapReduceBase implements Mapper<Text,Text,Text,Text>{
		private String wikifierPath;
        InferenceEngine inference = null;

		@Override
		public void configure(JobConf job){
			System.out.println("Configuring....");			
			Path[] cachedArchives = null;
			try {
				cachedArchives = DistributedCache.getLocalCacheArchives(job);
			}
			catch(IOException e){
				e.printStackTrace();
			}
			for(Path p : cachedArchives){
				System.out.println(p.getName());
				if(p.getName().equals("Wikifier2013.tar.gz")){
					wikifierPath = p.toString()+"/Wikifier2013";
				}
			}
			//initialize config files
			try {
				String[] newConfigPaths = writeNewConfigFiles(wikifierPath);
				GlobalParameters.loadSettings(getSettingManager(wikifierPath,newConfigPaths[0],newConfigPaths[1],newConfigPaths[2]));
				inference = new InferenceEngine(false);
			}
			  catch (Exception e) {
				e.printStackTrace();
			}
				
				
			System.out.println("Configured mapper");
			System.out.println("wikifierPath = " + wikifierPath);
		}
		
		@Override
		public void map(Text key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {

			try{
				TextAnnotation ta = GlobalParameters.curator.getTextAnnotation(value.toString()); 
				LinkingProblem problem=new LinkingProblem(key.toString(), ta, new ArrayList<ReferenceInstance>());
				inference.annotate(problem, null, false, false, 0);
				String wikifiedOutput = getWikifierOutput(problem);
				
				
				output.collect(key,new Text(wikifiedOutput));
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf,UIUCWikifierAppHadoop.class);
		
		
		
//		System.out.println("Run.. Envinronment Variables");
//		java.util.Map<String,String> env = System.getenv();
//
//		System.out.println("Printing environment variables");
//		for(String k : env.keySet()){
//			System.out.println(k + "\t" + env.get(k));
//		}
//		String jlpValue = System.getProperty("java.library.path");
//		System.out.println("java.library.path=" + jlpValue);
//		System.setProperty("java.library.path", jlpValue + ":" + "/home/jgilme1/bin/gurobi550/linux64/lib");
		
		
		//process command line options
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		//change current working directory to hdfs path..
		job.setJobName("entitylinker");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormat(DistributeInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(Map.class);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setNumReduceTasks(0);
		job.setNumMapTasks(Integer.parseInt(args[2]));
		job.set("mapreduce.input.fileinputformat.split.minsize", "0");
		job.set("mapred.child.java.opts", "-Xmx16g");
		job.setNumTasksToExecutePerJvm(-1);
		//job.setMemoryForMapTask(new Long(12288));
		//job.set(JobConf.MAPRED_MAP_TASK_ULIMIT, "12582912");
		


		String gurobiHomeVariable = "GUROBI_HOME";
		String gurobiHomeValue = "/home/jgilme1/bin/gurobi560/linux64";
		String pathVariable = "PATH";
		String newPathValue = gurobiHomeValue+"/bin";
		String ldLibraryPathVariable = "LD_LIBRARY_PATH";
		String ldLibraryPathValue = gurobiHomeValue+"/lib";
		String grbLicenseFileVariable = "GRB_LICENSE_FILE";
	    String grbLicenseFileValue = "/scratch6/usr/jgilme1/gurobiLicense/gurobi.lic";
		
		StringBuilder newEnvironment = new StringBuilder();
		newEnvironment.append(gurobiHomeVariable);
		newEnvironment.append("=");
		newEnvironment.append(gurobiHomeValue);
		newEnvironment.append(",");
		newEnvironment.append(pathVariable);
		newEnvironment.append("=");
		newEnvironment.append("$"+pathVariable+":");
		newEnvironment.append(newPathValue);
		newEnvironment.append(",");
		newEnvironment.append(ldLibraryPathVariable);
		newEnvironment.append("=$"+ldLibraryPathVariable+":");
		newEnvironment.append(ldLibraryPathValue);
		newEnvironment.append(",");
		newEnvironment.append(grbLicenseFileVariable);
		newEnvironment.append("=");
		newEnvironment.append(grbLicenseFileValue);
		
		//System.out.println(newEnvironment.toString());
		job.set(JobConf.MAPRED_MAP_TASK_ENV, newEnvironment.toString());
		

		DistributedCache.addCacheArchive(new URI("/user/jgilme1/entitylinking/Wikifier2013.tar.gz"), job);
		
		JobClient.runJob(job);
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new UIUCWikifierAppHadoop(), args);
	}
	
	
	public static class WholeFileInputFormat extends FileInputFormat<Text,Text>{
		@Override
		public boolean isSplitable(FileSystem fs, Path filename){
			return false;
		}
		@Override
		public RecordReader<Text, Text> getRecordReader(InputSplit arg0,
				JobConf job, Reporter arg2) throws IOException {
			return new WholeFileRecordReader((FileSplit)arg0,job);
		}
	}
	
	public static class FileLocationInputFormat extends FileInputFormat<Text,Text>{

		@Override
		public boolean isSplitable(FileSystem fs, Path filename){
			return true;
		}
		@Override
		public RecordReader<Text, Text> getRecordReader(InputSplit split,
				JobConf conf, Reporter reporter) throws IOException {
			return new FileLocationRecordReader((FileSplit)split,conf);
		}		
	}
	
	public static class FileLocationRecordReader implements RecordReader<Text,Text>{

		Configuration conf;
		FileSplit split;
		String locationFileString;
		String [] locationFileLines;
		FileSystem fs;
		int lineCount;
		int lineIndex;
		
		public FileLocationRecordReader(FileSplit split, Configuration conf) throws IOException{
			this.split = split;
			this.conf = conf;
			fs = FileSystem.get(conf);
			locationFileString = org.apache.commons.io.IOUtils.toString(new InputStreamReader(fs.open(split.getPath())));
			lineIndex =0;
			lineCount = locationFileString.split("\n").length;
			locationFileLines = locationFileString.split("\n");
		}
		
		@Override
		public void close() throws IOException {
		}

		@Override
		public Text createKey() {
			return new Text();
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			int byteCount = 0;
			
			for(int i =0; i < lineIndex; i++){
				byteCount += locationFileLines[i].getBytes().length;
			}
			return byteCount;
		}

		@Override
		public float getProgress() throws IOException {
			return ((float)lineIndex+1) / (float)lineCount;
		}

		@Override
		public boolean next(Text key, Text value) throws IOException {
			if(lineIndex < locationFileLines.length){
				String currentLine = locationFileLines[lineIndex];
				//System.out.println(currentLine);
				String[] lineValues = currentLine.split("\t");
				String docName = lineValues[0];
				String hdfsPath = lineValues[1];
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(hdfsPath))));
				key.set(docName);
				StringBuilder valueBuilder = new StringBuilder();
				String nextLine;
				while((nextLine = br.readLine())!=null){
					//System.out.println(nextLine);
					valueBuilder.append(nextLine);
					valueBuilder.append("\n");
				}
				value.set(valueBuilder.toString().trim());
				br.close();
				lineIndex++;
				return true;
			}
			else{
				return false;
			}
		}	
	}
	
	public static class WholeFileRecordReader implements RecordReader<Text,Text>{

		private JobConf job;
		private FileSplit fileSplit;
		boolean processed = false;
		
		public WholeFileRecordReader(FileSplit arg0, JobConf job) {
			this.job = job;
			this.fileSplit = arg0;
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public Text createKey() {
			return new Text();
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			return processed ? fileSplit.getLength() : 0;
		}

		@Override
		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f ;
		}

		@Override
		public boolean next(Text key, Text value) throws IOException {
			if(!processed){
				key.set(fileSplit.getPath().getName());
				FileSystem fs = fileSplit.getPath().getFileSystem(job);
				FSDataInputStream in = null;
				try{
					byte[] contents = new byte[(int)fileSplit.getLength()];
					in = fs.open(fileSplit.getPath());
					in.readFully(contents, 0, contents.length);
					value.set(contents);
					
				}
				finally{
					in.close();
				}
				processed = true;
				return true;
			}
			return false;
		}
		
	}
	
	/**
	 * Read in NER and NE default config files, write out new config files
	 * with appropriate paths then save config file and return its location
	 * @param pathToWikifierFiles
	 * @return
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	private static String[] writeNewConfigFiles(String pathToWikifierFiles) throws FileNotFoundException, IOException {
		String[] configFiles = new String[3];
		
		//read in old ner config parameters and change
		List<String> nerConfigLines = IOUtils.readLines(new FileInputStream(new File(pathToWikifierFiles+ "/" +pathToDefaultNERConfigFile)));
		List<String> newNERConfigLines = new ArrayList<String>();
		for(String l : nerConfigLines){
			String[] values = l.split("\\t+");
			StringBuilder newLine = new StringBuilder();
			for(String value: values){
				if(value.contains("/")){
					newLine.append(pathToWikifierFiles+"/"+value);
					newLine.append("\t");
				}
				else{
					newLine.append(value);
					newLine.append("\t");
				}
			}
			newNERConfigLines.add(newLine.toString().trim());
		}
		
		
		//write out new config parameters
		File newNERConfigFile = File.createTempFile("NER.config", ".tmp");
		newNERConfigFile.deleteOnExit();
		configFiles[0] = newNERConfigFile.getAbsolutePath();
		BufferedWriter nerWriter = new BufferedWriter(new FileWriter(newNERConfigFile));
		for(String l : newNERConfigLines){
			System.out.println(l);
			nerWriter.write(l+"\n");
		}
		nerWriter.close();
		
		
		
		//read in old ne config parameters and change
		List<String> neConfigLines = IOUtils.readLines(new FileInputStream(new File(pathToWikifierFiles + "/" + pathToDefaultNEConfigFile)));
		List<String> newNEConfigLines = new ArrayList<String>();
		for(String l : neConfigLines){
			String[] values = l.split("=");
			String value = values[1];
			if(value.contains("/")){
				String[] paths = value.split("\\s+");
				StringBuilder newValue = new StringBuilder();
				for(String path : paths){
					newValue.append(pathToWikifierFiles+"/"+path);
					newValue.append(" ");
				}
				StringBuilder newLine = new StringBuilder();
				newLine.append(values[0]);
				newLine.append("=");
				newLine.append(newValue.toString().trim());
				newNEConfigLines.add(newLine.toString());
			}
			else{
				newNEConfigLines.add(l);
			}
		}
		//write out new config parameters
		File newNEConfigFile = File.createTempFile("config.txt", ".tmp");
		newNEConfigFile.deleteOnExit();
		configFiles[1] = newNEConfigFile.getAbsolutePath();
		BufferedWriter neWriter = new BufferedWriter(new FileWriter(newNEConfigFile));
		for(String l : newNEConfigLines){
			neWriter.write(l+"\n");
		}
		neWriter.close();
		
		
		//read in old wordnet properties
		List<String> wordNetPropertiesLines = IOUtils.readLines(new FileInputStream(new File(pathToWikifierFiles + "/" + pathToDefaultJWNLConfigFile)));
		List<String> newWordNetPropertiesLines = new ArrayList<String>();
		String replacementString = pathToWikifierFiles +"/data/WordNet/";
		String stringToReplace = "data/WordNet/";
		for(String l : wordNetPropertiesLines){
			if(l.contains("dictionary_path")){
				newWordNetPropertiesLines.add(l.replace(stringToReplace, replacementString));
			}
			else{
				newWordNetPropertiesLines.add(l);
			}
		}
		File newWNConfigFile = File.createTempFile("jwnl_properties.xml",".tmp");
		newWNConfigFile.deleteOnExit();
		configFiles[2] = newWNConfigFile.getAbsolutePath();
		BufferedWriter wnWriter = new BufferedWriter(new FileWriter(newWNConfigFile));
		for(String l : newWordNetPropertiesLines){
			wnWriter.write(l+"\n");
		}
		wnWriter.close();
		
		return configFiles;

		

	}
	private static GlobalPaths setGlobalPaths(String prefix, String nerConfigFile, String neConfigFile, String wordNetConfigFile) {
		GlobalPaths gp = new GlobalPaths();
        gp.compressedRedirects = prefix+"/"+"data/WikiData/Redirects/2013-05-28.redirect";
        gp.protobufferAccessDir = prefix+"/"+"data/Lucene4Index/";
        gp.curatorCache = prefix+"/"+"data/TextAnnotationCache/";
        gp.wikiRelationIndexDir = prefix+"/"+"data/WikiData/Index/WikiRelation/";
        gp.models = prefix+"/"+"data/Models/TitleMatchPlusLexicalPlusCoherence/";
        gp.titleStringIndex = prefix+"/"+"data/WikiData/Index/TitleAndRedirects/";
        gp.wordnetConfig = wordNetConfigFile;
        gp.stopwords = prefix+"/"+"data/OtherData/stopwords_big";
        gp.wordNetDictionaryPath = prefix+"/"+"data/WordNet/";
        gp.nerConfig = nerConfigFile;
        gp.wikiSummary = null;
        gp.neSimPath = neConfigFile;
        return gp;
	}
	
	private static SettingManager getSettingManager(String pathToWikifierData, String nerConfigFile, String neConfigFile, String wordNetConfigFile){
		SettingManager sm = new SettingManager();
		sm.paths = setGlobalPaths(pathToWikifierData,nerConfigFile,neConfigFile,wordNetConfigFile);
		return sm;
	}
	
	public static String getWikifierOutput(LinkingProblem problem) {
	    StringBuilder res = new StringBuilder();

		for(Mention entity : problem.components){
			if(entity.topCandidate == null)
				continue;
			String escapedSurface = StringEscapeUtils.escapeXml(entity.surfaceForm.replace('\n', ' '));
			res.append(escapedSurface);
			res.append(" ");
			res.append(entity.charStart);
			res.append(":");
			res.append(entity.charStart + entity.charLength);
			res.append(" ");
			res.append(entity.topCandidate.titleName);
			res.append(" ");
			res.append(entity.linkerScore);
			res.append("\t");
		}
		return res.toString().trim();
	}
	
	public static class DistributeInputFormat extends FileLocationInputFormat{		
		/**
		 * custom getSplits should split a single input file into the number of splits
		 * equivalent to the number of nodes
		 */
		@Override
		public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException{
			
			List<InputSplit> splits = new ArrayList<>();
			JobClient jc= new JobClient(job);
			ClusterStatus cs = jc.getClusterStatus(true);
			Collection<String> trackerNames =  cs.getActiveTrackerNames();
			String[] nodeNames = new String[trackerNames.size()];
			nodeNames = trackerNames.toArray(nodeNames);

			
			//convert nodeNames to hostNames
			for(int i =0; i < nodeNames.length; i++){
				nodeNames[i] = nodeNames[i].split("_")[1].split(":")[0];
			}
				
			System.out.println("Node Names...");
			for(String nName: nodeNames){
				System.out.println(nName);
			}
							
			
			FileStatus[] fileStatuses = listStatus(job);
			FileStatus fileStatus = fileStatuses[0];
			Path p = fileStatus.getPath();
			long length = fileStatus.getLen();
			long blockSize = fileStatus.getBlockSize();
			long minSize = 0;
			int numNodes = trackerNames.size();
			long goalSize = Math.max((length/numNodes),(length/numSplits));
			
			long start = 0;
			long splitSize = computeSplitSize(goalSize,minSize,blockSize);
			int count =0;
			System.out.println("Size = " + length);
			
			long bytesRemaining = length;
			
			while ((double)bytesRemaining /splitSize > 1.1){
				System.out.println("New Split at " + nodeNames[count%nodeNames.length]);
				System.out.println("From " + (length-bytesRemaining) + " to " + (length-bytesRemaining+splitSize));
				splits.add(new FileSplit(p,length-bytesRemaining,splitSize,Arrays.copyOfRange(nodeNames, count%nodeNames.length, count%nodeNames.length+1)));
				bytesRemaining -= splitSize;
				count++;
			}
			
			if(bytesRemaining != 0){
				System.out.println("New Split at " + nodeNames[count%nodeNames.length]);
				System.out.println("From " + (length-bytesRemaining) + " to " + length);
				splits.add(new FileSplit(p,length-bytesRemaining,bytesRemaining,Arrays.copyOfRange(nodeNames, count%nodeNames.length, count%nodeNames.length+1)));
			}
			
//			while(start < length){
//				
//				long end = Math.min(start + computeSplitSize(goalSize,minSize,blockSize),length);
//
//				System.out.println("New Split at " + nodeNames[count%nodeNames.length]);
//				System.out.println("From " + start + " to " + end);
//				FileSplit fs = new FileSplit(p,start,end,(String[]) Arrays.copyOfRange(nodeNames, count%nodeNames.length, count%nodeNames.length+1));
//				splits.add(fs);
//				
//				count++;
//				start = end;
//				
//			}
			
			return splits.toArray(new InputSplit[splits.size()]);
		}
	}
}
