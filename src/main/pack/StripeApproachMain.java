package main.pack;

import java.io.IOException;

import helper.pack.WordPairMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StripeApproachMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
//		if(args.length!=2){
//			System.out.println("usage: [input] [output]");
//			System.exit(-1);
//			}
			System.out.println("========Stripe Algorithm Approach=========");
			Job job = Job.getInstance(new Configuration());
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			job.setMapperClass(MapStripeAglorithm.class);
			job.setReducerClass(RecuderStripeAlgrithm.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(WordPairMap.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
		
			Path outDir = new Path("files/output");
			FileSystem.get(job.getConfiguration()).delete(outDir,true);
	        FileInputFormat.setInputPaths(job, new Path("files/input"));
	        FileOutputFormat.setOutputPath(job, outDir);
	        
//	    	FileInputFormat.setInputPaths(job, new Path(args[0]));
//	      	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	        job.setJarByClass(StripeApproachMain.class);
	        job.setJobName("Stripes Algorithm Approach");
	        
	        System.exit(job.waitForCompletion(true)?0:1);		
	}

}
