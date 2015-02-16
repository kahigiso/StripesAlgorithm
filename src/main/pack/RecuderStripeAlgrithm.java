package main.pack;

import helper.pack.WordPairMap;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class RecuderStripeAlgrithm extends Reducer<Text, WordPairMap, Text, WordPairMap> {
	
	public void reduce(Text _key, Iterable<WordPairMap> values, Context context) throws IOException, InterruptedException {
		
		WordPairMap mp = new WordPairMap();
		Integer marginal = 0;
		for (WordPairMap wpMap : values) {
			for(Writable key: wpMap.keySet()){
				if(mp.containsKey(key)){
					mp.increment(key,((IntWritable)wpMap.get(key)).get());
					marginal += ((IntWritable)wpMap.get(key)).get();
				}else{
					mp.put(key, wpMap.get(key));
					marginal++;
				}
			}
			
		}
		for(Writable key: mp.keySet()){
			DoubleWritable doub = new DoubleWritable((double)((IntWritable)mp.get(key)).get()/marginal);
			mp.put(key, doub);
		}
		System.out.println("Reducer - Item: "+_key+" wordPairMap: "+mp);
		context.write(_key,mp);	
	}

}
