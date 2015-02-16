package main.pack;

import helper.pack.WordPairMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MapStripeAglorithm extends Mapper<LongWritable, Text, Text, WordPairMap> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		List<String> itemIds = Arrays.asList(value.toString().trim().split(" "));	
		for(int k=0; k<itemIds.size();k++){
			WordPairMap wordPairMap = null;
			//Get the all neighbors for the current item Id
			List<String> neighbors = getNeighbors(k, itemIds) ;
			if(!neighbors.isEmpty()) wordPairMap= new WordPairMap();
			for(String neighbor : neighbors){
				if(wordPairMap.containsKey(new Text(neighbor)))
					wordPairMap.increment(new Text(neighbor));
				else{
					wordPairMap.put(new Text(neighbor), new IntWritable(1));
				}
			}
			if(wordPairMap!=null){
				System.out.println("Mapper- Item: "+itemIds.get(k)+" wordPairMap: "+wordPairMap);
				context.write(new Text(itemIds.get(k)), wordPairMap);
			}
		}
		
	}

	private List<String> getNeighbors(Integer pos, List<String> items){
		List<String> neighbors = new ArrayList<String>();
		if(pos == items.size()-1) return neighbors;
		for(int i =pos+1; i< items.size(); i++){
			if(items.get(i).equals(items.get(pos))) break;
			else neighbors.add(items.get(i));
		}
		return neighbors;
	}

}
