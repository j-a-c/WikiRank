package PageRank;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import PageRank.XmlInputFormat;

/**
 * CIS4930IDS Project 1.
 * Computes the PageRank for the Wikipedia XML format.
 */
public class PageRank 
{

    // Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
    public static class Map extends MapReduceBase implements 
        Mapper<LongWritable, Text, Text, IntWritable> 
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable key, Text value, 
                OutputCollector<Text, IntWritable> output, 
                Reporter reporter) throws IOException 
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) 
            {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    // Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
    public static class Reduce extends MapReduceBase implements 
        Reducer<Text, IntWritable, Text, IntWritable> 
    {
        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(Text key, Iterator<IntWritable> values, 
                OutputCollector<Text, IntWritable> output, 
                Reporter reporter) throws IOException 
        {
            int sum = 0;
            while (values.hasNext())
            {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    /*
     * Parses the Wikipedia XML format.
     */
    public void parseXML(String inputPath) throws IOException
    {
        // Configuration for this job.
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("pagerank");

        // Input location.
        FileInputFormat.setInputPaths(conf, new Path(inputPath));

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        // Class to parse XML
        conf.setInputFormat(XmlInputFormat.class);
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        // Mapper
        conf.setMapperClass(Map.class);
        // Combiner
        conf.setCombinerClass(Reduce.class);
        // Reducer
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // TODO Set output path.
        FileOutputFormat.setOutputPath(conf, new Path("TODO"));

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
    }

    /*
     * Run the PageRank algorithm.
     *
     * arg[0] = input location.
     * arg[1] = output location.
     */
    public static void main(String[] args) throws Exception 
    {
        // Validate args length.
        if (args.length != 2)
        {
            System.out.println("Usage java PageRank inputPath outputPath");
        }

        PageRank pagerank = new PageRank();
        
        // Parse the XML to map input links to output links.
        pagerank.parseXML(args[0]);

        // TODO Count number of pages.
        // TODO Compute PageRank.
        // TODO Sort PageRank.


    }
}

