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
 *
 * Output format:
 *
 * your-bucket-name
 *      results/PageRank.inlink.out
 *      results/PageRank.n.out
 *      results/PageRank.iter1.out (output file for iteration 1)
 *      results/PageRank.iter8.out (output file for iteration 8)
 *      logs/ (the job log direcotry)
 *      job/PageRank.jar (job jar)
 *      tmp/ (temporary files) 
 */
public class PageRank 
{

    // Bucket that we will be operating in.
    String bucketName;
    // Input location for the Wikipedia XML dump.
    String XMLinputLocation;
    // Output for the parse Wikipedia XML dump.
    String XMLoutputLocation;

    PageRank(String bucketName)
    {
        // TODO Uncomment for submission.
        // this.bucketName = "s3n://" + bucketName;
        // this.XMLinputLocation = "s3://spring-2014-ds/data/enwiki-latest-pages-articles.xml";
        
        // TODO Delete before submission!
        this.bucketName = "/" + bucketName;
        this.XMLinputLocation = "/test/wiki-pages.xml";

        // Keep the file paths below.
        // Output for the parsed XML.
        this.XMLoutputLocation = this.bucketName + "/results/PageRank.inlink.out";

    }

    /**
     * Parses the Wikipedia XML input and outputs the link structure.
     *
     * Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
     */
    public class XMLMapper extends MapReduceBase implements 
        Mapper<LongWritable, Text, Text, Text> 
    {
        private Text word = new Text();

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable key, Text value, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) 
            {
                word.set(tokenizer.nextToken());
                output.collect(word, word);
            }
        }
    }

    /**
     * TODO Use this sample reduce function.
     * Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
     */
    public class Reduce extends MapReduceBase implements 
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

    /**
     * Parses the Wikipedia XML format.
     */
    public void parseXML() throws IOException
    {
        // Configuration for this job.
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("pagerank");

        // Input location.
        FileInputFormat.setInputPaths(conf, new Path(this.XMLinputLocation));

        // Class to parse XML
        conf.setInputFormat(XmlInputFormat.class);
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

         // Mapper class to parse XML.
        conf.setMapperClass(XMLMapper.class);
 
        // Output configuration.
        FileOutputFormat.setOutputPath(conf, new Path(XMLoutputLocation));
        conf.setOutputFormat(TextOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        JobClient.runJob(conf);
    }

    /**
     * Run the PageRank algorithm.
     *
     * arg[0] = bucket name.
     */
    public static void main(String[] args) throws Exception 
    {
        // Validate args length.
        if (args.length != 2)
        {
            System.out.println("Usage java PageRank inputPath outputPath");
        }

        PageRank pagerank = new PageRank(args[0]);
        
        // Parse the XML to map input links to output links.
        pagerank.parseXML();

        // TODO Count number of pages.
        // TODO Compute PageRank.
        // TODO Sort PageRank.


    }
}

