package PageRank;

import java.io.IOException;
import java.lang.StringBuilder;
import java.util.Iterator;
import java.util.regex.*;
import java.util.HashSet;
import java.util.Set;

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
        this.XMLinputLocation = "/wiki-pages.xml";

        // Keep the file paths below.
        // Output for the parsed XML.
        this.XMLoutputLocation = this.bucketName + "/results/PageRank.inlink.out";
    }

    /**
     * Parses the Wikipedia XML input and outputs the link structure.
     *
     * Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
     */
    public static class XMLMapper extends MapReduceBase implements 
        Mapper<LongWritable, Text, Text, Text> 
    {
        public XMLMapper(){}

        // Key and values to be output.
        private Text key = new Text();
        private Text value = new Text();

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text xml, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            // Parse the page title. This will be the key for our output.
            int titleStart = xml.find("<title>");
            int titleEnd = xml.find("</title>", titleStart);
            titleStart += 7; // Get outside of tag.

            String title = Text.decode(xml.getBytes(), titleStart,
                    titleEnd-titleStart);

            // Parse text body. This is where we will search for links.
            int bodyStart = xml.find("<text");
            // <text ...> may contain some fields.
            bodyStart = xml.find(">", bodyStart); 
            int bodyEnd = xml.find("</text>", bodyStart);
            bodyStart += 1; // Get outside of tag.

            // If there is no <text ...>...</text>.
            if(bodyStart == -1 || bodyEnd == -1)
                return;
            
            String body = Text.decode(xml.getBytes(), bodyStart, 
                    bodyEnd-bodyStart);
            
            // Match [a] and [a|b], in both cases returning 'a'.
            Pattern pattern = Pattern.compile("\\[([^\\]|]*)[^\\]]*\\]");
            Matcher matcher = pattern.matcher(body);

            // Holds the unique links found on this page.
            Set<String> uniqueLinks = new HashSet<String>();

            // Find the new links, and replace spaces with underscores.
            while(matcher.find())
                uniqueLinks.add(matcher.group(1).replace(' ', '_'));

            // This will hold the links the we will output.
            StringBuilder outLinks = new StringBuilder();
            for (String link : uniqueLinks)
            {
                outLinks.append(link);
                outLinks.append(' ');
            }
            
            // Set our key and value.
            key.set(title);
            value.set(outLinks.toString());

            //key.set, value.set
            output.collect(key, value);
        }
    }

    /**
     * TODO Use this sample reduce function later.
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
     * This has Job has no Reduce step.
     */
    public void parseXML() throws IOException
    {
        // Configuration for this job.
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("PageRankParseXML");

        // Input location.
        FileInputFormat.setInputPaths(conf, new Path(this.XMLinputLocation));

        // Class to parse XML
        conf.setInputFormat(XmlInputFormat.class);
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

         // Mapper class to parse XML.
        conf.setMapperClass(XMLMapper.class);

        // We will not use a reducer for this task to avoid the sorting and
        // shuffling.
        conf.setNumReduceTasks(0);
 
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
        if (args.length != 1)
        {
            System.out.println("Usage java PageRank bucketName");
            System.out.println(args[0]);
            return;
        }

        PageRank pagerank = new PageRank(args[0]);
        
        // Parse the XML to map input links to output links.
        pagerank.parseXML();

        // TODO Count number of pages.
        // TODO Compute PageRank.
        // TODO Sort PageRank.


    }
}

