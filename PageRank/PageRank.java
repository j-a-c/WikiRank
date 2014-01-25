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
    // Output for the parsed Wikipedia XML dump.
    String XMLoutputLocation;
    // Input location for the job that counts the number of pages.
    String CountInputLocation;
    // Output location for the job that counts the number of pages.
    String CountOutputLocation;

    /**
     * Constructor for a PageRank job.
     * Configures the various input and output locations using the given
     * bucket name.
     */
    public PageRank(String bucketName)
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
        // Input and output location for the count job.
        this.CountInputLocation = this.XMLoutputLocation;
        this.CountOutputLocation = this.bucketName + "/results/PageRank.n.out";
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

        // Match [a] and [a|b], in both cases returning 'a'.
        private static Pattern pattern = Pattern.compile("\\[\\[([^\\]|]*)[^\\]]*\\]");

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text xml, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            // Parse the page title. This will be the key for our output.
            int titleStart = xml.find("<title>");
            int titleEnd = xml.find("</title>", titleStart);
            titleStart += 7; // Get outside of tag.

            // Remove all spaces from the title.
            String title = Text.decode(xml.getBytes(), titleStart,
                    titleEnd-titleStart).replace(' ', '_');

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
            
            // Find the links.
            Matcher matcher = pattern.matcher(body);

            // Holds the unique links found on this page.
            Set<String> uniqueLinks = new HashSet<String>();

            // Find the unique links, and replace spaces with underscores.
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
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // Output type.
        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);
    }

    /**
     * Maps each page to <1,PageTitle>.
     * This mapper will be receiving the output produced from parseXML();
     * The input will be of the form "pageTitle link1 link2...".
     */
    public static class CountMapper extends MapReduceBase implements 
        Mapper<LongWritable, Text, IntWritable, IntWritable> 
    {
        // We will use this to group all the pages.
        private final static IntWritable one = new IntWritable(1);

        public CountMapper(){}

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text pageStats, 
                OutputCollector<IntWritable, IntWritable> output, 
                Reporter reporter) throws IOException 
        {
            // The value will be the page title.
            //Text outVal = new Text(pageStats.toString().split("\\s+")[0]);
            output.collect(one, one);
        }
    }

   /**
     * Counts the number of values (pages) this mapper has seen.
     * Receives the output of CountMapper.
     * Now, there shuffle/sort step will be smaller and the reducer can just
     * add a couple values.
     *
     * Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
     */
    public static class CountCombiner extends MapReduceBase implements
        Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
    {
        // We will use this to group all the pages at the reducer.
        private final static IntWritable one = new IntWritable(1);

        public CountCombiner(){}

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(IntWritable key, Iterator<IntWritable> values, 
                OutputCollector<IntWritable, IntWritable> output, 
                Reporter reporter) throws IOException 
        {
            // Count all the links this mapper has seen.
            int count = 0;
            while (values.hasNext())
            {
                count++;
                values.next();
            }

            output.collect(one, new IntWritable(count)); 
        }
    }


    /**
     * Counts the number of values (pages).
     * Receives the output of CountCombiner.
     *
     * The keys received will be <1, numPages>; we just need to add all the
     * numPages together to get the total number of pages.
     *
     * Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
     */
    public static class CountReducer extends MapReduceBase implements
        Reducer<IntWritable, IntWritable, Text, Text> 
    {
        public CountReducer(){}

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(IntWritable key, Iterator<IntWritable> values, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            // Count all the links.
            int count = 0;
            while (values.hasNext())
            {
                count += values.next().get();
            }

            Text outKey = new Text("N="+count);
            Text outVal = new Text("");

            output.collect(outKey, outVal); 
        }
    }

    /**
     * Counts the number of Wikipedia documents.
     * This job has one reduce step.
     */
    public void countPages() throws IOException
    {
        // Configuration for this job.
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("PageRankCountPages");

        // Input location.
        FileInputFormat.setInputPaths(conf, new Path(this.CountInputLocation));

        // Input type.
        conf.setInputFormat(TextInputFormat.class);

        // MApper class.
        conf.setMapperClass(CountMapper.class);
        // Combiner for the mapper.
        conf.setCombinerClass(CountCombiner.class);

        // Let us split the file into small pieces.
        //conf.setNumMapTasks(10);

        // We will only have one reducer so we can count all the pages.
        conf.setNumReduceTasks(1);
        conf.setReducerClass(CountReducer.class);
 
        // Output configuration.
        FileOutputFormat.setOutputPath(conf, 
                new Path(this.CountOutputLocation));
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        // Output type.
        conf.setOutputFormat(TextOutputFormat.class);

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
            return;
        }

        PageRank pagerank = new PageRank(args[0]);
        
        // Parse the XML to map input links to output links.
        pagerank.parseXML();

        // TODO Count number of pages.
        pagerank.countPages();

        // TODO Compute PageRank.
        // TODO Sort PageRank.


    }
}

