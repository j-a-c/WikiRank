package PageRank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.StringBuilder;
import java.util.Iterator;
import java.util.regex.*;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
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
    // The number of iterations to run the PageRank algorithm.
    private static final int NUM_PAGERANK_ITERS = 2;

    // The number of pages total in the PageRank matrix.
    // Needs to be set before calculating the PageRank.
    private static int NUM_PAGES_TOTAL;
    // This is the start index of NUM_PAGES_TOTAL in the file.
    // See CountReducer for the output format.
    private static final int NUM_PAGES_TOTAL_START = 2; 

    // Residual probablility and 1 - residual probablility.
    private static final double DELTA = 0.85;
    private static final double ONEMINUSDELTA = 1.0 - DELTA;

    // Whitespace tokenizer.
    private static final String WHITESPACE = "\\s+";

    // Bucket that we will be operating in.
    private String bucketName;
    // Input location for the Wikipedia XML dump.
    private String XMLinputLocation;
    // Output for the parsed Wikipedia XML dump.
    private String XMLoutputLocation;
    // Input location for the job that counts the number of pages.
    private String CountInputLocation;
    // Output location for the job that counts the number of pages.
    private String CountOutputLocation;
    // Temporary matrix output location.
    private String tempMatrixOutput;

    /**
     * Constructor for a PageRank job.
     * Configures the various input and output locations using the given
     * bucket name.
     */
    public PageRank(String bucketName)
    {
        // TODO Uncomment for submission. And test.
        // this.bucketName = "s3n://" + bucketName;
        // this.XMLinputLocation = "s3://spring-2014-ds/data/enwiki-latest-pages-articles.xml";
        
        // TODO Delete the two below here before submission!
        this.bucketName = "hdfs://localhost:54310/" + bucketName;
        this.XMLinputLocation = "/wiki-pages.xml";

        // Keep the file paths below.
        // Output for the parsed XML.
        this.XMLoutputLocation = this.bucketName + "/results/PageRank.inlink.out";
        // Input and output location for the count job.
        this.CountInputLocation = this.XMLoutputLocation;
        this.CountOutputLocation = this.bucketName + "/results/PageRank.n.out/part";
        // Temporary matrix input and output locations.
        this.tempMatrixOutput = this.bucketName + "/tmp/matrixOut";
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
     * We split the input by whitespace and output each token.
     */
    public static class CountMapper extends MapReduceBase implements 
        Mapper<LongWritable, Text, IntWritable, Text> 
    {
        // We will use this to group all the pages.
        private final static IntWritable one = new IntWritable(1);

        private Text outVal = new Text();

        public CountMapper(){}

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text pageStats, 
                OutputCollector<IntWritable, Text> output, 
                Reporter reporter) throws IOException 
        {
            for (String page : pageStats.toString().split(WHITESPACE))
            {
                outVal.set(page);
                output.collect(one, outVal);
            }
        }
    }

   /**
     * Only outputs the unique links from the CountMapper.
     * Receives the output of CountMapper.
     * Now, there shuffle/sort step will be smaller. 
     *
     * Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
     */
    public static class CountCombiner extends MapReduceBase implements
        Reducer<IntWritable, Text, IntWritable, Text> 
    {
        // We will use this to group all the pages at the reducer.
        private final static IntWritable one = new IntWritable(1);

        private Text outVal = new Text();

        public CountCombiner(){}

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(IntWritable key, Iterator<Text> values, 
                OutputCollector<IntWritable, Text> output, 
                Reporter reporter) throws IOException 
        {
            HashSet<String> uniques = new HashSet<String>();
            while (values.hasNext())
            {
                uniques.add(values.next().toString());
            }
    
            for (String unique : uniques)
            {
                outVal.set(unique);
                output.collect(one, outVal); 
            }
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
        Reducer<IntWritable, Text, Text, Text> 
    {
        private Text outKey = new Text();
        private final Text outVal = new Text("");

        public CountReducer(){}

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(IntWritable key, Iterator<Text> values, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {

            HashSet<String> uniques = new HashSet<String>();
            while (values.hasNext())
            {
                uniques.add(values.next().toString());
            }

            outKey.set("N="+uniques.size());
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
     * For each "page OutgoingLinksText" produces the following output:
     *      <outLink, contribution>
     *          This output can be used to calculate the PageRank of outLink in
     *          the Reducer.
     *      <in, '! origLinks'>.
     *          This output can be used to calculate the new PageRank matrix
     *          (once the PageRank for the in page is calculated). The first
     *          character of input value to the reducer will be '!' so the
     *          reducer can distinguish between the two output formats.
     *
     *  The first iteration will not have a PageRank associated with it, so we
     *  need to distinguish it using firstIteration. We will assign PageRank =
     *  1.0 for all pages. The first iteration receives inputs as follows:
     *      pageTitle outlink1 outlink2...
     */
    public static class PageRankMapper extends MapReduceBase implements 
        Mapper<LongWritable, Text, Text, Text> 
    {
        public static boolean firstIteration = true;

        private Text key = new Text();
        private Text value = new Text();

        public PageRankMapper(){}

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text pageStats, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            String[] tokens = pageStats.toString().split(WHITESPACE);

            // Will be used for the second type of output.
            StringBuilder builder = new StringBuilder();
            builder.append('!');
            builder.append(' ');

            // Output the first type of output.
            if (firstIteration)
            {
                // Calculate the PageRank contribution from the parent.
                int numLinks = tokens.length - 1;
                double parentPR = 1.0;
                String contribution = String.valueOf(parentPR / numLinks);

                for (int index = 1; index < tokens.length; index++)
                {
                    key.set(tokens[index]);
                    value.set(contribution);
                    output.collect(key, value);

                    builder.append(tokens[index]);
                    builder.append(' ');
                }

                // Output the second type of output
                key.set(tokens[0]);     
                value.set(builder.toString());
                output.collect(key, value);

            }
            else // We are receiving an input from the PageRank matrix.
            {
                int numLinks = tokens.length - 2;
                // The first token will be the parents PageRank.
                double parentPR = Double.parseDouble(tokens[1]);
                String contribution = String.valueOf(parentPR / numLinks);

                for (int index = 2; index < tokens.length; index++)
                {
                    key.set(tokens[index]);
                    value.set(contribution);
                    output.collect(key, value);

                    builder.append(tokens[index]);
                    builder.append(' ');
                }

                // Output the second type of output
                key.set(tokens[0]);     
                value.set(builder.toString());
                output.collect(key, value);
            }

        }
    }

    /**
     * Computes the final PageRank. The PageRankMapper will send two types of
     * output; their uses are shown below:
     *      <outLink, contribution>
     *          Will be used to compute the PageRank. These are partial
     *          PageRanks computed from a parent page.
     *      <in, '! origLinks'>.
     *          Replace the '!' with the complete PageRank.
     */
    public static class PageRankReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> 
    {
        public PageRankReducer(){}

        private Text outKey = new Text();
        private final Text outVal = new Text("");

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(Text key, Iterator<Text> values, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            // The matrix row. 
            String matrixString = null;

            // The final PageRank (not fully computed at this point).
            double finalPR = (ONEMINUSDELTA) / NUM_PAGES_TOTAL;
    
            double tempPR = 0.0;
            while(values.hasNext())
            {
                String value = values.next().toString();

                if (value.startsWith("!"))
                    matrixString = value;
                else
                    tempPR += Double.parseDouble(value);
            }
            
            finalPR = finalPR + (DELTA * tempPR);

            if (matrixString == null)
            {
                matrixString = key + " " + finalPR;
            }
            else
            {
                matrixString = matrixString.replace("!", String.valueOf(finalPR));
            }

            outKey.set(matrixString);
            output.collect(outKey, outVal);
        }
    }

    /**
     * Calculates the PageRank of the Wikipedia pages.
     */
    public void calculatePageRank() throws IOException
    {
        // Set the total number of links.
        try
        {
            Path pt = new Path(this.CountOutputLocation + "/part-00000");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = br.readLine();
            line = line.substring(NUM_PAGES_TOTAL_START);
            line = line.split(WHITESPACE)[0];
            this.NUM_PAGES_TOTAL = Integer.parseInt(line);
        }
        catch(Exception e)
        {
            System.out.println("Error: ");
            System.out.println(e);
            return;
        }

        // Run the PageRank calulation NUM_PAGERANK_ITERS times.
        // The index starts from 1 just for naming purposes.
        for (int i = 1; i <= NUM_PAGERANK_ITERS; i++)
        {
            // Configuration for this job.
            JobConf conf = new JobConf(PageRank.class);
            conf.setJobName("PageRankIteration" + i);

            if (i == 1)
            {
                // Input location. On the first iteration, we will read the output
                // from the XML mapper.
                FileInputFormat.setInputPaths(conf, new Path(this.XMLoutputLocation));
            }
            else
            {
                // For the other iterations, we will read from the matrix file.
                FileInputFormat.setInputPaths(conf, new Path(this.tempMatrixOutput + (i-1)));
            }

            // Input type.
            conf.setInputFormat(TextInputFormat.class);

            // Mapper class.
            conf.setMapperClass(PageRankMapper.class);

            // Reducer class.
            conf.setReducerClass(PageRankReducer.class);
     
            // Output configuration.
            FileOutputFormat.setOutputPath(conf,  new Path(this.tempMatrixOutput + i));
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            // Output type.
            conf.setOutputFormat(TextOutputFormat.class);

            JobClient.runJob(conf);
            
            // Set the first iteration marker to false.
            PageRankMapper.firstIteration = false;
        }

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

        // Count number of pages.
        pagerank.countPages();

        // Compute PageRank.
        pagerank.calculatePageRank();

        // TODO Sort PageRank.

    }
}

