package PageRank;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.StringBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.regex.*;
import java.util.HashSet;
import java.util.Set;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.FileUtil;
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
 *      results/PageRank.outlink.out
 *      results/PageRank.n.out
 *      results/PageRank.iter1.out (output file for iteration 1)
 *      results/PageRank.iterN.out (output file for iteration N)
 *      logs/ (the job log direcotry)
 *      job/PageRank.jar (job jar)
 *      tmp/ (temporary files) 
 */
public class PageRank 
{
    // Are we in debug mode?
    private static final boolean DEBUG = false;

    // Should we delete temporary files?
    private static final boolean DELETETEMP = false;

    // The number of iterations to run the PageRank algorithm.
    private static int NUM_PAGERANK_ITERS;

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
    // Marker
    private static final String MARKER = "!";

    // Bucket that we will be operating in.
    private String bucketName;
    // Input location for the Wikipedia XML dump.
    private String XMLinputLocation;
    // Temporary output for creating the outlink graph.
    private String XMLtempOutputLocation;
    // Output for the parsed Wikipedia XML dump.
    private String XMLoutputLocation;
    // Input location for the job that counts the number of pages.
    private String CountInputLocation;
    // Output location for the job that counts the number of pages.
    private String CountOutputLocation;
    // The final output location.
    private String finalCountOutput;
    // Temporary matrix output location.
    private String tempMatrixOutput;
    // Output for the sorted PageRanks.
    private String sortOutput;

    /**
     * Constructor for a PageRank job.
     * Configures the various input and output locations using the given
     * bucket name.
     */
    public PageRank(String bucketName)
    {
        // Some mode specific parameters.
        if (!DEBUG)
        {
            this.bucketName = "s3n://" + bucketName;
            this.XMLinputLocation = "s3n://spring-2014-ds/data/enwiki-latest-pages-articles.xml";
            this.NUM_PAGERANK_ITERS = 8;
        }
        else
        {
            this.bucketName = "hdfs://localhost:54310/" + bucketName;
            this.XMLinputLocation = "/wiki-pages.xml";
            this.NUM_PAGERANK_ITERS = 2;
        }

        // Keep the file paths below.

        this.XMLtempOutputLocation = this.bucketName + "/tmp/outlink.temp";
        // Output for the parsed XML.
        this.XMLoutputLocation = this.bucketName + "/tmp/PageRank.outlink.out";
        // Input and output location for the count job.
        this.CountInputLocation = this.XMLoutputLocation;
        this.CountOutputLocation = this.bucketName + "/tmp/PageRank.n.out";
        this.finalCountOutput = this.bucketName + "/results/PageRank.n.out";
        // Temporary matrix input and output locations.
        this.tempMatrixOutput = this.bucketName + "/tmp/matrixOut";
        // Output for the sorted PageRanks.
        this.sortOutput = this.bucketName + "/tmp/";
    }

    /**
     * Parses the Wikipedia XML input and outputs the link structure.
     *
     * For each page, output inlinks and a marker that the page exists.
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
        // Will not match [a:sd|f].
        //private static Pattern pattern = Pattern.compile("\\[\\[([^\\]|#:]*)[^\\]]*\\]");
        // Returns whatever is in [[...]]. We still need to post-process this
        // string.
        private static Pattern pattern = Pattern.compile("\\[\\[([^\\]]*)\\]");

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text xml, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            // Parse the page title. This will be the key for our output.
            int titleStart = xml.find("<title>");
            int titleEnd = xml.find("</title>", titleStart);
            titleStart += 7; // Get outside of tag.

            // Remove all spaces from the title and set it as the key.
            String title = Text.decode(xml.getBytes(), titleStart,
                    titleEnd-titleStart).replace(' ', '_');
            value.set(title);

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

            // Output <currPage, outlink>
            while(matcher.find())
            {
                // Raw link, we will need to post-process this.
                String outlink = matcher.group(1).replace(' ', '_');

                // Remove the pipe if it exists in the link.
                int pipeIndex = outlink.indexOf('|');
                if (pipeIndex != -1)
                    outlink = outlink.substring(0, pipeIndex);

                // Reject link if it contains ':' or '#'.
                if (outlink.contains(":") || outlink.contains("#"))
                    return;

                // Do not count self-referential links.
                // We are outputting the inlink graph.
                if (!outlink.equals(title))
                {
                    key.set(outlink);
                    output.collect(key, value);
                }
            }

            // Output a market that this page exists.
            key.set(title);
            value.set(MARKER);
            output.collect(key, value);
        }
    }

    /**
     * Outputs a partial outlink graph if the page exists.
     * If the page has no outgoing links, it will not be output.
     *
     * Output format is (child, parents...).
     *
     * Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
     */
    public static class XMLReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> 
    {
        private static final char SPACE = ' ';

        private Text outValue = new Text();

        public XMLReducer(){}

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(Text key, Iterator<Text> values, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            // The page does not exist until we find the marker.
            boolean exists = false;

            StringBuilder builder = new StringBuilder();

            // Add all parent links to the value string.
            // If the MARKER is the first entry, the next entry will be
            // double-spaced, but that is ok since we split using WHITESPACE.
            if (values.hasNext())
            {
                String value = values.next().toString();
                if (value.equals(MARKER))
                    exists = true;
                else
                    builder.append(value);
            }
            while (values.hasNext())
            {
                String value = values.next().toString();
            
                if (value.equals(MARKER))
                    exists = true;
                else
                {
                    builder.append(SPACE);
                    builder.append(value);
                }
            }
            
            // Only output if the page exists.
            if (exists)
            {
                outValue.set(builder.toString());
                output.collect(key, outValue); 
            }
        }
    }

    /**
     * Outputs pairs of (parent, child) links. These links definitely exist
     * since they come from the previous job. The input is (child parents...).
     * The input is an inlink graph. We are transforming it into an outlink graph.
     */
    public static class GraphMapper extends MapReduceBase implements 
        Mapper<LongWritable, Text, Text, Text> 
    {
        public GraphMapper(){}

        // Key and values to be output.
        private Text key = new Text();
        private Text value = new Text();

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text text, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            String[] tokens = text.toString().split(WHITESPACE);

            // Set the child link.
            value.set(tokens[0]);

            // Output (parent, child). Skip the first entry in the array
            // because it is the child link. 
            for (int i = 1; i < tokens.length; i++)
            {
                key.set(tokens[i]);
                output.collect(key, value);
            }
            
            // Output (child, "") so we can still build pages that don't have
            // outgoing links.
            key.set("");
            output.collect(value, key);
        }
    }

    /**
     * Creates the outlink graph given parts of the outlink graph row.
     */
    public static class GraphReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> 
    {
        private static final char SPACE = ' ';

        private Text outValue = new Text();

        public GraphReducer(){}

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(Text key, Iterator<Text> values, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {

            StringBuilder builder = new StringBuilder();

            if (values.hasNext())
            {
                builder.append(values.next().toString());
            }
            while (values.hasNext())
            {
                builder.append(SPACE);
                builder.append(values.next().toString());
            }

            outValue.set(builder.toString());
            output.collect(key, outValue);
        }
    }

    /**
     * Parses the Wikipedia XML format.
     *
     * Contains two jobs.
     * The first creates a partial link structure for pages that exists. (i.e.
     * removes the 'red links').
     * The second job merges the link structure to create the whole graph.
     */
    public void parseXML() throws IOException
    {
        // Configuration for the first job.
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("PageRankParseXML");

        // Input location.
        FileInputFormat.setInputPaths(conf, new Path(this.XMLinputLocation));

        // Class to parse XML
        conf.setInputFormat(XmlInputFormat.class);
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

         // Set classes to parse XML.
        conf.setMapperClass(XMLMapper.class);
        conf.setReducerClass(XMLReducer.class);
 
        // Output configuration.
        FileOutputFormat.setOutputPath(conf, new Path(this.XMLtempOutputLocation));
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // Output type.
        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);

        // Configuration for the second job.
        conf = new JobConf(PageRank.class);
        conf.setJobName("PageRankCreateOutlinkGraph");

        // Input location.
        FileInputFormat.setInputPaths(conf, new Path(this.XMLtempOutputLocation));

        // Input type.
        conf.setInputFormat(TextInputFormat.class);

         // Set classes to parse XML.
        conf.setMapperClass(GraphMapper.class);
        conf.setCombinerClass(GraphReducer.class);
        conf.setReducerClass(GraphReducer.class);
 
        // Output configuration.
        FileOutputFormat.setOutputPath(conf, new Path(this.XMLoutputLocation));
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // Output type.
        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);

    }

    /**
     * Maps each page to <1,1>.
     * This mapper will be receiving the output produced from parseXML();
     * The input will be of the form "pageTitle link1 link2...".
     * We produce a token for each pageTitle.
     */
    public static class CountMapper extends MapReduceBase implements 
        Mapper<LongWritable, Text, IntWritable, IntWritable> 
    {
        // We will use this to group all the pages.
        private final static IntWritable one = new IntWritable(1);

        private Text outVal = new Text();

        public CountMapper(){}

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text pageStats, 
                OutputCollector<IntWritable, IntWritable> output, 
                Reporter reporter) throws IOException 
        {
            output.collect(one, one);
        }
    }

   /**
    * Adds the output from this mapper.
    * Receives the output of CountMapper.
    * Now, there shuffle/sort step will be smaller. 
    * Outputs <1, totalPagesFromMapper>.
    *
    * Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
    */
    public static class CountCombiner extends MapReduceBase implements
        Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
    {
        // We will use this to group all the pages at the reducer.
        private final static IntWritable one = new IntWritable(1);

        private IntWritable outVal = new IntWritable();

        public CountCombiner(){}

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(IntWritable key, Iterator<IntWritable> values, 
                OutputCollector<IntWritable, IntWritable> output, 
                Reporter reporter) throws IOException 
        {
            int count = 0;
            while (values.hasNext())
            {
                count++;
                values.next();
            }

            outVal.set(count);
            output.collect(one, outVal);
    
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
        private Text outKey = new Text();
        private final Text outVal = new Text("");

        public CountReducer(){}

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(IntWritable key, Iterator<IntWritable> values, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {

            int count = 0;
            while (values.hasNext())
            {
                count += values.next().get();
            }

            outKey.set("N="+count);
            output.collect(outKey, outVal); 
        }
    }

    /**
     * Counts the number of Wikipedia documents.
     * This job has one reduce step.
     * The total number of pages is the total number of pages found from
     * parsing the <title></title> tags.
     */
    public void countPages() throws IOException, URISyntaxException
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

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        // We will only have one reducer so we can count all the pages.
        conf.setNumReduceTasks(1);
        conf.setReducerClass(CountReducer.class);
 
        // Output configuration.
        FileOutputFormat.setOutputPath(conf, 
                new Path(this.CountOutputLocation));

        // Output type.
        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);

        // We will merge and copy this output here since we will need it to
        // calculate the PageRank. (PageRank.n.out)
        Configuration config = new Configuration();

        // If we are running in AWS, we need to configure the FileSystem.
        FileSystem fs;
        if (DEBUG)
            fs = FileSystem.get(config);
        else
            fs = FileSystem.get(new URI(this.bucketName), config);

        Path src = new Path(this.CountOutputLocation);
        Path dst = new Path(this.finalCountOutput);
        FileUtil.copyMerge(fs, src, fs, dst, DELETETEMP, config, "");

    }

    /**
     * For each "page OutgoingLinks..." produces the following output:
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
                double parentPR = 1.0 / NUM_PAGES_TOTAL;
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
                    matrixString = key + " " + value;
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
            Path pt = new Path(this.finalCountOutput);
            // We need to get the correct FileSystem.
            Configuration config = new Configuration();
            FileSystem fs;
            if (DEBUG)
                fs = FileSystem.get(config);
            else
                fs = FileSystem.get(new URI(this.bucketName), config);

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
     * Mapper for the sort job.
     * Input is 'page PageRank outLinks...'
     * We only output values that are greater than the specified threshold.
     *
     * Output ("page PageRank", "page PageRank")
     */
    public static class SortMapper extends MapReduceBase implements 
        Mapper<LongWritable, Text, Text, Text> 
    {
        public SortMapper(){}

        private static final double threshold = 5.0 / NUM_PAGES_TOTAL;

        private Text key = new Text();
        private static final Text value = new Text("");

        // map(key, value, OutputCollector<KeyOut,ValueOut>)
        public void map(LongWritable keyIn, Text line, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            String[] tokens = line.toString().split(" ");

            if (Double.parseDouble(tokens[1]) >= threshold)
            {
                key.set(tokens[0] + "\t" + tokens[1]);
                value.set(tokens[0] + "\t" + tokens[1]);
                output.collect(key, value);
            }

        }
    }

    /**
     * Marks all inputs as equal.
     */
    public static class EqualComparator implements RawComparator<Text>
    {
        public EqualComparator(){}
            
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            return 0;
        }

        @Override
        public int compare(Text t1, Text t2)
        {
            return 0;
        }
    }


    /**
     * A comparator to sort the reduce key before they given to the
     * SortReducer.
     *
     * t1 and t2 are Texts containing "page PageRank".
     * PageRank will be sorted in descending order.
     */
    public static class SortComparator implements RawComparator<Text>
    {
        public SortComparator(){}
        
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {

            // Text contains an extra byte.
            String str1 = new String(Arrays.copyOfRange(b1, s1+1, s1+l1));
            String str2 = new String(Arrays.copyOfRange(b2, s2+1, s2+l2));

            Double d1 = Double.parseDouble(str1.split(WHITESPACE)[1]);
            Double d2 = Double.parseDouble(str2.split(WHITESPACE)[1]);

            return -1 * d1.compareTo(d2);
        }

        @Override
        public int compare(Text t1, Text t2)
        {
            return -1 * new Double(Double.parseDouble( 
                    t1.toString().split(WHITESPACE)[1]
                    )).compareTo(
                        Double.parseDouble(t2.toString().split(WHITESPACE)[1]));

        }
    }

    /**
     * Reducer for the sort job.
     * All the values should have the same key, so this reducer can sort them
     * all at once.
     */
    public static class SortReducer extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> 
    {
        public SortReducer(){}

        private Text outKey = new Text("");
        private final Text outVal = new Text("");

        // reduce(KeyIn key, Iterator<ValueIn> values, 
        // OutputCollector<KeyOut,ValueOut> output, Reporter reporter) 
        public void reduce(Text key, Iterator<Text> values, 
                OutputCollector<Text, Text> output, 
                Reporter reporter) throws IOException 
        {
            // Insert all pairs into the queue.
            while (values.hasNext())
            {
                outKey.set(values.next().toString());
                output.collect(outKey, outVal);
            }
        }
    }


    /**
     * Configures and runs the job for sorting a PageRank iteration.
     * @param i The iteration to sort.
     */
    public void sortIteration(int iter) throws IOException
    {
        // Configuration for this job.
        JobConf conf = new JobConf(PageRank.class);
        conf.setJobName("PageRankSort" + iter);

        // Read from the ith Matrix output.
        FileInputFormat.setInputPaths(conf, new Path(this.tempMatrixOutput + iter));

        // Input type.
        conf.setInputFormat(TextInputFormat.class);

        // Mapper class.
        conf.setMapperClass(SortMapper.class);

        // Mark all keys as equal.
        conf.setOutputValueGroupingComparator(EqualComparator.class);
        // Sort the keys.
        conf.setOutputKeyComparatorClass(SortComparator.class);

        // Reducer class.
        conf.setReducerClass(SortReducer.class);
        conf.setNumReduceTasks(1);
 
        // Output configuration.
        FileOutputFormat.setOutputPath(conf,  
                new Path(this.sortOutput +  "PageRank.iter" + iter + ".out"));
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // Output type.
        conf.setOutputFormat(TextOutputFormat.class);

        JobClient.runJob(conf);
    }

    /**
     * Sorts the first and last PageRank iterations.
     */
    public void sort() throws IOException
    {
        sortIteration(1);
        sortIteration(NUM_PAGERANK_ITERS);
    }

    /**
     * Moves our output from the temporary location to the final locations.
     * This is needed because we are using an older version of Hadoop.
     * PageRank.n.out should have been merged before the PageRank calculation.
     *
     *      results/PageRank.outlink.out
     *      results/PageRank.n.out
     *      results/PageRank.iter1.out (output file for iteration 1)
     *      results/PageRank.iter8.out (output file for iteration 8)
     */
    public void mergeOutput() throws IOException, URISyntaxException
    {
        //this.XMLoutputLocation = this.bucketName + "/tmp/PageRank.outlink.out";
        // Input and output location for the count job.
        this.CountInputLocation = this.XMLoutputLocation;
        this.CountOutputLocation = this.bucketName + "/tmp/PageRank.n.out";
        // Temporary matrix input and output locations.
        this.tempMatrixOutput = this.bucketName + "/tmp/matrixOut";
        // Output for the sorted PageRanks.
        this.sortOutput = this.bucketName + "/tmp/";


        Configuration config = new Configuration();

        // We need to configure the FileSystem if we are accessing from AWS.
        FileSystem fs;
        if (DEBUG)
            fs = FileSystem.get(config);
        else
            fs = FileSystem.get(new URI(this.bucketName), config);

        Path src;
        Path dst;

        // Outlink graph
        src = new Path(this.XMLoutputLocation);
        dst = new Path(this.bucketName + "/results/PageRank.outlink.out");
        FileUtil.copyMerge(fs, src, fs, dst, DELETETEMP, config, "");

        // iter1.out
        src = new Path(this.sortOutput + "PageRank.iter" + 1 + ".out");
        dst = new Path(this.bucketName + "/results/PageRank.iter" + 1 + ".out");
        FileUtil.copyMerge(fs, src, fs, dst, DELETETEMP, config, "");

        // iterN.out
        src = new Path(this.sortOutput + "PageRank.iter" + NUM_PAGERANK_ITERS + ".out");
        dst = new Path(this.bucketName + "/results/PageRank.iter" + NUM_PAGERANK_ITERS + ".out");
        FileUtil.copyMerge(fs, src, fs, dst, DELETETEMP, config, "");
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

        // Sort PageRank.
        pagerank.sort();

        // Merge outputs
        pagerank.mergeOutput();
    }
}

