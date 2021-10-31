import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;


public class WordsCoOccurrence {

    public static class ExamplesArticleList {
        public List<ExamplesArticle> articleList;
    }

    public static class ExamplesArticle {
        public String content;
        public String title;
        public List<String> author = new ArrayList<String>();
    }

    public static class ExamplesSAXParser extends DefaultHandler {
        private static final String ARTICLES = "dblp";
        private static final String ARTICLE = "article";
        private static final String AUTHOR = "author";
        private static final String TITLE = "title";
        private static final String CONTENT = "content";

        private ExamplesArticleList articleList;
        private StringBuilder elementValue;

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            if (elementValue == null) {
                elementValue = new StringBuilder();
            } else {
                elementValue.append(ch, start, length);
            }
        }

        @Override
        public void startDocument() throws SAXException {
            articleList = new ExamplesArticleList();
        }

        @Override
        public void startElement(String uri, String lName, String qName, Attributes attr) throws SAXException {

            if(qName == ARTICLES) {
                articleList.articleList = new ArrayList<ExamplesArticle>();
            } else if (qName == TITLE) {
                elementValue = new StringBuilder();
            }  else if (qName == CONTENT) {
                elementValue = new StringBuilder();
            }  else if (qName == AUTHOR) {
                elementValue = new StringBuilder();
            }  else if (qName == "inproceedings" || qName == "article" || qName == "www" || qName == "book" || qName == "mastersthesis" || qName == "phdthesis") {
                articleList.articleList.add(new ExamplesArticle());
            }

            if(articleList.articleList.size() > 10000) {
                throw new SAXException("limit reached");
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {

            if(qName == AUTHOR) {
                latestArticle().author.add(elementValue.toString());
            } else if(qName == TITLE) {
                latestArticle().title = elementValue.toString();
            } else if(qName == CONTENT) {
                latestArticle().content = elementValue.toString();
            }
        }

        private ExamplesArticle latestArticle() {
            List<ExamplesArticle> one = articleList.articleList;
            int latestArticleIndex = one.size() - 1;
            return one.get(latestArticleIndex);
        }

        public ExamplesArticleList getArticleList() {
            return articleList;
        }
    }

    public static class CooccurenceMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // get the filename
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();


            File inputFile = new File("/home/hadoop/dblp.xml");
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = null;
            try {
                saxParser = factory.newSAXParser();
            }catch (Exception e) {
                e.printStackTrace();
            }

            ExamplesSAXParser userhandler = new ExamplesSAXParser();
            ExamplesArticleList list = new ExamplesArticleList();

            try {
                saxParser.parse(inputFile, userhandler);
            } catch (Exception e) {
                list = userhandler.getArticleList();
                e.printStackTrace();
            }


            for(ExamplesArticle article : list.articleList) {
                if(article.author.size() > 0) {
                    String authorName = article.author.get(0);

                    for(int i = 1; i < article.author.size(); i++) {
                        String coAuthorName = article.author.get(i);

                        context.write( new Text(authorName), new Text(coAuthorName) );
                    }
                }
            }
        }
    }

    public static class CooccurenceReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            StringBuilder str = new StringBuilder();

            for (Text val : values) {
                str.append(val.toString()).append(",");
            }
            str.deleteCharAt(str.length() - 1);
            context.write(key, new Text(str.toString()));
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word cooccurence");
        job.setJarByClass(WordsCoOccurrence.class);
        job.setMapperClass(CooccurenceMapper.class);
        job.setCombinerClass(CooccurenceReducer.class);
        job.setReducerClass(CooccurenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, createTempfile("filename.txt") ? new Path("filename.txt") : new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static boolean createTempfile(String fileName) {
        try {
            FileWriter myWriter = new FileWriter(fileName);
            myWriter.write("Sample file");
            myWriter.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
