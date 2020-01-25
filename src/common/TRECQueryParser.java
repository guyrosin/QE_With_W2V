
package common;

/**
 *
 * @author dwaipayan
 */
import static common.CommonVariables.FIELD_BOW;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.select.NodeVisitor;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

public class TRECQueryParser {

    StringBuffer        buff;      // Accumulation buffer for storing the current topic
    String              queryFilePath;
    TRECQuery           query;
    Analyzer            analyzer;
    StandardQueryParser queryParser;
    String              fieldToSearch;  // field name of the index to be searched

    public List<TRECQuery>  queries;
    final static String[] tags = {"num", "title", "desc", "narr"};

    /**
     * Constructor: 
     *      fieldToSearch is initialized with 'content';
     *      analyzer is set to EnglishAnalyzer();
     * @param queryFilePath Absolute path of the query file
     */
    public TRECQueryParser(String queryFilePath)  {
       this.queryFilePath = queryFilePath;
       this.fieldToSearch = FIELD_BOW;
       buff = new StringBuffer();
       queries = new LinkedList<>();
       analyzer = new EnglishAnalyzer();
    }

    /**
     * Constructor: fieldToSearch is initialized with 'content'
     * @param queryFilePath Absolute path of the query file
     * @param analyzer Analyzer to be used for analyzing the query fields
     */
    public TRECQueryParser(String queryFilePath, Analyzer analyzer) {
       this.queryFilePath = queryFilePath;
       this.analyzer = analyzer;
       this.fieldToSearch = FIELD_BOW;
       buff = new StringBuffer();
       queries = new LinkedList<>();
       queryParser = new StandardQueryParser(this.analyzer);
    }

    /**
     * Constructor:
     * @param queryFilePath Absolute path of the query file
     * @param analyzer Analyzer to be used for analyzing the query fields
     * @param fieldToSearch Field of the index to be searched
     */
    public TRECQueryParser(String queryFilePath, Analyzer analyzer, String fieldToSearch) {
       this.queryFilePath = queryFilePath;
       this.analyzer = analyzer;
       this.fieldToSearch = fieldToSearch;
       buff = new StringBuffer();
       queries = new LinkedList<>();
       queryParser = new StandardQueryParser(this.analyzer);
    }

    /**
     * Parses the query file from xml format using SAXParser;
     * 'queries' list gets initialized with the queries 
     * (with title, desc, narr and qid in different place holders)
     * @throws Exception
     */
    public void queryFileParse() throws Exception {
        String xml = new String(Files.readAllBytes(Paths.get(queryFilePath)));
        Document doc = Jsoup.parse(xml);
        doc.traverse(new NodeVisitor() {
            public void head(Node node, int depth) {
                String qName = node.nodeName();
                    if (qName.equalsIgnoreCase("title")) {
                        query.qtitle = node.childNode(0).toString().trim();
                    } else if (qName.equalsIgnoreCase("desc")) {
                        query.qdesc = node.childNode(0).toString().trim();
                    } else if (qName.equalsIgnoreCase("num")) {
                        query.qid = node.childNode(0).toString().trim();
                        if (query.qid.startsWith("Number: ")) {
                            query.qid = query.qid.substring("Number: ".length());
                        }
                    } else if (qName.equalsIgnoreCase("narr")) {
                        query.qnarr = node.childNode(0).toString().trim();
                    } else if (qName.equalsIgnoreCase("top")) {
                        if (query != null) {
                            queries.add(query);
                        }
                        query = new TRECQuery();
                    }
            }
            public void tail(Node node, int depth) {
            }
        });

    }

    /**
     * Returns an unexpanded query from the given terms
     * @param qTerms
     * @return BooleanQuery - The query in boolean format
     */
    public BooleanQuery makeQuery(String[] qTerms) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : qTerms) {
            Term thisTerm = new Term(fieldToSearch, term);
            Query tq = new TermQuery(thisTerm);
            builder.add(tq, BooleanClause.Occur.SHOULD);
        }
        return builder.build();
    }

    public Query getAnalyzedQuery(TRECQuery trecQuery, boolean customParse) throws Exception {

        trecQuery.qtitle = trecQuery.qtitle.replaceAll("-", " ");
        String queryStr = trecQuery.qtitle.replaceAll("/", " ")
                .replaceAll("\\?", " ").replaceAll("\"", " ").replaceAll("\\&", " ");
        Query luceneQuery = customParse ? makeQuery(queryStr.toLowerCase().split(" ")) : queryParser.parse(queryStr, fieldToSearch);
        trecQuery.luceneQuery = luceneQuery;

        return luceneQuery;
    }

    public Query getAnalyzedQuery(TRECQuery trecQuery) throws Exception {
        return getAnalyzedQuery(trecQuery, false);
    }

    public Query getAnalyzedQuery(String queryString) {

//        queryString = queryString.replaceAll("-", " ");
//        Query luceneQuery = queryParser.parse(queryString.replaceAll("/", " ")
//            .replaceAll("\\?", " ").replaceAll("\"", " ").replaceAll("\\&", " "), fieldToSearch);

        Query luceneQuery = new TermQuery(new Term(fieldToSearch, queryString));
        return luceneQuery;
    }

    public static void main(String[] args) {

        if (args.length < 1) {
            args = new String[1];
            System.err.println("usage: java TRECQuery <input xml file>");
            args[0] = "/home/dwaipayan/Dropbox/ir/corpora-stats/topics_xml/trec5.xml";
        }

        try {

            EnglishAnalyzerWithSmartStopword obj;
            obj = new EnglishAnalyzerWithSmartStopword("/home/dwaipayan/smart-stopwords");
            Analyzer analyzer = obj.setAndGetEnglishAnalyzerWithSmartStopword();

            TRECQueryParser queryParser = new TRECQueryParser(args[0], analyzer);
            queryParser.queryFileParse();

            for (TRECQuery query : queryParser.queries) {
                System.out.println("ID: "+query.qid);
                System.out.println("Title: "+query.qtitle);
                Query luceneQuery;
                luceneQuery = queryParser.getAnalyzedQuery(query);
                System.out.println("Parsed: "+luceneQuery.toString(queryParser.fieldToSearch));
                System.out.println(query.queryFieldAnalyze(analyzer, query.qtitle));    // this statement analyze the query text as simple text
            }

        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
