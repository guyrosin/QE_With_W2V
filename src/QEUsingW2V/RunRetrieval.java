package QEUsingW2V;

import common.TRECQuery;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.*;
import org.apache.lucene.util.fst.PairOutputs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

public class RunRetrieval {

    private final String queriesPath;
    private PreRetrievalQE retriever;

    public RunRetrieval(PreRetrievalQE retriever, String queriesPath) {
        this.retriever = retriever;
        this.queriesPath = queriesPath;
    }

    public void retrieveAll() throws Exception {

        // Read and parse the queries
        ArrayList<TRECQuery> queries = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(this.queriesPath));
        String myLine;
        while ((myLine = reader.readLine()) != null) {
            TRECQuery query = new TRECQuery();
            String[] array = myLine.split(":");
            query.qid = array[0];
            query.qtitle = array[1];
            String[] terms = array[2].split("#");
            for (String term_str : terms) {
                String[] term_array = term_str.split("^");
                String term = term_array[0];
                float weight = Float.parseFloat(term_array[1]);
                query.addExtensionTerm(term, weight);
            }
            queries.add(query);
        }

        // Run the retriever

        ScoreDoc[] hits;
        TopDocs topDocs;
        TopScoreDocCollector collector;
        int totalHitThreshold = Integer.MAX_VALUE;

        for (TRECQuery query : queries) {
            Query luceneQuery = retriever.trecQueryParser.getAnalyzedQuery(query);

//            hits = null;
//            if (retriever.relevanceFeedback) {
//                collector = TopScoreDocCollector.create(retriever.numHits, totalHitThreshold);
//                retriever.searcher.search(luceneQuery, collector);
//                topDocs = collector.topDocs();
//                hits = topDocs.scoreDocs;
//            }

//            BooleanQuery bq = makeNewQuery(luceneQuery.toString(fieldToSearch).split(" "), hits);
            BooleanQuery bq = retriever.trecQueryParser.makeQuery(luceneQuery.toString(retriever.fieldToSearch).split(" "));
            if (bq == null) {
                System.out.println("Skipping query " + query.qid + ": " + query.qtitle);
                continue;
            }
            System.out.println(query.qid + ": " + query.qtitle + " --> " + bq.toString(retriever.fieldToSearch));
            collector = TopScoreDocCollector.create(retriever.numHits, totalHitThreshold);
            retriever.searcher.search(bq, collector);
            topDocs = collector.topDocs();
            hits = topDocs.scoreDocs;
            int hits_length = hits.length;
            if (hits_length < 1000)
                System.out.println(hits_length + " results retrieved for query: " + query.qid);

//            if (retriever.reRank) {
//                for (ScoreDoc scoreDoc : hits) {
//                    int docId = scoreDoc.doc;
//                    float score = scoreDoc.score;
//                    // TODO: calculate our score for this document, and interpolate with the current score
//                }
//            }

            // +++ Writing the result file
            new FileWriter(retriever.resPath);
            FileWriter resFileWriter = new FileWriter(retriever.resPath, true);
            StringBuilder resBuilder = new StringBuilder();
            for (int i = 0; i < hits_length; ++i) {
                int docId = hits[i].doc;
                Document d = retriever.searcher.doc(docId);
                resBuilder.append(query.qid).append("\tQ0\t").
                        append(d.get(retriever.docIdFieldName)).append("\t").
                        append((i)).append("\t").
                        append(hits[i].score).append("\t").
                        append(retriever.runName).append("\n");
            }
            resFileWriter.write(resBuilder.toString());
            resFileWriter.close();
            // --- result file written
        } // ends for each query
    } // ends retrieveAll

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        if (2 != args.length) {
            System.out.println("Usage: java PreRetrievalQE <.properties> <QueriesPath>");
            System.exit(1);
        }

        prop.load(new FileReader(args[0]));
        PreRetrievalQE retriever = new PreRetrievalQE(prop);

        String queriesPath = args[1];
        RunRetrieval runner = new RunRetrieval(retriever, queriesPath);

        runner.retrieveAll();
    }
}
