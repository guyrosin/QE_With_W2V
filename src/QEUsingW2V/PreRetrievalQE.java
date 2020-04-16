/**
 * COMPLETE.
 */
package QEUsingW2V;

/**
 * @author dwaipayan
 */

import WordVectors.WordVec;
import WordVectors.WordVecs;
import common.TRECQuery;
import common.TRECQueryParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author dwaipayan
 */

enum QEMethod {
    None,
    Roy,
    Kuzi
}

public class PreRetrievalQE {

    Properties prop;
    String indexPath;
    String queryPath;      // path of the query file
    File queryFile;      // the query file
    IndexReader reader;
    IndexSearcher searcher;
    String stopFilePath;
    String resPath;        // path of the res file
    FileWriter resFileWriter;  // the res file writer
    int numHits;      // number of document to retrieveWithExpansionTermsFromFile
    String runName;        // name of the run
    List<TRECQuery> queries;
    File indexFile;          // place where the index is stored
    Analyzer analyzer;           // the analyzer
    boolean boolIndexExists;    // boolean flag to indicate whether the index exists or not
    String fieldToSearch;  // the field in the index to be searched (e.g. 'content')
    String docIdFieldName;     // the field name of the unique-docid (e.g. 'docid')
    TRECQueryParser trecQueryParser;
    int simFuncChoice;
    float param1, param2;
    String retFunction;        // retrieval function name

    String nnDumpPath;     // path to the file containing the precomputed NNs
    int k;              // k terms to be added in the query
    float QMIX;
    WordVecs wordVecs;
    boolean toCompose;
    private boolean customParse;
    boolean relevanceFeedback;
    private boolean reRank;
    private QEMethod qeMethod;

    public PreRetrievalQE(Properties prop) throws Exception {

        this.prop = prop;
        /* property file loaded */

        // +++++ setting the analyzer with English Analyzer with Smart stopword list
        stopFilePath = prop.getProperty("stopFilePath");
        common.EnglishAnalyzerWithSmartStopword engAnalyzer = new common.EnglishAnalyzerWithSmartStopword(stopFilePath);
        analyzer = engAnalyzer.setAndGetEnglishAnalyzerWithSmartStopword();
        // ----- analyzer set: analyzer

        /* index path setting */
        indexPath = prop.getProperty("indexPath");
        System.out.println("Using index at: " + indexPath);
        indexFile = new File(prop.getProperty("indexPath"));
        Directory indexDir = FSDirectory.open(indexFile.toPath());

        if (!DirectoryReader.indexExists(indexDir)) {
            System.err.println("Index doesn't exists in " + indexFile.getAbsolutePath());
            System.out.println("Terminating");
            boolIndexExists = false;
            System.exit(1);
        }
        fieldToSearch = prop.getProperty("fieldToSearch", "content");
        System.out.println("Field: " + fieldToSearch + " of index will be searched.");
        /* index path set */
        docIdFieldName = prop.getProperty("docIdFieldName", "docid");

        relevanceFeedback = Boolean.parseBoolean(prop.getProperty("relevanceFeedback"));
        reRank = Boolean.parseBoolean(prop.getProperty("reRank"));

        simFuncChoice = Integer.parseInt(prop.getProperty("similarityFunction"));
        param1 = Float.parseFloat(prop.getProperty("param1"));
        param2 = Float.parseFloat(prop.getProperty("param2"));

        /* setting reader and searcher */
        reader = DirectoryReader.open(FSDirectory.open(indexFile.toPath()));
        searcher = new IndexSearcher(reader);
        setSimilarityFunction(simFuncChoice, param1, param2);
        /* reader and searher set */

        /* setting query path */
        queryPath = prop.getProperty("queryPath");
        queryFile = new File(queryPath);
        /* query path set */

        /* constructing the query */
        queries = constructQueries();
        /* constructed the query */
        trecQueryParser = new TRECQueryParser(queryPath, analyzer, fieldToSearch);

        numHits = Integer.parseInt(prop.getProperty("numHits", "1000"));

        /* All word vectors are loaded in wordVecs.wordvecmap */
        wordVecs = new WordVecs(prop);
        k = Integer.parseInt(prop.getProperty("k"));
        QMIX = Float.parseFloat(prop.getProperty("queryMix"));
        toCompose = Boolean.parseBoolean(prop.getProperty("composeQuery"));
        customParse = Boolean.parseBoolean(prop.getProperty("customParse"));
        qeMethod = QEMethod.valueOf(prop.getProperty("qeMethod"));

        /* setting res path */
        resPath = prop.getProperty("resPath");
//        setRunName_ResFileName();
        resFileWriter = new FileWriter(resPath);
        System.out.println("Result will be stored in: " + resPath);
        /* res path set */
    }

    private void setSimilarityFunction(int choice, float param1, float param2) {
        switch (choice) {
            case 0:
                searcher.setSimilarity(new BM25Similarity());
                retFunction = "Default";
                break;
            case 1:
                searcher.setSimilarity(new BM25Similarity(param1, param2));
                retFunction = "BM25-" + param1 + "-" + param2;
                break;
            case 2:
                searcher.setSimilarity(new LMJelinekMercerSimilarity(param1));
                retFunction = "LMJM-" + param1;
                break;
            case 3:
                searcher.setSimilarity(new LMDirichletSimilarity(param1));
                retFunction = "LMDir-" + (int) param1;
                break;
        }
    }

    private void setRunName_ResFileName() {

        String vectorPath = new File(prop.getProperty("vectorPath")).getName();

        runName = queryFile.getName() + "-" + retFunction + "-preRetQE" + vectorPath;
        if (toCompose)
            runName = runName + "-composed";
        runName = runName.replace(" ", "").replace("(", "").replace(")", "").replace("00000", "");
        runName = runName.concat("-" + k + "-" + QMIX);
        if (null == prop.getProperty("resPath"))
            resPath = "/home/dwaipayan/";
        else
            resPath = prop.getProperty("resPath");
        resPath = resPath + runName + ".txt";
    }

    /**
     * Parses the query from the file and makes a List<TRECQuery>
     * containing all the queries
     *
     * @return
     * @throws Exception
     */
    private List<TRECQuery> constructQueries() throws Exception {

        System.out.println("Reading queries from: " + queryPath);
        TRECQueryParser parser = new TRECQueryParser(queryPath, analyzer, fieldToSearch);
        parser.queryFileParse();
        return parser.queries;
    }

    /**
     * Makes Q' = vec(Q) U Qc
     *
     * @return
     */
    public List<WordVec> makeQueryVectorForms(String[] qTerms) {
        return makeQueryVectorForms(qTerms, toCompose);
    }

    /**
     * Makes Q' = vec(Q) U Qc
     *
     * @return
     */
    public List<WordVec> makeQueryVectorForms(String[] qTerms, boolean toCompose) {
        WordVec singleWV;
        List<WordVec> vec_Q = new ArrayList<>();
        // vec(Q)
        for (String qTerm : qTerms) {
            singleWV = wordVecs.wordvecmap.get(qTerm);
            if (null != singleWV) {   // query t has a vector associated with it
                singleWV.norm = singleWV.getNorm();
                singleWV.word = qTerm;
                vec_Q.add(singleWV);
            } else {
                System.out.println(qTerm + " doesn't exist in the model");
                return null;
            }
        }
        List<WordVec> q_prime = new ArrayList<>(vec_Q);
        // --- original query-term vectors are added

        if (toCompose) {
            // Qc
            System.out.println("Composing ");
            List<WordVec> q_c = new ArrayList<>();
            for (int i = 0; i < vec_Q.size() - 1; i++) {
                singleWV = WordVec.add(vec_Q.get(i), vec_Q.get(i + 1));
                singleWV.norm = singleWV.getNorm();
                singleWV.word = vec_Q.get(i).word + "+" + vec_Q.get(i + 1).word;
                q_c.add(singleWV);
            }
            q_prime.addAll(q_c);
        }
        // --- composed query terms are added

        return q_prime;
    }

    /**
     * Returns a hashmap of similar terms of q_prime, computed over the entire vocabulary
     *
     * @param q_prime List of the vectors of the query terms as well as the pairwise composed forms
     * @return Hashmap of terms from across the collection which are similar to q_prime
     */
    public HashMap<String, WordProbability> computeAndSortExpansionTerms_Roy(List<WordVec> q_prime) {

        List<WordVec> sortedExpansionTerms = new ArrayList<>();
        for (WordVec wv : q_prime) {
            //System.out.println(wv.word);
            sortedExpansionTerms.addAll(wordVecs.computeNNs(wv)); // Note this changes the querySim properties
        }
        // sortedExpansionTerms now contains similar terms of query terms (unsorted)

        Collections.sort(sortedExpansionTerms);
        // sortedExpansionTerms now sorted

        int expansionTermCount = 0;
        double norm = 0;
        HashMap<String, WordProbability> hashmap_et = new LinkedHashMap<>();  // to contain M terms with top P(w|R) among each w in R
        for (WordVec singleTerm : sortedExpansionTerms) {
            if (null == hashmap_et.get(singleTerm.word)) {
                hashmap_et.put(singleTerm.word, new WordProbability(singleTerm.word, (float) singleTerm.querySim));
                expansionTermCount++;
                norm += singleTerm.querySim;
                if (expansionTermCount >= k)
                    break;
            }
            //* else: The t is already entered in the hash-map 
        }

        // +++ normalization
        for (Map.Entry<String, WordProbability> entrySet : hashmap_et.entrySet()) {
            WordProbability value = entrySet.getValue();
            value.p_w_given_R /= norm;
        }
        // --- normalization

        return hashmap_et;
    }


    /**
     * Returns a hashmap of similar terms of q_prime, computed over the entire vocabulary
     *
     * @param q_prime List of the vectors of the query terms as well as the pairwise composed forms
     * @return Hashmap of terms from across the collection which are similar to q_prime
     */
    public HashMap<String, WordProbability> computeAndSortExpansionTerms_QAvg(List<WordVec> q_prime) {
        if (q_prime.size() == 0) {
            return new HashMap<>();
        }
        // Compute query average vector
        int vec_size = q_prime.get(0).vec.length;
        double[] query_avg = new double[vec_size];
        for (WordVec q : q_prime) {
            for (int i = 0; i < vec_size; i++) {
                query_avg[i] += q.vec[i];
            }
        }
        for (int i = 0; i < vec_size; i++) {
            query_avg[i] /= q_prime.size();
        }
        ArrayList<String> qWords = q_prime.stream().map(wv -> wv.word).collect(Collectors.toCollection(ArrayList::new));
        List<WordVec> sortedExpansionTerms = wordVecs.computeNNs(query_avg, qWords);
        // sortedExpansionTerms now contains similar terms of query terms (sorted)

//        int expansionTermCount = 0;
        double norm = 0;
        HashMap<String, WordProbability> hashmap_et = new LinkedHashMap<>();  // to contain M terms with top P(w|R) among each w in R
        for (WordVec singleTerm : sortedExpansionTerms) {
            if (null == hashmap_et.get(singleTerm.word)) {
                float prob = (float) Math.exp(singleTerm.querySim);
                hashmap_et.put(singleTerm.word, new WordProbability(singleTerm.word, prob));
//                expansionTermCount++;
                norm += prob;
//                if(expansionTermCount>=k)
//                    break;
            }
            //* else: The t is already entered in the hash-map
        }

        // +++ normalization
        for (Map.Entry<String, WordProbability> entrySet : hashmap_et.entrySet()) {
            WordProbability value = entrySet.getValue();
            value.p_w_given_R /= norm;
        }
        // --- normalization

        return hashmap_et;
    }

    /**
     * Returns a hashmap of similar terms of q_prime, computed over the entire vocabulary
     *
     * @param q_prime List of the vectors of the query terms as well as the pairwise composed forms
     * @param hits
     * @return Hashmap of terms from across the collection which are similar to q_prime
     */
    public HashMap<String, WordProbability> computeAndSortExpansionTerms_Saar(List<WordVec> q_prime, ScoreDoc[] hits) throws IOException {
        if (q_prime.size() == 0) {
            return new HashMap<>();
        }
        // Compute query sum vector
        int vec_size = q_prime.get(0).vec.length;
        double[] query_cent = new double[vec_size];
        for (WordVec q : q_prime) {
            for (int i = 0; i < vec_size; i++) {
                query_cent[i] += q.vec[i];
            }
        }
        ArrayList<String> qWords = q_prime.stream().map(wv -> wv.word).collect(Collectors.toCollection(ArrayList::new));

        HashMap<String, WordVec> feedbackTerms = new HashMap<>();
        List<WordVec> sortedExpansionTerms;
        if (relevanceFeedback) {
            int numFeedbackDocs = 10;
            for (int i = 0; i < Math.min(numFeedbackDocs, hits.length); i++) {
                // for each of the numFeedbackDocs initially retrieved documents:
                int luceneDocId = hits[i].doc;
                Document d = searcher.doc(luceneDocId);
                Terms lol = reader.getTermVector(luceneDocId, fieldToSearch);
                ArrayList<String> terms = new ArrayList<>();
                TermsEnum iterator = lol.iterator();
                BytesRef byteRef;

                //* for each word in the document
                while ((byteRef = iterator.next()) != null) {
                    String term = new String(byteRef.bytes, byteRef.offset, byteRef.length);
                    terms.add(term);
                }
//                queryParser.parse(queryStr, fieldToSearch)
                for (String term : terms) {
                    // for each of the terms of the feedback document
                    WordVec termVec = wordVecs.wordvecmap.get(term);
                    if (termVec != null)
                        feedbackTerms.put(term, termVec);
                }
            }
            sortedExpansionTerms = wordVecs.computeNNs(query_cent, qWords, true, feedbackTerms);
        } else {
            sortedExpansionTerms = wordVecs.computeNNs(query_cent, qWords, true);
        }

        // sortedExpansionTerms now contains similar terms of query terms (sorted)

//        int expansionTermCount = 0;
        double norm = 0;
        HashMap<String, WordProbability> hashmap_et = new LinkedHashMap<>();  // to contain M terms with top P(w|R) among each w in R
        for (WordVec singleTerm : sortedExpansionTerms) {
            if (null == hashmap_et.get(singleTerm.word)) {
                float prob = (float) Math.exp(singleTerm.querySim);
                hashmap_et.put(singleTerm.word, new WordProbability(singleTerm.word, prob));
//                expansionTermCount++;
                norm += prob;
//                if(expansionTermCount>=k)
//                    break;
            }
            //* else: The t is already entered in the hash-map
        }

        // +++ normalization
        for (Map.Entry<String, WordProbability> entrySet : hashmap_et.entrySet()) {
            WordProbability value = entrySet.getValue();
            value.p_w_given_R /= norm;
        }
        // --- normalization

        return hashmap_et;
    }

    /**
     * Returns the new formed, expanded query
     *
     * @param qTerms The initial query terms
     * @param hits
     * @return BooleanQuery - The expanded query in boolean format
     * @throws Exception
     */
    public BooleanQuery makeNewQuery_Kuzi(String[] qTerms, ScoreDoc[] hits) throws Exception {

        List<WordVec> qVecs = makeQueryVectorForms(qTerms, false);
        if (qVecs == null) {
            return null;
        }
        HashMap<String, WordProbability> hashmap_et = computeAndSortExpansionTerms_Saar(qVecs, hits);

        // Now hashmap_et contains all the expansion terms (normalized weights). No query specific information.

//        for (String wv : hashmap_et.keySet())
//            System.out.print(wv+" ");
//        System.out.println();

        float normFactor = 0;
        //* Each w of hashmap_et: existing-weight to be QMIX*existing-weight
        for (Map.Entry<String, WordProbability> entrySet : hashmap_et.entrySet()) {
            WordProbability value = entrySet.getValue();
            value.p_w_given_R = value.p_w_given_R * QMIX;
            normFactor += value.p_w_given_R;
        }

        // Now weight(w) = QMIX*existing-weight
        //* Each w which are also query terms: weight(w) += (1-QMIX)*P(w|Q)
        //      P(w|Q) = tf(w,Q)/|Q|
        for (String qTerm : qTerms) {
            WordProbability existingTerm = hashmap_et.get(qTerm);
            float newWeight = (1.0f - QMIX) * returnMLE_of_q_in_Q(qTerms, qTerm);
            if (null != existingTerm) // qTerm is already in hashmap_et
                existingTerm.p_w_given_R += newWeight;
            else  // the qTerm is not in R
                hashmap_et.put(qTerm, new WordProbability(qTerm, newWeight));
            normFactor += newWeight;
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
//        Set<String> qTermsSet = Set.of(qTerms);
        for (Map.Entry<String, WordProbability> entrySet : hashmap_et.entrySet()) {
            Term thisTerm = new Term(fieldToSearch, entrySet.getKey());
            Query tq = new TermQuery(thisTerm);
            tq = new BoostQuery(tq, entrySet.getValue().p_w_given_R / normFactor);
//            builder.add(tq, qTermsSet.contains(thisTerm.text()) ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD);
            builder.add(tq, BooleanClause.Occur.SHOULD);
//            System.out.println(entrySet.getKey()+"^"+entrySet.getValue().p_w_given_R);
        }

        return builder.build();
    }


    /**
     * Returns MLE of a query term q in Q;<p>
     * P(w|Q) = tf(w,Q)/|Q|
     *
     * @param qTerms all query terms
     * @param qTerm  query term under consideration
     * @return
     */
    public float returnMLE_of_q_in_Q(String[] qTerms, String qTerm) {
        int count = (int) Arrays.stream(qTerms).filter(qTerm::equals).count();
        return ((float) count / (float) qTerms.length);
    }

    /**
     * Returns the new formed, expanded query
     *
     * @param qTerms The initial query terms
     * @param hits
     * @return BooleanQuery - The expanded query in boolean format
     */
    public BooleanQuery makeNewQuery_Roy(String[] qTerms, ScoreDoc[] hits) {

        // Roy 2016's expansion
        List<WordVec> q_prime = makeQueryVectorForms(qTerms);
        if (q_prime == null) {
            return null;
        }
        HashMap<String, WordProbability> hashmap_et = computeAndSortExpansionTerms_QAvg(q_prime);

//        List<WordVec> qVecs = makeQueryVectorForms(qTerms, false);
//        if (qVecs == null) {
//            return null;
//        }
//        HashMap<String, WordProbability> hashmap_et = computeAndSortExpansionTerms_QAvg(qVecs);

        // Now hashmap_et contains all the expansion terms (normalized weights). No query specific information.

//        for (String wv : hashmap_et.keySet())
//            System.out.print(wv+" ");
//        System.out.println();

        float normFactor = 0;
        //* Each w of hashmap_et: existing-weight to be QMIX*existing-weight
        for (Map.Entry<String, WordProbability> entrySet : hashmap_et.entrySet()) {
            WordProbability value = entrySet.getValue();
            value.p_w_given_R = value.p_w_given_R * QMIX;
            normFactor += value.p_w_given_R;
        }

        // Now weight(w) = QMIX*existing-weight
        //* Each w which are also query terms: weight(w) += (1-QMIX)*P(w|Q)
        //      P(w|Q) = tf(w,Q)/|Q|
        for (String qTerm : qTerms) {
            WordProbability existingTerm = hashmap_et.get(qTerm);
            float newWeight = (1.0f - QMIX) * returnMLE_of_q_in_Q(qTerms, qTerm);
            if (null != existingTerm) // qTerm is already in hashmap_et
                existingTerm.p_w_given_R += newWeight;
            else  // the qTerm is not in R
                hashmap_et.put(qTerm, new WordProbability(qTerm, newWeight));
            normFactor += newWeight;
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
//        Set<String> qTermsSet = Set.of(qTerms);
        for (Map.Entry<String, WordProbability> entrySet : hashmap_et.entrySet()) {
            Term thisTerm = new Term(fieldToSearch, entrySet.getKey());
            Query tq = new TermQuery(thisTerm);
            tq = new BoostQuery(tq, entrySet.getValue().p_w_given_R / normFactor);
//            builder.add(tq, qTermsSet.contains(thisTerm.text()) ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD);
            builder.add(tq, BooleanClause.Occur.SHOULD);
//            System.out.println(entrySet.getKey()+"^"+entrySet.getValue().p_w_given_R / normFactor);
        }

        return builder.build();
    }

    public void retrieveAll() throws Exception {

        ScoreDoc[] hits;
        TopDocs topDocs;
        TopScoreDocCollector collector;
        int totalHitThreshold = Integer.MAX_VALUE;

        for (TRECQuery query : queries) {
            Query luceneQuery = trecQueryParser.getAnalyzedQuery(query, customParse);

            hits = null;
            if (relevanceFeedback) {
                collector = TopScoreDocCollector.create(numHits, totalHitThreshold);
                searcher.search(luceneQuery, collector);
                topDocs = collector.topDocs();
                hits = topDocs.scoreDocs;
            }

            String[] qTerms = luceneQuery.toString(fieldToSearch).split(" ");
            BooleanQuery bq;
            switch (qeMethod) {
                case None:
                    bq = trecQueryParser.makeQuery(qTerms);
                    break;
                case Roy:
                    bq = makeNewQuery_Roy(qTerms, hits);
                    break;
                case Kuzi:
                    bq = makeNewQuery_Kuzi(qTerms, hits);
                    break;
                default:
                    throw new IllegalStateException("Unexpected QEEMethod: " + qeMethod);
            }
            if (bq == null) {
                System.out.println("Skipping query " + query.qid + ": " + query.qtitle);
                continue;
            }
            System.out.println(query.qid + ": " + query.qtitle + " --> " + bq.toString(fieldToSearch));
            collector = TopScoreDocCollector.create(numHits, totalHitThreshold);
            searcher.search(bq, collector);
            topDocs = collector.topDocs();
            hits = topDocs.scoreDocs;
            int hits_length = hits.length;
            if (hits_length < 1000)
                System.out.println(hits_length + " results retrieved for query: " + query.qid);

            if (reRank) {
                for (ScoreDoc scoreDoc : hits) {
                    int docId = scoreDoc.doc;
                    float score = scoreDoc.score;
                    // TODO: calculate our score for this document, and interpolate with the current score
                }
            }

            // +++ Writing the result file 
            resFileWriter = new FileWriter(resPath, true);
            StringBuilder resBuilder = new StringBuilder();
            for (int i = 0; i < hits_length; ++i) {
                int docId = hits[i].doc;
                Document d = searcher.doc(docId);
                resBuilder.append(query.qid).append("\tQ0\t").
                        append(d.get(docIdFieldName)).append("\t").
                        append((i)).append("\t").
                        append(hits[i].score).append("\t").
                        append(runName).append("\n");
            }
            resFileWriter.write(resBuilder.toString());
            resFileWriter.close();
            // --- result file written
        } // ends for each query
    } // ends retrieveAll

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        if (1 != args.length) {
            System.out.println("Usage: java PreRetrievalQE <.properties>");
            System.exit(1);
        }

        prop.load(new FileReader(args[0]));
        PreRetrievalQE preRetqe = new PreRetrievalQE(prop);
        preRetqe.retrieveAll();
    }
}