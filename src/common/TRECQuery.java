/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package common;

import static common.CommonVariables.FIELD_BOW;

import java.io.StringReader;
import java.util.AbstractMap;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * @author dwaipayan
 */
public class TRECQuery {
    public String qid;
    public String qtitle;
    public String qdesc;
    public String qnarr;
    public Query luceneQuery;
    public String fieldToSearch;
    public ArrayList<AbstractMap.SimpleEntry<String, Float>> extensionTerms;

    @Override
    public String toString() {
        return qid + "\t" + qtitle;
    }

    public Query getLuceneQuery() {
        return luceneQuery;
    }

    /**
     * Returns analyzed queryFieldText from the query
     *
     * @param analyzer
     * @param queryFieldText
     * @return (String) The content of the field
     * @throws Exception
     */
    public String queryFieldAnalyze(Analyzer analyzer, String queryFieldText) throws Exception {
        fieldToSearch = FIELD_BOW;
        StringBuilder localBuff = new StringBuilder();
//        queryFieldText = queryFieldText.replace(".", "");
        TokenStream stream = analyzer.tokenStream(fieldToSearch, new StringReader(queryFieldText));
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
        stream.reset();
        while (stream.incrementToken()) {
            String term = termAtt.toString();
            term = term.toLowerCase();
            localBuff.append(term).append(" ");
        }
        stream.end();
        stream.close();
        return localBuff.toString();
    }

    public Query getBOWQuery(Analyzer analyzer, TRECQuery query) throws Exception {
        fieldToSearch = FIELD_BOW;
        Term thisTerm;

        String[] terms = queryFieldAnalyze(analyzer, query.qtitle).split("\\s+");
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String term : terms) {
            thisTerm = new Term(fieldToSearch, term);
            Query tq = new TermQuery(thisTerm);
            builder.add(tq, BooleanClause.Occur.SHOULD);
        }
        luceneQuery = builder.build();

        return luceneQuery;
    }

    public ArrayList<AbstractMap.SimpleEntry<String, Float>> getExtensionTerms() {
        if (extensionTerms == null)
            extensionTerms = new ArrayList<>();
        return extensionTerms;
    }

    public void addExtensionTerm(String term, float weight) {
        getExtensionTerms().add(new AbstractMap.SimpleEntry<>(term, weight));
    }

}
