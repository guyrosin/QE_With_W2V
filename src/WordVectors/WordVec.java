
package WordVectors;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @author dwaipayan
 */
public class WordVec implements Comparable<WordVec>, Cloneable {
    public String  word;       // the word
    public double[] vec;        // the vector for that word
    public double   norm = 1.0;       // the normalized value
    public double   querySim;   // distance from a reference query point

    public WordVec(int vecsize) {
        vec = new double[vecsize];
    }

    public WordVec(String word, double[] vec) {
        this.word = word;
        this.vec = vec;
        this.norm = getNorm();
    }

    public WordVec(String word, double[] vec, double querySim) {
        this.word = word;
        this.vec = vec;
        this.querySim = querySim;
        this.norm = getNorm();
    }

    public WordVec(String word, double querySim) {
        this.word = word;
        this.querySim = querySim;
    }

    public WordVec(String line) {
        String[] tokens = line.split("\\s+");
        word = tokens[0];
        vec = new double[tokens.length-1];
        for (int i = 1; i < tokens.length; i++)
            vec[i-1] = Float.parseFloat(tokens[i]);
        norm = getNorm();
    }

    public String getWord() {
        return word;
    }

    public double getNorm() {
        if (norm == 0 || norm == 1.0) {
            // calculate and store
            double sum = Arrays.stream(vec).map(v -> v * v).sum();
            norm = Math.sqrt(sum);
        }
        return norm;
    }

    public double cosineSim(WordVec that) {
        double sum = 0;
        for (int i = 0; i < this.vec.length; i++) {
            if (that == null) {
                return 0;
            }
            sum += vec[i] * that.vec[i];
        }
        return sum / (this.norm*that.norm);
    }

    @Override
    public int compareTo(WordVec that) {
        return Double.compare(that.querySim, this.querySim);
    }

    public static WordVec add(WordVec a, WordVec b) {
        WordVec sum = new WordVec(a.word + ":" + b.word);
        sum.vec = new double[a.vec.length];
        for (int i = 0; i < a.vec.length; i++) {
            sum.vec[i] = .5 * (a.vec[i]/a.getNorm() + b.vec[i]/b.getNorm());
        }
        return sum;
    }

    public static WordVec addWithoutAverage(WordVec a, WordVec b) {
        WordVec sum = new WordVec(a.word + ":" + b.word);
        sum.vec = new double[a.vec.length];
        for (int i = 0; i < a.vec.length; i++) {
            sum.vec[i] = (a.vec[i] + b.vec[i]);
        }
        return sum;
    }

    public static WordVec add(List<WordVec> list) {

        WordVec sum = new WordVec(list.get(0).vec.length); // initially an all zero vector

        for (int i = 0; i < list.size(); i++) {
            sum = addWithoutAverage(sum, list.get(i));
        }
        for (int i=0; i<sum.vec.length; i++)
            sum.vec[i] /= list.size();
        sum.word = list.get(0).word;
        return sum;
    }

    @Override
    public String toString() {
        return "WordVec{" +
                "word='" + word + '\'' +
                ", norm=" + norm +
                ", querySim=" + querySim +
                '}';
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
