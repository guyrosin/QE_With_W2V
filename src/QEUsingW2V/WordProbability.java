package QEUsingW2V;

/**
 * @author dwaipayan
 */

class WordProbability {
    String w;
    float p_w_given_R;      // probability of w given R

    public WordProbability(String w, float p_w_given_R) {
        this.w = w;
        this.p_w_given_R = p_w_given_R;
    }

}
