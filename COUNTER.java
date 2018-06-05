package it.cnr.isti.pad;

// The purpose of this class is to define the counter (called CONDITION_SAT)
// used by the Pagerank algorithm to detect the number of pages which agreed on
// the convergence condition:
//		| PRi(A) - PRi+1(A) | < e
// where A is a web page, PRi(A) is the pagerank of page A at the i-th
// iteration and e is the thresold error.

public enum COUNTER{
        CONDITION_SAT
};
