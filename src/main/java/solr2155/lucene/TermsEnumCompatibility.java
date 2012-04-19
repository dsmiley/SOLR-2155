package solr2155.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;

/**
 * Wraps Lucene 3 TermEnum to make it look like a Lucene 4 TermsEnum
 * @author dsmiley
 */
public class TermsEnumCompatibility {
  private final IndexReader reader;
  private final String fieldName;
  private TermEnum termEnum;
  private boolean initialState = true;

  public TermsEnumCompatibility(IndexReader reader, String fieldName) throws IOException {
    this.reader = reader;
    this.fieldName = fieldName.intern();
    this.termEnum = reader.terms(new Term(this.fieldName));
  }

  public TermEnum getTermEnum() {
    return termEnum;
  }

  public Term term() {
    Term t = termEnum.term();
    return t != null && t.field() == fieldName ? t : null;
  }

  public Term next() throws IOException {
    //in Lucene 3, a call to reader.terms(term) is already pre-positioned, you don't call next first
    if (initialState) {
      initialState = false;
      return term();
    } else {
      return termEnum.next() ? term() : null;
    }
  }

  public void close() throws IOException {
    termEnum.close();
  }

  public static enum SeekStatus {END, FOUND, NOT_FOUND}

  public SeekStatus seek(String value) throws IOException {
    termEnum = reader.terms(new Term(this.fieldName,value));
    Term t = term();
    if (t == null)
      return SeekStatus.END;
    return (t.text().equals(value)) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
  }
}
