package com.danawa.dsearch.server.entity;

import java.util.List;
import java.util.Map;

public class DocumentAnalyzer {

    private String index;
    // document_id, analyzer
    private Map<String, List<Analyzer>> analyzer;

    public static class Analyzer {
        private String field;
        private String text;
        private String analyzer;
        private List<String> term;

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public String getAnalyzer() {
            return analyzer;
        }

        public void setAnalyzer(String analyzer) {
            this.analyzer = analyzer;
        }

        public List<String> getTerm() {
            return term;
        }

        public void setTerm(List<String> term) {
            this.term = term;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Map<String, List<Analyzer>> getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(Map<String, List<Analyzer>> analyzer) {
        this.analyzer = analyzer;
    }
}
