package com.star.flink.model;

public class Word {
    private String word;
    private long frequency;

    public Word() {
    }

    public Word(String word, long frequency) {
        this.word = word;
        this.frequency = frequency;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getFrequency() {
        return frequency;
    }

    public void setFrequency(long frequency) {
        this.frequency = frequency;
    }

    @Override
    public String toString() {
        return "WordCount: " + word + " " + frequency;
    }
}
