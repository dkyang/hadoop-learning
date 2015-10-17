package ydk.learn.storm.wordcount;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by yangdekun on 2015/9/30.
 */
public class WordReaderSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean isCompleted = false;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try {
            this.fileReader = new FileReader(map.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            System.err.println("wordsFile not found!");
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        if (isCompleted) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {

            }
            return;
        }

        String line;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while ((line = reader.readLine()) != null) {
                /*
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                while (stringTokenizer.hasMoreTokens()) {
                    String token = stringTokenizer.nextToken();
                    System.out.println("Word: " + token);
                    collector.emit(new Values(token));
                }
                */
                collector.emit(new Values(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            isCompleted = true;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public void close() {

    }

}
