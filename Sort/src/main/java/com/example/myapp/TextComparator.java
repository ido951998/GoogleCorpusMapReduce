package com.example.myapp;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.util.StringTokenizer;

public class TextComparator implements RawComparator<Text> {
    @Override
    public int compare(Text o1, Text o2) {
        StringTokenizer itr = new StringTokenizer(o1.toString());
        String[] arr1 = itr.nextToken().split("!!!");
        itr = new StringTokenizer(o2.toString());
        String[] arr2 = itr.nextToken().split("!!!");
        if (arr1[0].compareTo(arr2[0]) == 0) {
            if (arr1[1].compareTo(arr2[1]) == 0) {
                Double first = Double.parseDouble(arr1[2]);
                Double second = Double.parseDouble(arr2[2]);
                return second.compareTo(first);
            }
            return arr1[1].compareTo(arr2[1]);
        }
        return arr1[0].compareTo(arr2[0]);
    }

    @Override
    public int compare(byte[] bytes1, int s1, int l1, byte[] bytes2, int s2, int l2) {
        Text t1 = null, t2 = null;
        try {
            t1 = new Text(Text.decode(bytes1, s1, l1));
            t2 = new Text(Text.decode(bytes2, s2, l2));
        } catch (CharacterCodingException e) {
            e.printStackTrace();
        }
        return this.compare(t1, t2);
    }
}