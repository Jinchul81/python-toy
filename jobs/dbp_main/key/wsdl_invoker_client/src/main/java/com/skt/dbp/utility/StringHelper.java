package com.skt.dbp.utility;

import org.apache.xml.utils.XMLChar;

import java.util.Vector;

/**
 * Created by jinchulkim on 2017. 8. 24..
 */
public class StringHelper {
    public static Vector<String> splitString(final char separator, final String input) {
        Vector<String> v = new Vector<String>();

        int prevPos = 0;
        for (int i = 0; i < input.length(); ++i) {
            final char c = input.charAt(i);
            if (c == separator) {
                v.add(input.substring(prevPos, i));
                prevPos = i+1;
            }
        }
        return v;
    }

    public static boolean isNumeric(final String s) {
        return isNumeric(s, 10);
    }

    public static boolean isNumeric(final String s, final int radix) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); ++i) {
            if (Character.digit(s.charAt(i), radix) < 0) return false;
        }
        return true;
    }

    public static String stripInvalidXMLCharacters(final String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); ++i) {
            final char c = s.charAt(i);
            if (XMLChar.isValid(c)) {
                sb.append(c);
            }
        }

        final String result = sb.toString();
//        if (! s.equals(result)) { System.err.println("[" + s + "] => [" + result + "]"); }
        return result;
    }

    public static String removeRoundBrackets(final String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); ++i) {
            final char c = s.charAt(i);
            if ('(' == c || ')' == c) {
                sb.append(' ');
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }
}
