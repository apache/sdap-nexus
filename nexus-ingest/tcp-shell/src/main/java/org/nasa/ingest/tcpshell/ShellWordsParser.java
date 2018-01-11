package org.nasa.ingest.tcpshell;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses a string into a collection of words based on the
 * <a href="http://pubs.opengroup.org/onlinepubs/009695399/toc.htm">POSIX / SUSv3 standard</a>.  The functionality in
 * this class is ported from the Ruby
 * <a href="https://github.com/rubysl/rubysl-shellwords/blob/2.0/lib/rubysl/shellwords/shellwords.rb">Shellwords</a>
 * module.
 */
final class ShellWordsParser {

    private static final Pattern SHELLWORDS = Pattern.compile("\\G\\s*(?>([^\\s\\\\'\"]+)|'([^']*)'|\"((?:[^\"\\\\]|\\\\.)*)\"|(\\\\.?)|(\\S))(\\s|\\z)?",
            Pattern.MULTILINE);

    /**
     * Parses a string into a collection of words based on POSIX standard
     *
     * @param s the string to parse
     * @return the collection of words
     */
    List<String> parse(String s) {
        List<String> words = new ArrayList<>();
        String field = "";

        Matcher matcher = SHELLWORDS.matcher(s);
        while (matcher.find()) {
            String word = matcher.group(1);
            String sq = matcher.group(2);
            String dq = matcher.group(3);
            String esc = matcher.group(4);
            String garbage = matcher.group(5);
            String sep = matcher.group(6);

            if (garbage != null) {
                throw new IllegalArgumentException("Unmatched double quote: " + s);
            }

            if (word != null) {
                field = word;
            } else if (sq != null) {
                field = sq;
            } else if (dq != null) {
                field = dq.replaceAll("\\\\(?=.)", "");
            } else if (esc != null) {
                field = esc.replaceAll("\\\\(?=.)", "");
            }

            if (sep != null) {
                words.add(field);
                field = "";
            }
        }

        return words;
    }
}
