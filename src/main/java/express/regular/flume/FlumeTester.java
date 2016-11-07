package express.regular.flume;

import express.regular.common.*;
import express.regular.exception.InvalidConfigException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.RegexExtractorInterceptor;
import org.apache.flume.interceptor.RegexFilteringInterceptor;
import org.apache.flume.interceptor.SearchAndReplaceInterceptor;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.*;

public class FlumeTester extends Tester {

    public static final String CONFIG_TYPE = "test_type";
    public static final String CONFIG_FILTERING_REGEX = RegexFilteringInterceptor.Constants.REGEX;
    public static final String CONFIG_FILTERING_EXCLUDE_EVENTS = RegexFilteringInterceptor.Constants.EXCLUDE_EVENTS;
    public static final String CONFIG_EXTRACTOR_FLUME_CONTEXT = "flume_context";
    public static final String CONFIG_REPLACE_PATTERN = "searchPattern";
    public static final String CONFIG_REPLACE_STRING = "replaceString";
    public static final String CONFIG_REPLACE_CHARSET = "charset";

    public static final String TYPE_FILTERING = "filtering";
    public static final String TYPE_EXTRACTOR = "extractor";
    public static final String TYPE_SEARCH_AND_REPLACE = "replace";

    public TestResult testRegexFilteringInterceptor(Map<String, Object> configMap, List<String> testStrings) throws IOException {
        TestResult testResult = new TestResult();
        testResult.setType(TestResult.Type.MATCH);

        Context context = new Context();
        if(configMap.containsKey(CONFIG_FILTERING_REGEX)) {
            context.put(RegexFilteringInterceptor.Constants.REGEX, (String) configMap.get(CONFIG_FILTERING_REGEX));
        }
        if(configMap.containsKey(CONFIG_FILTERING_EXCLUDE_EVENTS)) {
            context.put(RegexFilteringInterceptor.Constants.EXCLUDE_EVENTS, String.valueOf(configMap.get(CONFIG_FILTERING_EXCLUDE_EVENTS)));
        }

        RegexFilteringInterceptor.Builder regexFilteringInterceptor = new RegexFilteringInterceptor.Builder();
        regexFilteringInterceptor.configure(context);
        Interceptor interceptor = regexFilteringInterceptor.build();
        interceptor.initialize();

        MatchResult matchResult = new MatchResult();
        for(int i = 0; i < testStrings.size(); i++) {
            String testString = testStrings.get(i);
            Event event = EventBuilder.withBody(testString.getBytes());
            event = interceptor.intercept(event);
            if(event != null) {
                matchResult.getResultList().add(true);
            } else {
                matchResult.getResultList().add(false);
            }
        }
        testResult.setResult(matchResult);

        return testResult;
    }

    public TestResult testRegexExtractorInterceptor(Map<String, Object> configMap, List<String> testStrings) throws IOException {
        TestResult testResult = new TestResult();
        testResult.setType(TestResult.Type.GROUP);

        String flumeContextString = (String) configMap.get(CONFIG_EXTRACTOR_FLUME_CONTEXT);
        Properties props = new Properties();
        props.load(new StringReader(flumeContextString.replace("\\","\\\\")));

        Context context = new Context((Map)props);
        RegexExtractorInterceptor.Builder builder = new RegexExtractorInterceptor.Builder();
        builder.configure(context);
        Interceptor interceptor = builder.build();

        GroupResult groupResult = new GroupResult();
        for(String testString : testStrings) {
            Event event = EventBuilder.withBody(testString.getBytes());
            event.setHeaders(new LinkedHashMap<String, String>());
            event = interceptor.intercept(event);

            int group = 0;
            List<String> row = new ArrayList(event.getHeaders().size());
            for(Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
                String headerName = entry.getKey();
                String value = entry.getValue();
                row.add(value);

                if(group == 0) {
                    groupResult.getColumns().add(headerName);
                }
            }

            if(row.size() > 0) {
                GroupResult.GroupsList groupsList = new GroupResult.GroupsList(row);
                groupResult.getResultList().add(groupsList);
            } else {
                groupResult.getResultList().add(null);
            }
        }

        testResult.setResult(groupResult);
        return testResult;
    }

    public TestResult testRegexSearchAndReplaceInterceptor(Map<String, Object> configMap, List<String> testStrings) throws IOException {
        TestResult testResult = new TestResult();
        testResult.setType(TestResult.Type.STRING);

        Context context = new Context();
        Charset charset = Charset.defaultCharset();
        if(configMap.containsKey(CONFIG_REPLACE_CHARSET) && !String.valueOf(configMap.get(CONFIG_REPLACE_CHARSET)).isEmpty()) {
            String charsetName = String.valueOf(configMap.get(CONFIG_REPLACE_CHARSET));
            context.put(CONFIG_REPLACE_CHARSET, charsetName);
            charset = Charset.forName(charsetName);
        }
        context.put(CONFIG_REPLACE_PATTERN, String.valueOf(configMap.get(CONFIG_REPLACE_PATTERN)));
        context.put(CONFIG_REPLACE_STRING, String.valueOf(configMap.get(CONFIG_REPLACE_STRING)));

        SearchAndReplaceInterceptor.Builder builder = new SearchAndReplaceInterceptor.Builder();
        builder.configure(context);
        Interceptor interceptor = builder.build();
        interceptor.initialize();

        StringResult stringResult = new StringResult();
        for(int i = 0; i < testStrings.size(); i++) {
            String testString = testStrings.get(i);
            Event event = EventBuilder.withBody(testString.getBytes());
            event = interceptor.intercept(event);
            String replaced = new String(event.getBody(), charset);
            stringResult.getResultList().add(replaced);
        }
        testResult.setResult(stringResult);

        return testResult;
    }

    public TestResult testRegex(Map<String, Object> configMap, List<String> testStrings) throws Exception {
        String testType = (String) configMap.get(CONFIG_TYPE);
        if(testType == null) {
            throw new InvalidConfigException(String.format("'%s' parameter doesn't exists.", CONFIG_TYPE));
        }

        if (testType.equals(TYPE_FILTERING)) {
            return testRegexFilteringInterceptor(configMap, testStrings);
        } else if (testType.equals(TYPE_EXTRACTOR)) {
            return testRegexExtractorInterceptor(configMap, testStrings);
        } else if (testType.equals(TYPE_SEARCH_AND_REPLACE)) {
            return testRegexSearchAndReplaceInterceptor(configMap, testStrings);
        } else {
            throw new InvalidConfigException(String.format("Unsupported Test Type: %s", testType));
        }
    }

    public static void main(String args[]) {
        Tester tester = new FlumeTester();
        tester.testMain(args[0], args[1]);
    }
}
