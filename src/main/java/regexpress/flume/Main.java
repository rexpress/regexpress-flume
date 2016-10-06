package regexpress.flume;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.RegexFilteringInterceptor;
import regexpress.common.MatchResult;
import regexpress.common.TestResult;
import regexpress.common.TestResultType;
import regexpress.common.Tester;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class Main implements Tester {

    private static Gson gson = new Gson();

    public TestResult testRegexFilteringInterceptor(Map<String, Object> configJsonObject, List<String> testStrings) {
        TestResult testResult = new TestResult();
        testResult.setResultType(TestResultType.MATCH);

        RegexFilteringInterceptor.Builder regexFilteringInterceptor = new RegexFilteringInterceptor.Builder();
        Context context = new Context();
        context.put(RegexFilteringInterceptor.Constants.REGEX, (String) configJsonObject.get(RegexFilteringInterceptor.Constants.REGEX));
        if(configJsonObject.containsKey(RegexFilteringInterceptor.Constants.EXCLUDE_EVENTS)) {
            context.put(RegexFilteringInterceptor.Constants.EXCLUDE_EVENTS, (String) configJsonObject.get(RegexFilteringInterceptor.Constants.EXCLUDE_EVENTS));
        }
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
        testResult.setTestResult(matchResult);

        return testResult;
    }

    public TestResult testRegexExtractorInterceptor(Map<String, Object> configJsonObject, List<String> testStrings) {
        TestResult testResult = new TestResult();
        testResult.setResultType(TestResultType.GROUP);

        return testResult;
    }

    public TestResult testRegex(String configJsonString, String testJsonString) {
        Type mapType = new TypeToken<Map<String, Object>>(){}.getType();
        Type listType = new TypeToken<List<String>>(){}.getType();
        Map<String, Object> configJsonObject = gson.fromJson(configJsonString, mapType);
        List<String> testStrings = gson.fromJson(testJsonString, listType);

        Boolean filtering = (Boolean) configJsonObject.get("filtering");
        if(filtering) {
            return testRegexFilteringInterceptor(configJsonObject, testStrings);
        } else {
            return testRegexExtractorInterceptor(configJsonObject, testStrings);
        }
    }

    public static void main(String args[]) {
        TestResult result = null;

        try {
            result = new Main().testRegex(args[0], args[1]);
        } catch (Exception e) {
            result = new TestResult();
            result.setException(e);
        }

        if(result != null) {
            System.out.println(gson.toJson(result));
        }
    }
}
