package express.regular.flume.test;

import com.google.gson.Gson;
import express.regular.common.GroupResult;
import express.regular.common.MatchResult;
import express.regular.common.TestResult;
import express.regular.flume.FlumeTester;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlumeTesterTest {

    @Test
    public void flumeFilteringTest() {
        Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(FlumeTester.CONFIG_TYPE, FlumeTester.TYPE_FILTERING);
        configMap.put(FlumeTester.CONFIG_FILTERING_REGEX,  "([a-zA-Z]*) ([a-zA-Z]*) ([a-zA-Z]*)");
        configMap.put(FlumeTester.CONFIG_FILTERING_EXCLUDE_EVENTS, "false");

        List<String> testMap = Arrays.asList(new String[]{"Hello Test String", "Hello2 Test2 String2"});

        Gson gson = new Gson();
        String configJsonString = gson.toJson(configMap);
        String testJsonString = gson.toJson(testMap);

        FlumeTester flumeTester = new FlumeTester();
        TestResult testResult = flumeTester.testMain(configJsonString, testJsonString);
        Assert.assertEquals(testResult.getType(), TestResult.Type.MATCH);
        Assert.assertNotNull(testResult.getResult());
        MatchResult matchResult = (MatchResult) testResult.getResult();
        Assert.assertEquals(matchResult.getResultList().get(0), true);
        Assert.assertEquals(matchResult.getResultList().get(1), false);
    }

    @Test
    public void flumeExtractorTest() {
        Map<String, Object> configMap = new HashMap<String, Object>();
        configMap.put(FlumeTester.CONFIG_TYPE, FlumeTester.TYPE_EXTRACTOR);
        StringBuffer flumeContext = new StringBuffer();
        flumeContext.append("regex=([a-zA-Z]*) ([a-zA-Z]*) ([a-zA-Z]*)").append("\n")
                .append("serializers=s1 s2 s3").append("\n")
                .append("serializers.s1.name=A").append("\n")
                .append("serializers.s1.type=DEFAULT").append("\n")
                .append("serializers.s2.name=B").append("\n")
                .append("serializers.s2.type=DEFAULT").append("\n")
                .append("serializers.s3.name=C").append("\n")
                .append("serializers.s3.type=DEFAULT").append("\n");

        configMap.put(FlumeTester.CONFIG_EXTRACTOR_FLUME_CONTEXT, flumeContext.toString());

        List<String> testStrings = Arrays.asList(new String[]{"Hello Test String", "Hello2 Test2 String2"});

        Gson gson = new Gson();
        String configJsonString = gson.toJson(configMap);
        String testJsonString = gson.toJson(testStrings);

        FlumeTester flumeTester = new FlumeTester();
        TestResult testResult = flumeTester.testMain(configJsonString, testJsonString);

        Assert.assertEquals(testResult.getType(), TestResult.Type.GROUP);
        Assert.assertNotNull(testResult.getResult());
        GroupResult groupResult = (GroupResult) testResult.getResult();
        Assert.assertArrayEquals(groupResult.getColumns().toArray(), new String[]{"A", "B", "C"});
        Assert.assertArrayEquals(groupResult.getResultList().get(0).getGroups(0).toArray(), new String[]{"Hello", "Test", "String"});
        Assert.assertNull(groupResult.getResultList().get(1));
    }
}