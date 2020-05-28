// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.stream.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

public class LogSpout implements ISpout {

    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();
    private final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final String[] userAgents = {
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)",
            "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727)",
            "Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
            "Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
            "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
            "Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
            "Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
            "Mozilla/4.0 (compatible; MSIE6.0; Windows NT 5.0; .NET CLR 1.1.4322)",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_3 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B511 Safari/9537.53",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_3 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B511 Safari/9537.53",
            "Mozilla/5.0 (Linux; Android 4.2.1; Galaxy Nexus Build/JOP40D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Mobile Safari/535.19",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36",
            "-",
            "-",
            "-"
    };

    private final String[] urlPaths = {
            "login.php",
            "login.php",
            "login.php",
            "login.php",
            "login.php",
            "view.php",
            "view.php",
            "view.php",
            "view.php",
            "list.php",
            "list.php",
            "list.php",
            "upload.php",
            "upload.php",
            "edit.php",
            "index.php",
            "index.php",
            "index.php",
            "index.php",
            "index.php",
            "index.php",
            "admin/login.php",
            "admin/login.php",
            "admin/manage.php",
            "admin/backdoor.php"
    };

    private final String[] httpRefers = {
            "http://www.baidu.com/s?wd=%s",
            "http://www.baidu.com/s?wd=%s",
            "http://www.baidu.com/s?wd=%s",
            "http://www.baidu.com/s?wd=%s",
            "http://www.google.cn/search?q=%s",
            "http://www.sogou.com/web?query=%s",
            "http://www.sogou.com/web?query=%s",
            "http://www.sogou.com/web?query=%s",
            "http://one.cn.yahoo.com/s?p=%s",
            "http://cn.bing.com/search?q=%s",
            "http://cn.bing.com/search?q=%s"
    };

    private final String[] searchedKeywords = {
            "spark",
            "spark",
            "spark",
            "hadoop",
            "hadoop",
            "hadoop",
            "hive",
            "hive",
            "spark%20mlib",
            "spark%20sql",
            "awesome%20website",
            "awesome%20website",
            "awesome%20website",
            "awesome%20website",
            "awesome%20website",
    };

    private final String[] ipSlices = {
            "10", "29", "30", "46", "55", "63", "72", "87", "98", "132", "156", "124",
            "167", "143", "187", "168", "190", "201", "202", "214", "215", "222"
    };

    private String sampleIp() {
        String[] ip = new String[4];
        for (int i = 0; i < 4; ++i) {
            ip[i] = ipSlices[rnd.nextInt(ipSlices.length)];
        }
        return String.join(".", ip);
    }

    private String sampleUrl() {
        return urlPaths[rnd.nextInt(urlPaths.length)];
    }

    private String sampleUserAgent() {
        return userAgents[rnd.nextInt(userAgents.length)];
    }

    private String sampleRefer() {
        if (rnd.nextFloat() > 0.3) {
            return "-";
        }

        String referStr = httpRefers[rnd.nextInt(httpRefers.length)];
        String queryStr = searchedKeywords[rnd.nextInt(searchedKeywords.length)];
        return String.format(referStr, queryStr);
    }

    @Override
    public void nextTuple(OutputCollector collector) {
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
        String timeStr = fmt.format(LocalDateTime.now());
        String log = String.format("%s - - [%s] \"GET /%s HTTP/1.1\" 200 0 \"%s\" \"%s\" \"-\" ",
                sampleIp(), timeStr, sampleUrl(), sampleRefer(), sampleUserAgent());
        collector.emit("logStream", new Value("log", log));
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer.addSchema("logStream", new Field("log", FieldType.STRING));
    }
}
