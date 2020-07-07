package com.lucas.mapreduce.WebLogETL;

/**
 * 194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
 * 58.215.204.118 - - [18/Sep/2013:06:52:32 +0000] "-" 400 0 "-" "-"
 * @author lucas
 * @create 2020-07-07-14:21
 */
public class testSplit {
    public static void main(String[] args) {
        String line = "58.215.204.118 - - [18/Sep/2013:06:51:36 +0000] \"GET /wp-content/uploads/2013/08/chat2.png HTTP/1.1\" 200 59852 \"http://blog.fens.me/nodejs-socketio-chat/\" \"Mozilla/5.0 (Windows NT 5.1; rv:23.0) Gecko/20100101 Firefox/23.0\"\n";
//        String line = "58.215.204.118 - - [18/Sep/2013:06:52:32 +0000] \"-\" 400 0 \"-\" \"-\"\n";
        String[] split = line.split("\"");
        for (String s : split) {
            System.out.println("<"+s+">");
        }
            System.out.println("xxxxxxxxx");
        System.out.println(split[split.length-2]);

        LogBean logBean = new LogBean();
        String[] item1 = split[0].split(" ");
        logBean.setRemote_addr(item1[0]);
        logBean.setRemote_user(split[0].split("-")[1]);
        logBean.setTime_local(item1[3].substring(1));
        logBean.setRequest(split[1]);
        logBean.setStatus(split[2].trim().split(" ")[0]);
        logBean.setBody_bytes_sent(split[2].trim().split(" ")[1]);
        logBean.setHttp_referer(split[3]);
        logBean.setHttp_user_agent(split[5]);

        if (Integer.parseInt(logBean.getStatus()) >= 400) {
            logBean.setValid(false);
        }

        System.out.println(logBean.toString());

    }
}
