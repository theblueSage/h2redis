/*
*  Copyright (c) 2013, Jumptap (http://www.jumptap.com) All Rights Reserved.
*
*  Jumptap licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package com.jumptap.h2redis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RedisDriver extends Configured implements Tool {
    public static final String REDIS_HOST = "com.jumptap.h2redis.redis.host";
    public static final String REDIS_PORT = "com.jumptap.h2redis.redis.port";
    public static final String REDIS_DB = "com.jumptap.h2redis.redis.database";
    public static final String REDIS_PW = "com.jumptap.h2redis.redis.password";
    public static final String REDIS_KEY_FIELD = "com.jumptap.h2redis.redis.key.field";
    public static final String REDIS_KEY_PREFIX = "com.jumptap.h2redis.redis.key.prefix";
    public static final String REDIS_KEY_PREFIX_DELIM = "com.jumptap.h2redis.redis.key.prefix.delim";
    public static final String REDIS_HASHKEY_FIELD = "com.jumptap.h2redis.redis.hash.key.field";
    public static final String REDIS_HASHKEY_PREFIX = "com.jumptap.h2redis.redis.hash.key.prefix";
    public static final String REDIS_HASHVAL_FIELD = "com.jumptap.h2redis.redis.hash.val.field";
    public static final String REDIS_KEY_FILTER = "com.jumptap.h2redis.redis.key.filter";
    public static final String REDIS_HASH_FILTER = "com.jumptap.h2redis.redis.hash.filter";
    public static final String REDIS_VAL_FILTER = "com.jumptap.h2redis.redis.val.filter";
    public static final String REDIS_KEY_TTL = "com.jumptap.h2redis.redis.key.ttl";
    public static final String REDIS_KEY_TS = "com.jumptap.h2redis.redis.key.ts";
    private static final String REDIS_CMD = "-redis";
    private static final String INPUT_CMD = "-input";
    private static final String REDIS_PW_CMD = "-pw";
    private static final String REDIS_DB_CMD = "-db";
    private static final String KEY_CMD = "-key";
    private static final String KEY_PFX_CMD = "-pkey";
    private static final String KEY_PFX_DELIM_CMD = "-delim";
    private static final String HASH_KEY_CMD = "-hkey";
    private static final String HASH_KEY_PFX_CMD = "-hpkey";
    private static final String HASH_VAL_CMD = "-hval";
    private static final String KEY_FILTER_CMD = "-kf";
    private static final String HASH_FILTER_CMD = "-hf";
    private static final String VAL_FILTER_CMD = "-vf";
    private static final String TTL_CMD = "-ttl";
    private static final String TS_KEY_CMD = "-tsk";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RedisDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 5) {
            usage();
            return 1;
        }

        Map<String, String> argMap = new HashMap<String, String>();
        String[] kv;

        for (String arg : args) {
            kv = arg.split("=");
            if (kv.length != 2) {
                usage();
                return 1;
            }
            argMap.put(kv[0].trim(), kv[1]);
        }

        Configuration conf = getConf();
        String[] hostPort = argMap.get(REDIS_CMD).split(":");
        conf.set(REDIS_HOST, hostPort[0].trim());
        conf.setInt(REDIS_PORT, Integer.valueOf(hostPort[1].trim()));
        conf.setInt(REDIS_KEY_FIELD, Integer.valueOf(argMap.get(KEY_CMD).trim()));
        conf.setInt(REDIS_HASHKEY_FIELD, Integer.valueOf(argMap.get(HASH_KEY_CMD).trim()));
        conf.setInt(REDIS_HASHVAL_FIELD, Integer.valueOf(argMap.get(HASH_VAL_CMD).trim()));

        if (argMap.containsKey(REDIS_DB_CMD)) {
            conf.set(REDIS_DB, argMap.get(REDIS_DB_CMD).trim());
        }
        if (argMap.containsKey(REDIS_PW_CMD)) {
            conf.set(REDIS_PW, argMap.get(REDIS_PW_CMD).trim());
        }
        if (argMap.containsKey(KEY_PFX_CMD)) {
            conf.set(REDIS_KEY_PREFIX, argMap.get(KEY_PFX_CMD).trim());
        }
        if (argMap.containsKey(HASH_KEY_PFX_CMD)) {
            conf.set(REDIS_HASHKEY_PREFIX, argMap.get(HASH_KEY_PFX_CMD).trim());
        }
        if (argMap.containsKey(KEY_PFX_DELIM_CMD)) {
            conf.set(REDIS_KEY_PREFIX_DELIM, argMap.get(KEY_PFX_DELIM_CMD).trim());
        }
        if (argMap.containsKey(KEY_FILTER_CMD)) {
            conf.setPattern(REDIS_KEY_FILTER, Pattern.compile(argMap.get(KEY_FILTER_CMD).trim()));
        }
        if (argMap.containsKey(HASH_FILTER_CMD)) {
            conf.setPattern(REDIS_HASH_FILTER, Pattern.compile(argMap.get(HASH_FILTER_CMD).trim()));
        }
        if (argMap.containsKey(VAL_FILTER_CMD)) {
            conf.setPattern(REDIS_VAL_FILTER, Pattern.compile(argMap.get(VAL_FILTER_CMD).trim()));
        }
        if (argMap.containsKey(VAL_FILTER_CMD)) {
            conf.setPattern(REDIS_VAL_FILTER, Pattern.compile(argMap.get(VAL_FILTER_CMD).trim()));
        }
        if (argMap.containsKey(TTL_CMD)) {
            conf.setInt(REDIS_KEY_TTL, Integer.valueOf(argMap.get(TTL_CMD).trim()));
        }
        if (argMap.containsKey(TS_KEY_CMD)) {
            conf.set(REDIS_KEY_TS, argMap.get(TS_KEY_CMD).trim());
        }
        else {
            conf.set(REDIS_KEY_TS, "redis.lastupdate");
        }

        Job job = new Job(conf, "RedisDriver");
        FileInputFormat.addInputPath(job, new Path(argMap.get(INPUT_CMD)));
        job.setJarByClass(RedisDriver.class);
        job.setMapperClass(RedisOutputMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(RedisOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void usage() {
        System.err.println("" +
            "Usage: h2redis " +
            REDIS_CMD + " <host:port> " +
            KEY_CMD + " <int> " +
            HASH_KEY_CMD + " <int> " +
            HASH_VAL_CMD + " <int> " +
            INPUT_CMD + " <path> " +
            TTL_CMD + " <ttl in seconds> " +
            TS_KEY_CMD + " <key of last update timestamp> " +
            REDIS_PW_CMD + " <optional password> " +
            REDIS_DB_CMD + " <optional database, default is 0> " +
            KEY_PFX_CMD + " <optional primary key prefix ie: foo.key> " +
            HASH_KEY_PFX_CMD + " <optional hash key prefix ie: foo.hkey> " +
            KEY_PFX_DELIM_CMD + " <optional delimiter, used when concatenating a key prefix and a key, default is \".\"> ");
        ToolRunner.printGenericCommandUsage(System.err);
    }
}
