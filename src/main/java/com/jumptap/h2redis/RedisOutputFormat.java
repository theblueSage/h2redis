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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

public class RedisOutputFormat extends OutputFormat<Text, Text> {
    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context)
        throws IOException, InterruptedException {
        return new RedisHMRecordWriter(context.getConfiguration());
    }

    @Override
    public void checkOutputSpecs(JobContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String host = conf.get(RedisDriver.REDIS_HOST);
        int key = conf.getInt(RedisDriver.REDIS_KEY_FIELD, -1);
        int hash = conf.getInt(RedisDriver.REDIS_HASHKEY_FIELD, -1);
        int val = conf.getInt(RedisDriver.REDIS_HASHVAL_FIELD, -1);
        if (host == null || host.isEmpty() || key == -1 || hash == -1 || val == -1)
            throw new IOException("Missing configuration param, check usage.");
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
        throws IOException, InterruptedException {
        return (new NullOutputFormat<Text, Text>()).getOutputCommitter(context);
    }
}
