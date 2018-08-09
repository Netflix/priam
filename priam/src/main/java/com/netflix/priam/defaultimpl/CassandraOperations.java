/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.defaultimpl;

import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;

import javax.inject.Inject;
import java.util.*;

/**
 * Created by aagrawal on 7/23/18.
 */
public class CassandraOperations
{
    private IConfiguration configuration;

    @Inject
    CassandraOperations(IConfiguration configuration){
        this.configuration = configuration;
    }

    public List<String> getKeyspaces() throws Exception{
        return new RetryableCallable<List<String>>(){
            public List<String> retriableCall() throws Exception{
                try(JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                    return nodeTool.getKeyspaces();
                }
            }
        }.call();
    }

    public Map<String, List<String>> getColumnfamilies() throws Exception{
        return new RetryableCallable<Map<String, List<String>>>(){
            public Map<String, List<String>> retriableCall() throws Exception{
                try(JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                    final Map<String, List<String>> columnfamilies = new HashMap<>();
                    Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> columnfamilyStoreMBean = nodeTool.getColumnFamilyStoreMBeanProxies();
                    columnfamilyStoreMBean.forEachRemaining(entry -> {
                     columnfamilies.putIfAbsent(entry.getKey(), new ArrayList<>());
                     columnfamilies.get(entry.getKey()).add(entry.getValue().getColumnFamilyName());
                    });
                    return columnfamilies;
                }
            }
        }.call();
    }

    public void forceKeyspaceCompaction(String keyspaceName, String... columnfamilies) throws Exception{
        new RetryableCallable<Void>(){
            public Void retriableCall() throws Exception{
                try(JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                        nodeTool.forceKeyspaceCompaction(false, keyspaceName, columnfamilies);
                        return null;
                }
            }
        }.call();
    }

    public void forceKeyspaceFlush(String keyspaceName) throws Exception{
        new RetryableCallable<Void>(){
            public Void retriableCall() throws Exception{
                try(JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                   nodeTool.forceKeyspaceFlush(keyspaceName, new String[0]);
                    return null;
                }
            }
        }.call();
    }
}