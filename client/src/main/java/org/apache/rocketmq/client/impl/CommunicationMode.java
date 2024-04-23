/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl;

public enum CommunicationMode {
    SYNC, // 同步，Producer 将 Message 发送出去之后，会等待 Broker 的返回，然后再发送下一条消息
    ASYNC, // 异步，Producer 发送消息就不会等待 Broker 的返回了，而是会通过回调的方式来处理 Broker 的响应
    ONEWAY, // 单向，Producer 就只管发送消息，不会关心 Broker 的返回，也没有任何回调函数，活像个“渣男”。不过相应的，由于不用处理返回结果，此模式的性能会非常好，类似于日志收集的场景可以考虑使用 ONEWAY 模式
}
