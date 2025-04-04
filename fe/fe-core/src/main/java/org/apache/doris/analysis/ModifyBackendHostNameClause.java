// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

public class ModifyBackendHostNameClause extends ModifyNodeHostNameClause {
    public ModifyBackendHostNameClause(String hostPort, String hostName) {
        super(hostPort, hostName);
    }

    public ModifyBackendHostNameClause(String hostPort, String hostName,
            String host, int port) {
        super(hostPort, hostName);
        this.host = host;
        this.port = port;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER SYSTEM MODIFY BACKEND \"");
        sb.append(hostPort).append("\"");
        sb.append(" HOSTNAME ").append("\"");
        sb.append(newHost).append("\"");
        return sb.toString();
    }
}
