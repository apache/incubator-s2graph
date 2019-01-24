<!---
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
--->
# S2Graph HTTP Layer

## Development Setup

### Run Server 
Let's run http server.

```bash
sbt 'project s2http' '~reStart'
```

When the server is running, connect to `http://localhost:8000`. If it works normally, you can see the following screen.

```json
{
  "port": 8000,
  "started_at": 1543218853354
}
```

### API testing
```test
sbt 'project s2http' "test-only *s2graph.http*"
```

### API List
  - link to S2GraphDocument
