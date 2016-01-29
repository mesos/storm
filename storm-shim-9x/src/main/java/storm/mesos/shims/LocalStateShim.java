/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.mesos.shims;

import backtype.storm.utils.LocalState;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LocalStateShim implements ILocalStateShim {
  LocalState _state;
  private static final Logger LOG = Logger.getLogger(LocalStateShim.class);

  public LocalStateShim(String localDir) throws IOException {
    _state = new LocalState(localDir);
  }

  public String get(String key) {
    String retVal = null;
    try {
      retVal = (String) _state.get(key);
    } catch (IOException e) {
      LOG.error("Halting process...", e);
      Runtime.getRuntime().halt(1);
    }
    return retVal;
  }

  public void put(String key, String value) {
    try {
      _state.put(key, value);
    } catch (IOException e) {
      LOG.error("Halting process...", e);
      Runtime.getRuntime().halt(1);
    }
  }

  public void remove(String key) {
    try {
      _state.remove(key);
    } catch (IOException e) {
      LOG.error("Halting process...", e);
      Runtime.getRuntime().halt(1);
    }
  }

  public Map<String, String> snapshot() {
    Map<Object, Object> snapshot = null;
    try {
      snapshot = _state.snapshot();
    } catch (IOException e) {
      LOG.error("Halting process...", e);
      Runtime.getRuntime().halt(1);
    }
    HashMap<String, String> retVal = new HashMap<>();
    for (Object key: snapshot.keySet()) {
      retVal.put((String) key, get((String) key));
    }
    return retVal;
  }

}
