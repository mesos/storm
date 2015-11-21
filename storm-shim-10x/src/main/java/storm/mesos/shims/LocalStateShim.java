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

import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.JavaObject;
import backtype.storm.generated.JavaObjectArg;
import backtype.storm.utils.LocalState;
import org.apache.thrift7.TBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalStateShim implements ILocalStateShim {
  LocalState _state;

  public LocalStateShim(String localDir) throws IOException {
    _state = new LocalState(localDir);
  }

  public String get(String key) {
    TBase tBase = _state.get(key);
    if (tBase != null) {
      JavaObject jo = ((JavaObject) tBase);
      ComponentObject co = ComponentObject.java_object(jo);
      List<JavaObjectArg> l = jo.get_args_list();
      if (l != null && l.size() >= 0 && l.get(0) != null) {
        return l.get(0).get_string_arg();
      }
    }
    return null;
  }

  public void put(String key, String value) {
    ArrayList<JavaObjectArg> tmpList = new ArrayList<JavaObjectArg>();
    tmpList.add(JavaObjectArg.string_arg(value));
    _state.put(key, new JavaObject("java.lang.String", tmpList));
  }

  public void remove(String key) {
    _state.remove(key);
  }

  public Map<String, String> snapshot() {
    Map<String, TBase> snapshot = _state.snapshot();
    HashMap<String, String> retVal = new HashMap<>();
    for (String key: snapshot.keySet()) {
      retVal.put(key, get(key));
    }
    return retVal;
  }

}
