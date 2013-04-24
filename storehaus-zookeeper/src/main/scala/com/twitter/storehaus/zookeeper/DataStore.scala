/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.storehaus.zookeeper

import com.twitter.util.Future
import com.twitter.storehaus.Store
import com.twitter.zk.ZkClient
import org.apache.zookeeper.data.Stat

/**
 *  @author Doug Tangren
 */

object DataStore {
  def apply(client: ZkClient) =
    new DataStore(client)
}

class DataStore(val client: ZkClient)
  extends Store[String, (Stat, Array[Byte])] {

  override def get(k: String): Future[Option[(Stat, Array[Byte])]] =
    client(k).sync.flatMap { node =>
      node.getData().map {
        case data =>
          Some((data.stat, data.bytes))
      }
    }

  override def put(kv: (String, Option[(Stat, Array[Byte])])): Future[Unit] =
    kv match {
      case (key, Some(value)) =>
        client(key).sync.map { _(value._1, value._2) }.unit
      case (key, None) =>
        client(key).sync.flatMap { node =>
          node.getData().map {
            case data =>
              node.delete(data.stat.getVersion)
          }          
        }.unit
    }

  override def close { client.release }
}
