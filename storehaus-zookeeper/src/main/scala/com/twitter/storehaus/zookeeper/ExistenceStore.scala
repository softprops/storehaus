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

import com.twitter.util.{ Duration, Future, Time, Timer }
import com.twitter.storehaus.ReadableStore
import com.twitter.zk.{ ZkClient, ZNode }
import org.apache.zookeeper.data.Stat

/**
 *  @author Doug Tangren
 */

object ExistenceStore {
  def apply(client: ZkClient)(implicit timer: Timer) =
    new ExistenceStore(client)(timer)
}

class ExistenceStore(val client: ZkClient)(implicit timer: Timer)
  extends ReadableStore[String, Stat] {

  override def get(k: String): Future[Option[Stat]] =
    client(k).sync.flatMap { node =>
      node.exists().map {
        case ZNode.Exists(path, stat) => Option(stat)
      }.handle {
        case ZNode.Error(_) => None
      }
    }

  override def close(time: Time) =
    client.release.within(Duration.fromMilliseconds(time.inMillis))
}

