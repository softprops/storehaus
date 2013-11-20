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
import com.twitter.storehaus.Store
import com.twitter.zk.{ ZkClient, ZNode }
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.KeeperException

/**
 *  @author Doug Tangren
 */

object DataStore {
  def apply(client: ZkClient)(implicit timer: Timer) =
    new DataStore(client)(timer)
}

class DataStore(val client: ZkClient)(implicit timer: Timer)
  extends Store[String, Array[Byte]] {

  override def get(k: String): Future[Option[Array[Byte]]] =
    client(k).sync.flatMap { node =>
      node.getData().map {
        case ZNode.Data(_, _, bytes) => Some(bytes)
      }.handle {
        case ZNode.Error(_) => None
      }
    }

  override def put(kv: (String, Option[Array[Byte]])): Future[Unit] =
    kv match {
      case (key, Some(value)) =>
        client(key).create(value).unit.handle {
          case e: KeeperException.InvalidACLException =>
            () // what to do with invalid acl exception
          case e @ ZNode.Error(path) =>
            () // todo: what todo in other failure cases
        }
      case (key, None) =>
        client(key).sync.flatMap { node =>
          node.exists().map {
            case ZNode.Exists(path, stat) =>
              node.delete(stat.getVersion)
          }.handle {
            case e: KeeperException.NoNodeException =>
              () // todo: what todo when node does not exist
            case e @ ZNode.Error(path) =>
              () // todo: what to do in other failure cases
          }
        }.unit
    }

  override def close(time: Time) =
    client.release.within(Duration.fromMilliseconds(time.inMillis))
}
