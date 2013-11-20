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

/**
 *  @author Doug Tangren
 */

object ChildrenStore {
  def apply(client: ZkClient)(implicit timer: Timer) =
    new ChildrenStore(client)(timer)
}

class ChildrenStore(val client: ZkClient)(implicit timer: Timer)
  extends Store[String, Seq[String]] {

  override def get(k: String): Future[Option[Seq[String]]] =
    client(k).sync.flatMap {
      _.getChildren().map {
        case ZNode.Children(_, _, children) =>
          Some(children.map(_.path))
      }.handle({
        case ZNode.Error(_) => None
      })
    }

  override def put(kv: (String, Option[Seq[String]])): Future[Unit] =
    kv match {
      case (path, Some(children)) =>
        client(path).sync.flatMap {
          _.exists().map(_(children).children.map({ node =>
            println("creating node %s" format node)
            node.create()
          }))
        }.unit
      case (path, None) =>
        // TODO: how to delete children
        Future.value(Nil)
    }

  override def close(time: Time) =
    client.release.within(Duration.fromMilliseconds(time.inMillis))
}
