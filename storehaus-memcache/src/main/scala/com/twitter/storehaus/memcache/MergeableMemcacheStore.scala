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

package com.twitter.storehaus.memcache

import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.finagle.memcached.Client
import com.twitter.storehaus.ConvertedStore
import com.twitter.storehaus.algebra.MergeableStore
import com.twitter.util.{ Duration, Future }

import org.jboss.netty.buffer.ChannelBuffer

import scala.util.{ Failure, Success, Try }

/** Factory for [[com.twitter.storehaus.memcache.MergeableMemcacheStore]] instances. */
object MergeableMemcacheStore {

  // max retries for merge/cas operation
  // this is to support multiple concurrent writers
  val MAX_RETRIES = 10

  def apply[V](client: Client, ttl: Duration = MemcacheStore.DEFAULT_TTL, flag: Int = MemcacheStore.DEFAULT_FLAG,
      maxRetries: Int = MAX_RETRIES)
      (inj: Injection[V, ChannelBuffer], semigroup: Semigroup[V]) =
    new MergeableMemcacheStore[V](MemcacheStore(client, ttl, flag), maxRetries)(inj, semigroup)
}

/** Returned when merge fails after a certain number of retries */
class MergeFailedException(val key: String)
  extends RuntimeException("Merge failed for key " + key)

/**
 * Mergeable MemcacheStore that uses CAS.
 *
 * The store supports multiple concurrent writes to the same key, but you might
 * see a performance hit if there are too many concurrent writes to a hot key.
 * The solution is to group by a hot key, and use only a single (or few) writers to that key.
 */
class MergeableMemcacheStore[V](underlying: MemcacheStore, maxRetries: Int)(implicit inj: Injection[V, ChannelBuffer],
    override val semigroup: Semigroup[V])
  extends ConvertedStore[String, String, ChannelBuffer, V](underlying)(identity)
  with MergeableStore[String, V] {

  // NOTE: we might want exponential backoff if there are a lot of retries.
  // use a timer to wait a random interval between [0,t), then [0,2t), then [0,4t), then [0,16t), etc...

  // retryable merge
  protected def doMerge(kv: (String, V), currentRetry: Int) : Future[Option[V]] =
    (currentRetry > maxRetries) match {
      case false => // use 'gets' api to obtain casunique token
        underlying.client.gets(kv._1).flatMap { res : Option[(ChannelBuffer, ChannelBuffer)] =>
          res match {
            case Some((cbValue, casunique)) =>
              inj.invert(cbValue) match {
                case Success(v) => // attempt cas
                  val resV = semigroup.plus(v, kv._2)
                  underlying.client.cas(kv._1, inj.apply(resV), casunique).flatMap { success =>
                    success.booleanValue match {
                      case true => Future.value(Some(v))
                      case false => doMerge(kv, currentRetry + 1) // retry
                    }
                  }
                case Failure(ex) => Future.exception(ex)
              }
            // no pre-existing value, try to 'add' it
            case None =>
              underlying.client.add(kv._1, inj.apply(kv._2)).flatMap { success =>
                success.booleanValue match {
                  case true => Future.None
                  case false => doMerge(kv, currentRetry + 1) // retry, next retry should call cas
                }
              }
          }
        }
      // no more retries
      case true => Future.exception(new MergeFailedException(kv._1))
    }

  override def merge(kv: (String, V)): Future[Option[V]] = doMerge(kv, 1)
}

