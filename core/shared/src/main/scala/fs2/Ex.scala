/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2

import cats.effect._
import cats.effect.std._
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

object Ex {

  // I have settled on this type, which is the safest to use
  // (plus some additional dials perhaps)
  def partition[F[_]: Concurrent, I, K, O](select: I => K)(f: K => Pipe[F, I, O]): Pipe[F, I, O] = ???

  // open questions around:
  // do we introduce any backpressure?
  // do we limit concurrency, and/or fail on too many keys?
  // question around partitions closing and reopening:
  // e.g. take(3) on a partition, and then a new element for that key arrives
  // should the selector be effectful
  


  // first implementation uses a map of queues, which forces boxing in Option per element on write
  // the Stream.fromQueue will do some rechunking though
  // no backpressure atm
  def p1[F[_]: Concurrent, A, K, O](select: A => K)(f: K => Pipe[F, A, O]): Pipe[F, A, O] =
  in => {
    Stream.eval(Concurrent[F].ref(Map.empty[K, Queue[F, Option[A]]])).flatMap { state =>
      in
        .noneTerminate
        .evalMap { a =>
          state.get.flatMap { queues =>
            a match {
              case Some(a) =>
                val k = select(a)
                // safe to split `get` and `set` here, as there is only
                // one stream modifying the Ref, in a sequential evalMap pattern
                // EDIT: broken, because we want to delete the queue when the
                // stream has finalised
                queues.get(k) match {
                  case None =>
                    Queue
                      .unbounded[F, Option[A]]
                      .flatMap { q =>
                        state.set(queues + (k -> q)) >>
                        q.offer(a.some).as((k, q).some)
                      }
                  case Some(q) => q.offer(a.some).as(none[(K, Queue[F, Option[A]])])
                }

              case None => // TODO move the finalisation before the evalMap?
                queues.values.toList.traverse(_.offer(None)).as(none[(K, Queue[F, Option[A]])])
            }
          }
        }
        .unNone
        .map { case (k, q) =>
          Stream.fromQueueNoneTerminated(q).through(f(k))
        }
        .parJoinUnbounded
    }
  }
}
