/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.exchange

import scala.language.existentials

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[ShuffleExchange]] Operators where required.  Also ensure that the
 * input partition ordering requirements are met.
 */
case class EnsureRequirements(conf: SQLConf) extends Rule[SparkPlan] {
  private def defaultNumPreShufflePartitions: Int = conf.numShufflePartitions

  private def targetPostShuffleInputSize: Long = conf.targetPostShuffleInputSize

  private def adaptiveExecutionEnabled: Boolean = conf.adaptiveExecutionEnabled

  private def minNumPostShufflePartitions: Option[Int] = {
    val minNumPostShufflePartitions = conf.minNumPostShufflePartitions
    if (minNumPostShufflePartitions > 0) Some(minNumPostShufflePartitions) else None
  }

  /**
   * Given a required distribution, returns a partitioning that satisfies that distribution.
   */
  private def createPartitioning(
      requiredDistribution: Distribution,
      numPartitions: Int,
      childPartitionings: Seq[Partitioning] = Seq()): Partitioning = {
    requiredDistribution match {
      case AllTuples => SinglePartition
        // TODO (tejasp) see if this `hashingFunction` needs to be respected
      case ClusteredDistribution(clustering, _, hashingFunction) =>
        val distinctChildHashingFunctions = childPartitionings.map {
          case HashPartitioning(_, _, hashingFunction) => hashingFunction
          case _ => classOf[Murmur3Hash]
        }.distinct

        // If all the children use the same hashing function, then use it. Else fallback to the
        // default hashing function (ie. Murmur3Hash). This might not be the most optimal thing
        // to do. eg. In case of join, if left child is hashed using HiveHash and the right one
        // using Murmur3Hash, this would shuffle the left relation. If the left relation is
        // larger than the right relation, the cost of shuffling it will be high. Instead more
        // optimal thing to do would be to shuffle the right relation using HiveHash so that
        // at the join side both the children are shuffled using the same hashing function. Using
        // Murmur3Hash might turn out better if there are downstream operator's needing data
        // partitioned over Murmur3Hash which cannot be estimated at this point.
        val targetHashingFunction = if (distinctChildHashingFunctions.length == 1) {
          distinctChildHashingFunctions.head
        } else {
          classOf[Murmur3Hash]
        }
        HashPartitioning(clustering, numPartitions, targetHashingFunction)

      case OrderedDistribution(ordering) => RangePartitioning(ordering, numPartitions)
      case dist => sys.error(s"Do not know how to satisfy distribution $dist")
    }
  }

  /**
   * Adds [[ExchangeCoordinator]] to [[ShuffleExchange]]s if adaptive query execution is enabled
   * and partitioning schemes of these [[ShuffleExchange]]s support [[ExchangeCoordinator]].
   */
  private def withExchangeCoordinator(
      children: Seq[SparkPlan],
      requiredChildDistributions: Seq[Distribution]): Seq[SparkPlan] = {
    val supportsCoordinator =
      if (children.exists(_.isInstanceOf[ShuffleExchange])) {
        // Right now, ExchangeCoordinator only support HashPartitionings.
        children.forall {
          case e @ ShuffleExchange(hash: HashPartitioning, _, _) => true
          case child =>
            child.outputPartitioning match {
              case hash: HashPartitioning => true
              case collection: PartitioningCollection =>
                collection.partitionings.forall(_.isInstanceOf[HashPartitioning])
              case _ => false
            }
        }
      } else {
        // In this case, although we do not have Exchange operators, we may still need to
        // shuffle data when we have more than one children because data generated by
        // these children may not be partitioned in the same way.
        // Please see the comment in withCoordinator for more details.
        val supportsDistribution =
          requiredChildDistributions.forall(_.isInstanceOf[ClusteredDistribution])
        children.length > 1 && supportsDistribution
      }

    val withCoordinator =
      if (adaptiveExecutionEnabled && supportsCoordinator) {
        val coordinator =
          new ExchangeCoordinator(
            children.length,
            targetPostShuffleInputSize,
            minNumPostShufflePartitions)
        children.zip(requiredChildDistributions).map {
          case (e: ShuffleExchange, _) =>
            // This child is an Exchange, we need to add the coordinator.
            e.copy(coordinator = Some(coordinator))
          case (child, distribution) =>
            // If this child is not an Exchange, we need to add an Exchange for now.
            // Ideally, we can try to avoid this Exchange. However, when we reach here,
            // there are at least two children operators (because if there is a single child
            // and we can avoid Exchange, supportsCoordinator will be false and we
            // will not reach here.). Although we can make two children have the same number of
            // post-shuffle partitions. Their numbers of pre-shuffle partitions may be different.
            // For example, let's say we have the following plan
            //         Join
            //         /  \
            //       Agg  Exchange
            //       /      \
            //    Exchange  t2
            //      /
            //     t1
            // In this case, because a post-shuffle partition can include multiple pre-shuffle
            // partitions, a HashPartitioning will not be strictly partitioned by the hashcodes
            // after shuffle. So, even we can use the child Exchange operator of the Join to
            // have a number of post-shuffle partitions that matches the number of partitions of
            // Agg, we cannot say these two children are partitioned in the same way.
            // Here is another case
            //         Join
            //         /  \
            //       Agg1  Agg2
            //       /      \
            //   Exchange1  Exchange2
            //       /       \
            //      t1       t2
            // In this case, two Aggs shuffle data with the same column of the join condition.
            // After we use ExchangeCoordinator, these two Aggs may not be partitioned in the same
            // way. Let's say that Agg1 and Agg2 both have 5 pre-shuffle partitions and 2
            // post-shuffle partitions. It is possible that Agg1 fetches those pre-shuffle
            // partitions by using a partitionStartIndices [0, 3]. However, Agg2 may fetch its
            // pre-shuffle partitions by using another partitionStartIndices [0, 4].
            // So, Agg1 and Agg2 are actually not co-partitioned.
            //
            // It will be great to introduce a new Partitioning to represent the post-shuffle
            // partitions when one post-shuffle partition includes multiple pre-shuffle partitions.
            val targetPartitioning =
              createPartitioning(distribution, defaultNumPreShufflePartitions)
            assert(targetPartitioning.isInstanceOf[HashPartitioning])
            ShuffleExchange(targetPartitioning, child, Some(coordinator))
        }
      } else {
        // If we do not need ExchangeCoordinator, the original children are returned.
        children
      }

    withCoordinator
  }

  private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements:
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) =>
        BroadcastExchangeExec(mode, child)
      case (child, distribution) =>
        ShuffleExchange(createPartitioning(distribution, defaultNumPreShufflePartitions), child)
    }

    // If the operator has multiple children and specifies child output distributions (e.g. join),
    // then the children's output partitionings must be compatible:
    def requireCompatiblePartitioning(distribution: Distribution): Boolean = distribution match {
      case UnspecifiedDistribution => false
      case BroadcastDistribution(_) => false
      case _ => true
    }
    if (children.length > 1
        && requiredChildDistributions.exists(requireCompatiblePartitioning)
        && !Partitioning.allCompatible(children.map(_.outputPartitioning))) {

      // First check if the existing partitions of the children all match. This means they are
      // partitioned by the same partitioning into the same number of partitions. In that case,
      // don't try to make them match `defaultPartitions`, just use the existing partitioning.
      val childPartitionings = children.map(_.outputPartitioning)
      val maxChildrenNumPartitions = childPartitionings.map(_.numPartitions).max
      val useExistingPartitioning = childPartitionings.zip(requiredChildDistributions).forall {
        case (childPartitioning, distribution) =>
          val targetPartitioning =
            createPartitioning(distribution, maxChildrenNumPartitions, childPartitionings)
          childPartitioning.guarantees(targetPartitioning)
      }

      children = if (useExistingPartitioning) {
        // We do not need to shuffle any child's output.
        children
      } else {
        // We need to shuffle at least one child's output.
        // Now, we will determine the number of partitions that will be used by created
        // partitioning schemes.
        val numPartitions = {
          // Let's see if we need to shuffle all child's outputs when we use
          // maxChildrenNumPartitions.
          val shufflesAllChildren = children.zip(requiredChildDistributions).forall {
            case (child, distribution) =>
              !child.outputPartitioning.guarantees(
                createPartitioning(distribution, maxChildrenNumPartitions))
          }
          // If we need to shuffle all children, we use defaultNumPreShufflePartitions as the
          // number of partitions. Otherwise, we use maxChildrenNumPartitions.
          if (shufflesAllChildren) defaultNumPreShufflePartitions else maxChildrenNumPartitions
        }

        children.zip(requiredChildDistributions).map {
          case (child, distribution) =>
            val targetPartitioning = createPartitioning(distribution, numPartitions)
            if (child.outputPartitioning.guarantees(targetPartitioning)) {
              child
            } else {
              child match {
                // If child is an exchange, we replace it with
                // a new one having targetPartitioning.
                case ShuffleExchange(_, c, _) => ShuffleExchange(targetPartitioning, c)
                case _ => ShuffleExchange(targetPartitioning, child)
              }
          }
        }
      }
    }

    // Now, we need to add ExchangeCoordinator if necessary.
    // Actually, it is not a good idea to add ExchangeCoordinators while we are adding Exchanges.
    // However, with the way that we plan the query, we do not have a place where we have a
    // global picture of all shuffle dependencies of a post-shuffle stage. So, we add coordinator
    // at here for now.
    // Once we finish https://issues.apache.org/jira/browse/SPARK-10665,
    // we can first add Exchanges and then add coordinator once we have a DAG of query fragments.
    children = withExchangeCoordinator(children, requiredChildDistributions)

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      if (requiredOrdering.nonEmpty) {
        // If child.outputOrdering is [a, b] and requiredOrdering is [a], we do not need to sort.
        val orderingMatched = if (requiredOrdering.length > child.outputOrdering.length) {
          false
        } else {
          requiredOrdering.zip(child.outputOrdering).forall {
            case (requiredOrder, childOutputOrder) =>
              childOutputOrder.satisfies(requiredOrder)
          }
        }

        if (!orderingMatched) {
          SortExec(requiredOrdering, global = false, child = child)
        } else {
          child
        }
      } else {
        child
      }
    }

    operator.withNewChildren(children)
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator @ ShuffleExchange(partitioning, child, _) =>
      child.children match {
        case ShuffleExchange(childPartitioning, baseChild, _)::Nil =>
          if (childPartitioning.guarantees(partitioning)) child else operator
        case _ => operator
      }
    case operator: SparkPlan => ensureDistributionAndOrdering(operator)
  }
}
