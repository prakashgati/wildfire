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

package org.apache.spark.sql.execution.joins

import java.util
import java.util.Objects

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{DynamicPruning, Expression, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{InnerLike, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, V2TableWriteExec}
import org.apache.spark.sql.types.DataType

object BroadcastFilterPushdown extends Rule[SparkPlan] with PredicateHelper {
  override def apply(plan: SparkPlan): SparkPlan = {
    val shouldAttemptBroadcastVarPushdown = plan.conf.pushBroadcastedJoinKeysASFilterToScan &&
      !plan.isInstanceOf[V2TableWriteExec]
    if (shouldAttemptBroadcastVarPushdown) {
      val (newPlan, removedDpps) = useTopDownPush(plan)
      if (!removedDpps.isEmpty) {
        import scala.collection.JavaConverters._
        val allDpps = removedDpps.values().asScala.flatten.toSeq
        val newlpOpt = plan.logicalLink.map(lp =>
          lp.transformAllExpressionsWithPruning(
            _.containsAnyPattern(DYNAMIC_PRUNING_EXPRESSION, DYNAMIC_PRUNING_SUBQUERY)) {
            case dp: DynamicPruning if allDpps.exists(_ eq dp) => Literal.TrueLiteral
          })
        newlpOpt.foreach(newPlan.setLogicalLink)
        newPlan
      } else {
        newPlan
      }
    } else {
      plan
    }
  }

  private def buildLegsBlockingAncestorsPushForReusePreference(
      plan: SparkPlan): util.IdentityHashMap[SparkPlan, SparkPlan] = {
    val buildLegBlockingPushFromAncestors = new util.IdentityHashMap[SparkPlan, SparkPlan]()
    val canonicalizedBuildLegToOriginal = mutable.Map[SparkPlan, SparkPlan]()
    val nodesToTraverse = mutable.ListBuffer[SparkPlan](plan)
    while (nodesToTraverse.nonEmpty) {
      val nodeToAnalyze = nodesToTraverse.remove(0)
      nodeToAnalyze match {
        case BroadcastHashJoinExtractorForBCPush(bhj) =>
          val (buildLegCanonicalized, buildLeg) = bhj.buildSide match {
            case BuildRight => (bhj.right.canonicalized, bhj.right)

            case _ => (bhj.left.canonicalized, bhj.left)
          }
          val cached =
            canonicalizedBuildLegToOriginal.getOrElseUpdate(buildLegCanonicalized, buildLeg)
          if (cached.ne(buildLeg)) {
            buildLegBlockingPushFromAncestors.put(cached, cached)
            buildLegBlockingPushFromAncestors.put(buildLeg, buildLeg)
          }
          nodesToTraverse.prependAll(Seq(bhj.left, bhj.right))

        case _ =>
          val children = nodeToAnalyze.children
          if (children.nonEmpty) {
            nodesToTraverse.prependAll(children)
          }
      }
    }
    buildLegBlockingPushFromAncestors
  }

  private def useTopDownPush(plan: SparkPlan)
      : (SparkPlan, java.util.IdentityHashMap[LogicalPlan, Seq[DynamicPruning]]) = {
    val buildLegsBlockingPushFromAncestors =
      if (conf.preferReuseExchangeOverBroadcastVarPushdown) {
        buildLegsBlockingAncestorsPushForReusePreference(plan)
      } else {
        new util.IdentityHashMap[SparkPlan, SparkPlan]()
      }
    val batchScanToJoinLegMapping =
      new util.IdentityHashMap[BatchScanExec, mutable.Map[LogicalPlan, Seq[JoiningKeyData]]]()
    val batchScanToRemoveDpp = new util.IdentityHashMap[BatchScanExec, BatchScanExec]()
    val removedDpps = new util.IdentityHashMap[LogicalPlan, Seq[DynamicPruning]]()
    val buildLegPlanToOriginalBatchScans =
      new util.IdentityHashMap[LogicalPlan, Seq[BatchScanExec]]()
    val originalBatchScanToNewBatchScan = new util.IdentityHashMap[BatchScanExec, BatchScanExec]()
    val batchScanToStreamingCol = new util.IdentityHashMap[BatchScanExec, Seq[Int]]()
    val transformedPlanPart1 = plan transformDown {
      case BroadcastHashJoinExtractorForBCPush(bhj) =>
        val (
          buildPlan,
          streamedPlan,
          streamedKeys,
          buildKeys,
          canonicalizedStreamKeys,
          canonicalizedJoinKeys) =
          bhj.buildSide match {
            case BuildLeft =>
              (
                bhj.left,
                bhj.right,
                bhj.rightKeys,
                bhj.leftKeys,
                bhj.canonicalized.asInstanceOf[BroadcastHashJoinExec].rightKeys,
                bhj.canonicalized.asInstanceOf[BroadcastHashJoinExec].leftKeys)

            case BuildRight =>
              (
                bhj.right,
                bhj.left,
                bhj.leftKeys,
                bhj.rightKeys,
                bhj.canonicalized.asInstanceOf[BroadcastHashJoinExec].leftKeys,
                bhj.canonicalized.asInstanceOf[BroadcastHashJoinExec].rightKeys)
          }
        var pushingAnyFilter = false
        val temp = BroadcastHashJoinUtil.canPushBroadcastedKeysAsFilter(
          conf,
          streamedKeys,
          buildKeys,
          streamedPlan,
          buildPlan,
          batchScanToJoinLegMapping,
          buildLegsBlockingPushFromAncestors)
        val groupingOnBasisOfBatchScanExec = temp.groupBy(_.targetBatchScanExec)
        val logicalNodeOpt = buildPlan.logicalLink
        if (logicalNodeOpt.isDefined) {
          val logicalNode = logicalNodeOpt.get
          buildLegPlanToOriginalBatchScans.put(
            logicalNode,
            BroadcastHashJoinUtil
              .getAllBatchScansForSparkPlan(buildPlan))

          groupingOnBasisOfBatchScanExec.foreach { case (bsExec, list) =>
            val keysToPush = list.filter {
              case BroadcastVarPushDownData(baseStreamCol, _, _, _, _) =>
                !batchScanToStreamingCol.containsKey(bsExec) ||
                  !batchScanToStreamingCol.get(bsExec).contains(baseStreamCol)
            }
            if (keysToPush.nonEmpty) {
              val removeDpp = keysToPush.exists(_.requiresDPPRemoval)
              pushingAnyFilter = true
              if (removeDpp) {
                batchScanToRemoveDpp.put(bsExec, bsExec)
              }
              keysToPush.foreach {
                case BroadcastVarPushDownData(
                streamingColLeafIndex,
                _,
                joiningColDataType,
                joinIndex,
                _) =>
                  batchScanToJoinLegMapping.compute(
                    bsExec,
                    (_, prevVal) => {
                      val mappings = if (prevVal eq null) {
                        mutable.Map[LogicalPlan, Seq[JoiningKeyData]]()
                      } else {
                        prevVal
                      }
                      val joiningKeysDataOpt = mappings.get(logicalNode)
                      val joiningKeysData = joiningKeysDataOpt.fold(
                        Seq(
                          JoiningKeyData(
                            canonicalizedStreamKeys(joinIndex),
                            canonicalizedJoinKeys(joinIndex),
                            streamingColLeafIndex,
                            joiningColDataType,
                            joinIndex)))(
                        _ :+ JoiningKeyData(
                          canonicalizedStreamKeys(joinIndex),
                          canonicalizedJoinKeys(joinIndex),
                          streamingColLeafIndex,
                          joiningColDataType,
                          joinIndex))
                      mappings += (logicalNode -> joiningKeysData)
                      mappings
                    })
                  batchScanToStreamingCol.compute(
                    bsExec,
                    (_, v) =>
                      if (v eq null) {
                        Seq(streamingColLeafIndex)
                      } else {
                        v :+ streamingColLeafIndex
                      })
              }
            }
          }
        }
        if (pushingAnyFilter) {
          val newBhj = bhj.copy(bcVarPushNode = SELF_PUSH)
          bhj.logicalLink.foreach(lp => {
            newBhj.setLogicalLink(lp)
            newBhj.preserveLogicalJoinAsHashSelfPush(buildPlan.logicalLink)
          })
          newBhj
        } else {
          bhj
        }

      case bs: BatchScanExec if batchScanToJoinLegMapping.containsKey(bs) =>
        val buildLeg = batchScanToJoinLegMapping.get(bs)
        val newBs =
          if (conf.preferBroadcastVarPushdownOverDPP && batchScanToRemoveDpp.containsKey(bs)) {
            val newBatchScan = bs.copy(
              proxyForPushedBroadcastVar = Option(
                buildLeg.toSeq
                  .sortBy(_._1.hashCode())
                  .map { case (sp, joinData) =>
                    new ProxyBroadcastVarAndStageIdentifier(
                      sp,
                      joinData.sortBy(_.joinKeyIndexInJoiningKeys))
                  }),
              runtimeFilters = Seq.empty)

            val dppRemoved = bs.runtimeFilters
              .filter(_.isInstanceOf[DynamicPruning])
              .map(_.asInstanceOf[DynamicPruning])
            bs.logicalLink.foreach(lpForBs => {
              val leafForBs = lpForBs.collectLeaves().head
              removedDpps.put(leafForBs, dppRemoved)
              newBatchScan.setLogicalLink(leafForBs)
            })
            newBatchScan
          } else {
            bs.copy(proxyForPushedBroadcastVar = Option(
              buildLeg.toSeq
                .sortBy(_._1.hashCode())
                .map { case (lp, streamSideJoinKeysForBuildLeg) =>
                  new ProxyBroadcastVarAndStageIdentifier(
                    lp,
                    streamSideJoinKeysForBuildLeg.sortBy(_.joinKeyIndexInJoiningKeys))
                }))
          }
        originalBatchScanToNewBatchScan.put(bs, newBs)
        newBs
    }
    val finalTransformedPlan = insertBuildLegProxiesOnBatchScans(
      transformedPlanPart1,
      buildLegPlanToOriginalBatchScans,
      originalBatchScanToNewBatchScan)
    finalTransformedPlan -> removedDpps
  }

  private def insertBuildLegProxiesOnBatchScans(
      sparkPlan: SparkPlan,
      buildLegPlanToOriginalBatchScans: util.IdentityHashMap[LogicalPlan, Seq[BatchScanExec]],
      originalBatchScanToNewBatchScan: util.IdentityHashMap[BatchScanExec, BatchScanExec])
      : SparkPlan = {
    sparkPlan match {
      case bs: BatchScanExec if bs.proxyForPushedBroadcastVar.isDefined =>
        val currentProxy = bs.proxyForPushedBroadcastVar.get
        val buildLps = currentProxy.map(_.buildLegPlan)
        val buildProxyiesData = buildLps.map(lp => {
          val oldBs = buildLegPlanToOriginalBatchScans.get(lp)
          oldBs.flatMap(old => {
            if (originalBatchScanToNewBatchScan.containsKey(old)) {
              val later = originalBatchScanToNewBatchScan.get(old)
              if (originalBatchScanToNewBatchScan.containsKey(later)) {
                val latest = originalBatchScanToNewBatchScan.get(later)
                latest.proxyForPushedBroadcastVar.getOrElse(Seq.empty)
              } else {
                later.proxyForPushedBroadcastVar.getOrElse(Seq.empty)
              }
            } else {
              Seq.empty
            }
          })
        })
        val newProxies =
          currentProxy.zip(buildProxyiesData).map { case (proxy, buildLegPrxoxies) =>
            new ProxyBroadcastVarAndStageIdentifier(
              proxy.buildLegPlan,
              proxy.joiningKeysData,
              buildLegPrxoxies)
          }
        val newBs = bs.copy(proxyForPushedBroadcastVar = Option(newProxies))
        bs.logicalLink.foreach(newBs.setLogicalLink)
        //  make another entry which is for the bs to new batch scan, which is send update
        originalBatchScanToNewBatchScan.put(bs, newBs)
        newBs
      case bhj: BroadcastHashJoinExec =>
        val (buildPlan, streamPlan) = bhj.buildSide match {
          case BuildRight => bhj.right -> bhj.left

          case BuildLeft => bhj.left -> bhj.right
        }
        val newBuildPlan = insertBuildLegProxiesOnBatchScans(
          buildPlan,
          buildLegPlanToOriginalBatchScans,
          originalBatchScanToNewBatchScan)
        val newStreamPlan = insertBuildLegProxiesOnBatchScans(
          streamPlan,
          buildLegPlanToOriginalBatchScans,
          originalBatchScanToNewBatchScan)
        val newBhj = bhj.buildSide match {
          case BuildRight => bhj.copy(right = newBuildPlan, left = newStreamPlan)

          case BuildLeft => bhj.copy(right = newStreamPlan, left = newBuildPlan)
        }
        bhj.logicalLink.foreach(newBhj.setLogicalLink)
        if (bhj.bcVarPushNode == SELF_PUSH) {
          newBhj.preserveLogicalJoinAsHashSelfPush(buildPlan.logicalLink)
        }
        newBhj
      case _ =>
        if (sparkPlan.children.isEmpty) {
          sparkPlan
        } else {
          val newChildren = sparkPlan.children.map(pl =>
            insertBuildLegProxiesOnBatchScans(
              pl,
              buildLegPlanToOriginalBatchScans,
              originalBatchScanToNewBatchScan))
          val newSp = sparkPlan.withNewChildren(newChildren)
          sparkPlan.logicalLink.foreach(newSp.setLogicalLink)
          newSp
        }
    }
  }
}

object BroadcastHashJoinExtractorForBCPush {
  def unapply(plan: SparkPlan): Option[BroadcastHashJoinExec] = {
    plan match {
      case bhj: BroadcastHashJoinExec =>
        bhj.joinType match {
          case _: InnerLike => Option(bhj)

          case LeftSemi if bhj.buildSide == BuildRight => Option(bhj)

          case _ => None
        }

      case _ => None
    }
  }
}

// because we are storing build leg's logical plan as join condition identifier
// in tpcds query 2 type cases, there can be situation where the build leg LogicalPlan's
// are identical, but during BroadcastFilterPushDown, the build legs may get pushed broadcastvar
// and they may be different. so for correct equality considerations while var push down to
// the stream legs, we need to store the build leg's proxy identifier too.
class ProxyBroadcastVarAndStageIdentifier(
    val buildLegPlan: LogicalPlan,
    val joiningKeysData: Seq[JoiningKeyData],
    val buildLegProxyBroadcastVarAndStageIdentifiers: Seq[ProxyBroadcastVarAndStageIdentifier] =
      Seq.empty[ProxyBroadcastVarAndStageIdentifier]) {

  override def toString: String = s"ProxyBroadcastVar..: buildlegPlan=not" +
    s" printing:${joiningKeysData.mkString(",")}: proxy identifiers for buildleg" +
    s"=${buildLegProxyBroadcastVarAndStageIdentifiers.mkString(",")}"

  lazy val canonicalized: ProxyBroadcastVarAndStageIdentifier =
    new ProxyBroadcastVarAndStageIdentifier(
      buildLegPlan.canonicalized,
      this.joiningKeysData,
      buildLegProxyBroadcastVarAndStageIdentifiers.map(_.canonicalized))

  override def equals(that: Any): Boolean =
    that match {
      case prxy: ProxyBroadcastVarAndStageIdentifier =>
        prxy.buildLegPlan.canonicalized == this.buildLegPlan.canonicalized &&
        prxy.joiningKeysData == this.joiningKeysData &&
        this.buildLegProxyBroadcastVarAndStageIdentifiers ==
          prxy.buildLegProxyBroadcastVarAndStageIdentifiers
      case _ => false
    }

  override def hashCode(): Int =
    Objects.hashCode(
      this.buildLegPlan.canonicalized,
      this.joiningKeysData,
      this.buildLegProxyBroadcastVarAndStageIdentifiers.map(_.canonicalized))
}

case class JoiningKeyData(
    streamSideJoinKeyAtJoin: Expression,
    buildSideJoinKeyAtJoin: Expression,
    streamsideLeafJoinAttribIndex: Int,
    joiningColDataType: DataType,
    joinKeyIndexInJoiningKeys: Int)
