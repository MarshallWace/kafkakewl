/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import cats.instances.map._
import cats.instances.set._
import cats.kernel.Monoid
import com.mwam.kafkakewl.common.validation.TopologyToDeployValidator
import com.mwam.kafkakewl.common.{AuthorizationCode, ReadableStateStore}
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyStateChange, DeploymentAllowUnsafeKafkaClusterChange, TopologyToDeployWithVersion}
import com.mwam.kafkakewl.domain.kafkacluster._
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.kafka.utils.KafkaOperationResult
import com.mwam.kafkakewl.processor.kafkacluster.KafkaClusterCommandProcessing.ProcessFuncResult
import com.mwam.kafkakewl.processor.kafkacluster.{KafkaClusterAdmin, deployment}
import com.mwam.kafkakewl.utils._
import com.typesafe.scalalogging.Logger

import scala.util.Try

private[kafkacluster] object DeployTopology {
  implicit class TryKafkaOperationResultExtensions(result: Try[KafkaOperationResult[Unit]]) {
    def toKafkaClusterCommandActionExecution(action: String): KafkaClusterCommandActionExecution = {
      KafkaClusterCommandActionExecution(
        action,
        result.isSuccess,
        result.toEither
          .map(_.written.mkString("\n"))
          .left.map(_.toErrorMessage)
          .merge
      )
    }
  }

  def nonKewlKafkaClusterItem(nonKewlKafkaResources: NonKewlKafkaResources, kafkaClusterItem: KafkaClusterItem): Boolean = {
    kafkaClusterItem match {
      case topic: KafkaClusterItem.Topic => nonKewlKafkaResources.isTopicNonKewl(topic.name)
      case acl: KafkaClusterItem.Acl => nonKewlKafkaResources.isAclNonKewl(
        acl.resourceType,
        acl.resourcePatternType,
        acl.resourceName,
        acl.principal,
        acl.host,
        acl.operation,
        acl.permission
      )
    }
  }

  def logDiscrepanciesBetweenCurrentAndDeployed(logger: Logger, deployedDiffBeforeOfOthers: KafkaClusterItem.DiffResult): Unit = {
    if (deployedDiffBeforeOfOthers.missingFrom2.nonEmpty) {
      logger.warn("deployed but not in the current topologies (maybe an unsafe change is not applied? Note that it can be a legitimate error too!):")
      deployedDiffBeforeOfOthers.missingFrom2.foreach { deployed =>
        logger.warn(s"deployed but not in the current topologies: [${deployed.kafkaClusterItem}]")
      }
    }
    if (deployedDiffBeforeOfOthers.missingFrom1.nonEmpty) {
      logger.error(s"in the current topologies but not deployed (this must not happen, something was deleted directly from kafka):")
      deployedDiffBeforeOfOthers.missingFrom1.foreach { before =>
        // these are really errors
        logger.error(s"in the current topologies but not deployed: [${before.kafkaClusterItem}]")
      }
    }
    if (deployedDiffBeforeOfOthers.changes.nonEmpty) {
      logger.warn(s"current != deployed (maybe an unsafe change is not applied? Note that it can be a legitimate error too!):")
      deployedDiffBeforeOfOthers.changes.foreach { case (deployed, before) =>
        logger.warn(s"current != deployed: [${before.kafkaClusterItem}] != [${deployed.kafkaClusterItem}]")
      }
    }
  }

  def kafkaClusterCommandResponseActionsIn(
    changesWithApprovalAndActionResult: Seq[(KafkaClusterApprovedChange, Option[KafkaClusterCommandActionExecution])]
  ): Seq[KafkaClusterDeployCommandResponseAction] =
    changesWithApprovalAndActionResult.map { case (ca, execResult) =>
      KafkaClusterDeployCommandResponseAction(
        ca.plannedChange.toString,
        ca.safety,
        ca.notAllowedUnsafeChange,
        execResult
      )
    }

  def hasAnyActionFailedIn(
    changesWithApprovalAndActionResult: Seq[(KafkaClusterApprovedChange, Option[KafkaClusterCommandActionExecution])]
  ): Boolean =
    changesWithApprovalAndActionResult.exists { case (_, execResult) => !execResult.forall(_.success)}

  def succeededToPerformPlannedChanges(
    changesWithApprovalAndActionResult: Seq[(KafkaClusterApprovedChange, Option[KafkaClusterCommandActionExecution])]
  ): Boolean =
    changesWithApprovalAndActionResult
      // we expect all actions NOT to have not-allowed unsafe change AND the action should succeed
      .forall { case (ca, execResult) => ca.notAllowedUnsafeChange.isEmpty && execResult.exists(_.success) }

  def removesNotDoneIn(
    changesWithApprovalAndActionResult: Seq[(KafkaClusterApprovedChange, Option[KafkaClusterCommandActionExecution])]
  ): Seq[String] = {
    changesWithApprovalAndActionResult
      .filter { case (ca, execResult) => ca.plannedChange.isInstanceOf[KafkaClusterChange.Remove] && execResult.forall(!_.success) }
      .map { case (ca, _) => ca.plannedChange.key }
      .sorted
  }

  def createApprovedChangesToDeployTopology(
    allowedUnsafeChanges: Seq[DeploymentAllowUnsafeKafkaClusterChange],
    changes: Seq[KafkaClusterChange]
  ): Seq[KafkaClusterApprovedChange] =
    KafkaClusterApprovedChange.fromKafkaClusterChanges(_.isAllowed(allowedUnsafeChanges))(changes)

  /**
    * Creates the changes needed to deploy or undeploy the specified topology in the kafka cluster.
    *
    * If topologyToDeployOrNone is None it undeploys the topology which means an "empty" topology will be deployed instead (removes everything).
    *
    * @param resolveTopicConfig the resolve replica placement function
    * @param isTopicConfigManaged the function deciding which topic config's are managed by kewl
    * @param currentDeployedTopologies the current set of deployed-topology entities
    * @param currentTopologies the current set of topology entities (in the deployed-topologies)
    * @param deployedKafkaClusterItems the current set of KafkaClusterItems in the kafka cluster
    * @param topologyId the topology id to deploy or undeploy
    * @param topologyToDeployOrNone the topology to deploy or None in which case it'll be undeployed
    * @param topicDefaults the topic defaults
    * @return the changes needed to deploy or undeploy the given topology
    */
  def createChangesToDeployTopology(
    resolveTopicConfig: ResolveTopicConfig,
    isTopicConfigManaged: IsTopicConfigManaged,
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    currentTopologies: Map[TopologyEntityId, TopologyToDeploy],
    deployedKafkaClusterItems: Seq[KafkaClusterItemOfTopology],
    isKafkaClusterSecurityEnabled: Boolean,
    topologyId: TopologyEntityId,
    topologyToDeployOrNone: Option[TopologyToDeploy],
    topicDefaults: TopicDefaults,
    loggerOrNone: Option[Logger] = None,
  ): Seq[KafkaClusterChange] = {

    // figuring out the "before" and "after" state
    val beforeKafkaClusterItems = KafkaClusterItems.forAllTopologies(resolveTopicConfig, currentTopologies, isKafkaClusterSecurityEnabled, topicDefaults)
    val afterKafkaClusterItems = topologyToDeployOrNone
      .map(topology => KafkaClusterItems.forAllTopologies(resolveTopicConfig, currentTopologies, isKafkaClusterSecurityEnabled, topologyId, topology, topicDefaults))
      // if there is no topology to deploy (i.e. un-deploy), we just pretend that it doesn't exist, only the other existing topologies
      .getOrElse(KafkaClusterItems.forAllTopologies(resolveTopicConfig, currentTopologies - topologyId, isKafkaClusterSecurityEnabled, topicDefaults))

    // building a map to find the owner topology ids of KafkaClusterItems that were supposed to be removed but for some reason they were not
    // (e.g. the removal was unsafe, or failed)
    // we use these to associate orphaned items in the actual kafka cluster to topologies so we know what orphaned items need to be removed now
    // as part of the deployment of the current topology
    val notRemovedKeysOwnerTopologyIds = Monoid[Map[String, Set[TopologyEntityId]]].combineAll(
      currentDeployedTopologies.values.map(dp => dp.entity.notRemovedKeys.map((_, Set(dp.entity.topologyId))).toMap)
    )

    // Step #1: finding out the kafka cluster changes that are needed to get from the current cluster's state to the "before" state
    // figuring out the difference between the "before" and the current kafka cluster,
    // associating all discrepancies with owner topology ids (if possible)
    val deployedDiffBefore = KafkaClusterItem.diff(deployedKafkaClusterItems, beforeKafkaClusterItems)
      .assignOwnerTopologyIdsIfMissing(notRemovedKeysOwnerTopologyIds)
    val (deployedDiffBeforeOfTopology, deployedDiffBeforeOfOthers) = deployedDiffBefore.partitionByOwnerTopologyId(topologyId)
    // we log other topologies's discrepancies (not much we can or should do about those right now)
    loggerOrNone.foreach { logger => logDiscrepanciesBetweenCurrentAndDeployed(logger, deployedDiffBeforeOfOthers) }
    // generate changes that fixes the discrepancies of this topology
    val deployedToBeforeChanges = deployedDiffBeforeOfTopology.toKafkaClusterChanges

    // Step #2: finding out the kafka cluster changes that are needed to get from the "before" state to the "after" state
    val beforeDiffAfter = KafkaClusterItem.diff(beforeKafkaClusterItems, afterKafkaClusterItems)
    val beforeDiffAfterChanges = beforeDiffAfter.toKafkaClusterChanges

    // Step #3: merging the deployedToBeforeChanges and beforeDiffAfterChanges so that changes for the same item are merged into zero or one changes
    val changes = (deployedToBeforeChanges.keys ++ beforeDiffAfterChanges.keys)
      .flatMap(k => KafkaClusterChanges.merge(deployedToBeforeChanges.get(k), beforeDiffAfterChanges.get(k)))
      // also, filtering out the non-real changes: e.g. unManaged topic creation/removal
      .filter(_.isReal)
      .toSeq

    // Step #4: comparing the changes with the kafka-cluster's state, if needed alter the changes
    // (e.g. if a topic is already there that we're about to add here, we need to remove that change or
    // make it an UpdateTopic if it's there but different)
    val deployedKafkaClusterItemsMap = deployedKafkaClusterItems.map(_.toTuple).toMap
    changes
      .flatMap {
        case c @ KafkaClusterChange.Add(itemToAdd) =>
          deployedKafkaClusterItemsMap
            .get(itemToAdd.key)
            .map(existingItem => {
              (existingItem.kafkaClusterItem, itemToAdd) match {
                // this item already exists, exactly the way we want to Add -> nothing to do
                case (before, after) if before == after =>
                  Seq.empty
                // this item exists, but not exactly the same way -> need to update the topic
                case (before: KafkaClusterItem.Topic, after: KafkaClusterItem.Topic) if before != after =>
                  Seq(KafkaClusterChange.UpdateTopic(before, after))
                // this shouldn't happen, ACLs can't be updated (the key is the same as the value)
                case (before, after) =>
                  loggerOrNone.foreach { _.error(s"should not happen: trying to add $after, but $before already exists, they have the same key but they are not topics.") }
                  Seq(c)
              }
            })
            // the item we're trying to add, doesn't exist yet -> just carry on adding
            .getOrElse(Seq(c))
        case c @ KafkaClusterChange.Remove(_) =>
          // it can't go wrong: it was already removed in the cluster, then the merge must have found an Add-Remove pair of changes
          // which must be collapsed to no-change at all
          Seq(c)
        case c @ KafkaClusterChange.UpdateTopic(_, after) =>
          deployedKafkaClusterItemsMap
              .get(after.key)
            // if it really exists in the kafka-cluster, we just let it through
            .map(_ => Seq(c))
            // if it does not exists, we convert it to an add-topic (it probably can't happen though)
            .getOrElse(Seq(KafkaClusterChange.Add(after)))
      }
      .flatMap {
        case c: KafkaClusterChange.UpdateTopic => c.ignoreNotManagedTopicConfigs(isTopicConfigManaged)
        case c => Some(c)
      }
  }

  def failIfAuthorizationCodeNeeded(
    kafkaCluster: KafkaCluster,
    topologyToDeployWithAuthorizationCode: Option[Boolean],
    topologiesState: String,
    command: KafkaClusterCommand,
    hasChanges: Boolean,
    authorizationCode: Option[String]
  ): ValueOrCommandErrors[Unit] = {
    val validLastMinutes = 5

    val authorizationCodeInput = AuthorizationCode.Generator.inputFor(kafkaCluster.kafkaCluster, command.userName, topologiesState)
    def isAuthorizationCodeValid(code: String) = AuthorizationCode.isValidInLastMinutes(code, validLastMinutes, authorizationCodeInput)

    Either.cond(
      !topologyToDeployWithAuthorizationCode.getOrElse(kafkaCluster.requiresAuthorizationCode) || command.dryRun || !hasChanges || authorizationCode.exists(isAuthorizationCodeValid),
      (),
      {
        val currentCode = AuthorizationCode.generateCurrent(authorizationCodeInput)
        Seq(CommandError.permissionError(
            if (authorizationCode.nonEmpty) s"authorization code invalid, new authorization code: $currentCode"
            else s"authorization code required: $currentCode"
        ))
      }
    )
  }

  implicit class KafkaClusterCommandExtensions(command: KafkaClusterCommand) {
    def failedResultWithNotPerformedChanges(changes: Seq[KafkaClusterApprovedChange], reasons: Seq[CommandError] = Seq.empty): KafkaClusterCommandResult.Failed =
      KafkaClusterCommandResult.Failed(
        command.metadata,
        command.kafkaClusterId,
        reasons = reasons,
        response = Some(KafkaClusterCommandResponse.Deployment(kafkaClusterCommandResponseActionsIn(changes.map((_, None))), deployedTopologyChanged = false))
      )
  }
}

private[kafkacluster] trait DeployTopology {
  import DeployTopology._

  val kafkaClusterId: KafkaClusterEntityId
  val kafkaClusterAdmin: KafkaClusterAdmin
  protected val logger: Logger
  val topicDefaults: TopicDefaults

  private def kafkaClusterItemsOfCluster(
    command: KafkaClusterCommand,
    nonKewlKafkaResources: NonKewlKafkaResources,
    exceptTopicKeys: Set[String] = Set.empty
  ): Either[KafkaClusterCommandResult.Failed, Seq[KafkaClusterItemOfTopology]] =
    // loading everything from the kafka cluster
    kafkaClusterAdmin.kafkaClusterItems().toEither
      .map(items => items
        // excluding the internal KafkaClusterItems that are part of the cluster without kafka-kewl
        .filterNot(nonKewlKafkaClusterItem(nonKewlKafkaResources, _))
        // and the except-topics too
        .filterNot(t => exceptTopicKeys.contains(t.key))
        .map(deployment.KafkaClusterItemOfTopology(_))
      )
      .left.map(command.failedResult)

  private def deleteTopicsToBeRecreated(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand.DeployTopology,
    topicsToRecreate: Seq[FlexibleTopologyTopicId] = Seq.empty,
    topicsToRecreateResetGroups: Boolean = true,
    topics: Map[TopicId, TopologyToDeploy.Topic],
    dryRun: Boolean
  ): Either[KafkaClusterCommandResult.Failed, (Set[String], Seq[(KafkaClusterApprovedChange, Option[KafkaClusterCommandActionExecution])])] = {
    def deleteTopics(
      topicsToDelete: Seq[KafkaClusterItem.Topic],
      topicPrepareErrors: Map[String, String]
    ): (Set[String], Seq[(KafkaClusterApprovedChange, Option[KafkaClusterCommandActionExecution])]) = {

      // these remove changes are allowed (the user explicitly stated the topicsToRemove)
      val changesToRecreateTopicsWithApproval = KafkaClusterApprovedChange.fromKafkaClusterChanges(_ => true)(
        topicsToDelete.map(KafkaClusterChange.Remove)
      )

      val topicsPreparedToDelete = topicsToDelete.filter(td => !topicPrepareErrors.contains(td.name))

      // splitting the changes to 2: the ones which couldn't be prepared and the rest
      val (changesWithPrepareErrors, preparedChanges) = changesToRecreateTopicsWithApproval.partition {
        case KafkaClusterApprovedChange(KafkaClusterChange.Remove(t: KafkaClusterItem.Topic), _, _, _) => topicPrepareErrors.contains(t.name)
        case _ => true // just so that the compiler shuts up
      }

      // the ones that failed to prepare are not deployed: instead we just put the error in the result
      val changesWithPrepareErrorsResults = changesWithPrepareErrors.collect {
        case ca @ KafkaClusterApprovedChange(KafkaClusterChange.Remove(t: KafkaClusterItem.Topic), _, _, _) =>
          (ca, Some(KafkaClusterCommandActionExecution(ca.plannedChange.toString, success = false, result = topicPrepareErrors(t.name))))
      }

      if (topicsPreparedToDelete.nonEmpty)
        logger.info(s"deleting topics ${topicsPreparedToDelete.map(_.name.quote).mkString(", ")}...")

      // deploying the delete topic changes (only the ones that were successfully prepared)
      val changesToRecreateTopicsWithApprovalAndActionResult = deployChangesWithApproval(preparedChanges, dryRun)
      // returning the topics' keys that will have to be ignored when loading kafka-cluster items from the cluster
      // it's populated only in dry-run mode, because otherwise those topics are deleted anyway or if not, we want them to be there
      // in dry-run mode however, those topics are obviously not deleted physically, so we need to explicitly skip them when loading
      val kafkaClusterTopicKeysToIgnore = if (dryRun) topicsPreparedToDelete.map(_.key).toSet else Set.empty[String]

      // generally we need to wait some time for the topic deletions to actually happen. Not sure how robust this is, especially in case of many deleted topics...
      if (preparedChanges.nonEmpty && !dryRun) {
        val delayMs = 5000
        logger.info(s"waiting for $delayMs ms after deleting topics ${topicsToDelete.map(_.name.quote).mkString(", ")}")
        Thread.sleep(delayMs)
      }

      (
        // the dry-run's to-be-ignored topic names contain only the ones that could have been successfully prepared
        kafkaClusterTopicKeysToIgnore,
        // the deployment results are combined with the failed-to-prepare results
        changesWithPrepareErrorsResults ++ changesToRecreateTopicsWithApprovalAndActionResult
      )
    }

    // finding the topics to be deleted for re-creation
    val topologyTopicsToRecreate = topics
      .filterKeys(id => topicsToRecreate.exists(_.doesMatch(id)))
      .values
      .flatMap(KafkaClusterItems.forTopic(kafkaCluster.resolveTopicConfig))
      .filter(_.isReal) // only real topics are deleted (i.e. managed ones)
      .toSeq

    val topologyTopicNamesToRecreate = topologyTopicsToRecreate.map(_.name)

    if (topologyTopicNamesToRecreate.nonEmpty)
      logger.info(s"preparing the topics ${topologyTopicNamesToRecreate.map(_.quote).mkString(", ")} to be deleted...")

    val result = for {
      // in some cases we cannot perform any kafka cluster change, unless the user provides a code we generate
      // this is a basic protection against accidental unwanted prod deployments
      _ <- failIfAuthorizationCodeNeeded(
        kafkaCluster,
        command.topologyToDeploy.deployWithAuthorizationCode,
        command.topologiesStateForAuthorizationCode,
        command,
        topologyTopicsToRecreate.nonEmpty,
        command.options.authorizationCode
      ).left.map(f => f.map(_.mapMessage(m => s"while trying to delete ${topologyTopicNamesToRecreate.map(_.quote).mkString(", ")}: $m")))

      // fail-fast if there is some fatal error during preparing the topics for deletion
      prepareResults <- kafkaClusterAdmin.prepareTopicsToBeDeleted(topologyTopicNamesToRecreate, topicsToRecreateResetGroups, command.dryRun)
    } yield {
      val dryRunString = if (command.dryRun) "dry-run: " else ""
      val topicErrors = prepareResults.toSeq
        .sortBy { case ((topic, consumerGroupId), _) => (topic, consumerGroupId) }
        .flatMap {
          case ((topic, consumerGroupId), Right(_)) =>
            logger.info(s"${dryRunString}topic ${topic.quote} - reset consumer group ${consumerGroupId.quote}")
            None
          case ((topic, consumerGroupId), Left(ce)) =>
            val errorMessage = s"${dryRunString}topic ${topic.quote} - consumer group ${consumerGroupId.quote}: ${ce.mkString(" ;; ")}"
            logger.warn(errorMessage)
            Some((topic, errorMessage))
        }
        .groupBy { case (topic, _) => topic }
        // a topic can have multiple different errors from different consumer groups - creating only a single error message per topic
        .mapValues(_.map { case (_, errorMessage) => errorMessage }.mkString("\n") )

      // performing the actual deletions
      val (kafkaClusterTopicKeysToIgnore, changesToRecreateTopicsWithApprovalAndActionResult) = deleteTopics(topologyTopicsToRecreate, topicErrors)

      // the result
      (kafkaClusterTopicKeysToIgnore, changesToRecreateTopicsWithApprovalAndActionResult)
    }

    result.left.map(command.failedResult)
  }

  private def deployChangesWithApproval(
    changesWithApproval: Seq[KafkaClusterApprovedChange],
    dryRun: Boolean
  ): Seq[(KafkaClusterApprovedChange, Option[KafkaClusterCommandActionExecution])] =
    changesWithApproval
      .map(ca => {
        ca.allowedChange.foreach { change => logger.info(s"deploying $change") }
        val execResult = ca.allowedChange
          .map(ac => kafkaClusterAdmin.deployKafkaClusterChange(ac, dryRun).toKafkaClusterCommandActionExecution(ac.toString))
        execResult.foreach { er => logger.info(s"$er") }
        (ca, execResult)
      })

  private def createStateChanges[E <: Entity](
    command: KafkaClusterCommand,
    stateStore: ReadableStateStore[E],
    id: String,
    newEntity: E,
    allStateChanges: EntityStateMetadata => AllDeploymentEntitiesStateChanges
  ): AllDeploymentEntitiesStateChanges = {
    val previousIsSameAsNew = stateStore.getLatestLiveState(id).exists(_.entity == newEntity)
    // if the previous version is exactly the same as the new one -> generate no state change, just return success
    if (previousIsSameAsNew) AllDeploymentEntitiesStateChanges()
    else allStateChanges(EntityStateMetadata(id, stateStore.getNextStateVersion(id), command.userName))
  }

  private def createDeleteStateChanges[E <: Entity](
    command: KafkaClusterCommand,
    stateStore: ReadableStateStore[E],
    id: String
  ): AllDeploymentEntitiesStateChanges = {
    if (stateStore.hasLiveState(id))
      AllDeploymentEntitiesStateChanges(
        DeployedTopologyStateChange.Deleted(EntityStateMetadata(id, stateStore.getNextStateVersion(id), command.userName))
      )
    else
      AllDeploymentEntitiesStateChanges()
  }

  /**
    * Calculates the difference between the kafka cluster's state and the current desired state in the topologies.
    *
    * @param kafkaCluster the kafka cluster details
    * @param command the deploy topology command
    * @param stateStore the deployed-topology state-store
    * @return the success or failure result with all the details
    */
  def diff(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand.Diff,
    stateStore: ReadableStateStore[DeployedTopology]
  ): ProcessFuncResult = {
    val currentDeployedTopologies = stateStore.getLatestLiveStates.map(dp => (dp.entity.topologyId, dp)).toMap
    val currentTopologies = currentDeployedTopologies
      .mapValues(_.entity.topologyWithVersion)
      .collect { case (p, Some(pv)) => (p, pv.topology) }

    for {
      // loading all kafka-cluster items from the cluster
      deployedKafkaClusterItems <- kafkaClusterItemsOfCluster(
        command,
        if (command.ignoreNonKewlKafkaResources) kafkaCluster.nonKewl else NonKewlKafkaResources()
      )

      result <- {
        val kafkaStreamsAppIds = currentTopologies.values
          .flatMap(_.applications.values.map(_.`type`))
          .collect { case r: TopologyToDeploy.Application.Type.KafkaStreams => r.kafkaStreamsAppId }
          .toSeq.distinct
        val currentKafkaClusterItems = KafkaClusterItems.forAllTopologies(kafkaCluster.resolveTopicConfig, currentTopologies, kafkaClusterAdmin.isSecurityEnabled, topicDefaults)
        val deployedDiffBefore = KafkaClusterItem.diff(deployedKafkaClusterItems, currentKafkaClusterItems)
        val missingFromKafkaCluster = deployedDiffBefore.missingFrom1.map(_.kafkaClusterItem).filter(_.isReal)
        val notNeededInKafkaCluster = deployedDiffBefore.missingFrom2
          .map(_.kafkaClusterItem)
          .filter(_.isReal)
          .collect {
            // keep all kafka cluster ACLs
            case i: KafkaClusterItem.Acl => i
            // ignore the kafka cluster topics that start with a known kafka streams app id
            case i: KafkaClusterItem.Topic if !kafkaStreamsAppIds.exists(ksappid => i.name.startsWith(ksappid)) => i
          }
        val differences = deployedDiffBefore.changes
          .map { case (i1, i2) => (i1.kafkaClusterItem, i2.kafkaClusterItem) }
          .filter { case (i1, i2) => i1.isReal && i2.isReal }
        // returning success always, but the command result may be failure (that failure is not really failure, we still
        // modify the deployed-topology states, we still may have had actions run, etc...)

        def aclResponse(acl: KafkaClusterItem.Acl) =
          KafkaClusterCommandResponse.DiffResult.Acl(s"${acl.permission} ${acl.operation}", acl.principal, s"${acl.resourcePatternType} ${acl.resourceType} ${acl.resourceName}", acl.toString)

        val response = KafkaClusterCommandResponse.DiffResult(
          topics = KafkaClusterCommandResponse.DiffResult.Topics(
            missingFromKafkaCluster.collect { case t: KafkaClusterItem.Topic => t.toString }.sorted,
            notNeededInKafkaCluster.collect { case t: KafkaClusterItem.Topic => t.toString }.sorted,
            differences
              .collect {
                case (t1: KafkaClusterItem.Topic, t2: KafkaClusterItem.Topic) =>
                  KafkaClusterCommandResponse.DiffResult.Topics.Difference(t1.toString, t2.toString)
              }
              .sortBy(_.kafka)
          ),
          acls = KafkaClusterCommandResponse.DiffResult.Acls(
            missingFromKafkaCluster.collect { case a: KafkaClusterItem.Acl => aclResponse(a) }.sortBy(a => (a.perm, a.user, a.resource)),
            notNeededInKafkaCluster.collect { case a: KafkaClusterItem.Acl => aclResponse(a) }.sortBy(a => (a.perm, a.user, a.resource))
          ),
        )

//        {
//          import java.io._
//          val pwTopics = new PrintWriter(new File("remove-topics.json" ))
//          val removeTopics = notNeededInKafkaCluster
//            .collect { case t: KafkaClusterItem.Topic => t.name }
//            .sorted
//            .map(t => s"""    {"DeleteTopic":{"topic":"$t"}}""")
//            .mkString(",\n")
//          pwTopics.write(
//            s"""{
//              |  "dryRun": true,
//              |  "mode": "TryAll",
//              |  "commands": [
//              |$removeTopics
//              |  ]
//              |}""".stripMargin)
//          pwTopics.close()
//
//          val pwAcls = new PrintWriter(new File("remove-acls.json" ))
//          def toU(s: String) =
//            if (s != null && s.nonEmpty) s.head.toUpper + s.toLowerCase.substring(1)
//            else s
//          def toResourceType(t: String) = if (t.toLowerCase == "transactional_id") "TransactionalId" else t
//          val removeAcls = notNeededInKafkaCluster
//            .collect { case a: KafkaClusterItem.Acl => a }
//            .sortBy(a => (a.operation.toString, a.principal, a.resourcePatternType.toString, a.resourceType.toString, a.resourceName))
//            .map(a => s"""    {"RemoveAcl":{"operation": "${toU(a.operation.toString)}","permission": "${toU(a.permission.toString)}","principal": "${a.principal}","resourcePattern": "${toU(a.resourcePatternType.toString)}","resourceType": "${toResourceType(toU(a.resourceType.toString))}","resourceName": "${a.resourceName.toString}","host": "${a.host}"}}""")
//            .mkString(",\n")
//          pwAcls.write(
//            s"""{
//               |  "dryRun": true,
//               |  "mode": "TryAll",
//               |  "commands": [
//               |$removeAcls
//               |  ]
//               |}""".stripMargin)
//          pwAcls.close()
//        }

        ProcessFuncResult.success(command.succeededResult(response))
      }
    } yield result
  }

  /**
    * Executes the specified DeployTopology command.
    *
    * @param kafkaCluster the kafka cluster details
    * @param command the deploy topology command
    * @param stateStore the deployed-topology state-store
    * @return the success or failure result with all the details
    */
  def deployTopology(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand.DeployTopology,
    stateStore: ReadableStateStore[DeployedTopology]
  ): ProcessFuncResult = {
    val kafkaClusterId = command.kafkaClusterId
    val topologyId = command.topologyId
    val topologyVersion = command.topologyVersion
    val topologyToDeploy = command.topologyToDeploy
    val deployedTopologyId = KafkaClusterAndTopology.id(kafkaClusterId, topologyId)
    val currentDeployedTopologies = stateStore.getLatestLiveStates.map(dp => (dp.entity.topologyId, dp)).toMap
    val currentTopologies = currentDeployedTopologies
      .mapValues(_.entity.topologyWithVersion)
      .collect { case (p, Some(pv)) => (p, pv.topology) }

    val allowedUnsafeChanges = command.options.allowedUnsafeChanges
    val topicsToRecreate = command.options.topicsToRecreate

    for {
      // fail-fast validation
      _ <- TopologyToDeployValidator.validateTopology(currentTopologies, topologyId, Some(topologyToDeploy), kafkaClusterId, kafkaCluster, topicDefaults)
        .toEither
        .left.map(f => command.failedResult(f.toList))

      // first, delete the topology's topics that we need to re-create as part of this deployment (respecting dry-run of course)
      recreatedTopicsResult <-
        if (topicsToRecreate.nonEmpty) {
          // we call this only if we have topics to recreate, otherwise no point and we should avoid
          // the prepare phase when it asks consumer groups, etc...
          deleteTopicsToBeRecreated(
            kafkaCluster,
            command,
            // need to associate the topics-to-recreate matchers with the topology to support local and fully-qualified names too
            topicsToRecreate.map(_.withTopologyNamespace(topologyToDeploy.topologyNamespace)),
            command.options.topicsToRecreateResetGroups,
            topologyToDeploy.fullyQualifiedTopics,
            command.dryRun
          )
        } else {
          Right((Set.empty[String], Seq.empty))
        }
      (kafkaClusterTopicKeysToIgnore, changesToRecreateTopicsWithApprovalAndActionResult) = recreatedTopicsResult

      // loading all kafka-cluster items from the cluster (except the ones we were supposed to delete-to-recreate in dry-run mode)
      deployedKafkaClusterItems <- kafkaClusterItemsOfCluster(
        command,
        kafkaCluster.nonKewl,
        exceptTopicKeys = kafkaClusterTopicKeysToIgnore
      )

      // what changes do we need to perform to achieve the state defined in the topology, given the current items in the cluster
      changes = createChangesToDeployTopology(
        kafkaCluster.resolveTopicConfig,
        kafkaCluster.isTopicConfigManaged,
        currentDeployedTopologies,
        currentTopologies,
        deployedKafkaClusterItems,
        kafkaClusterAdmin.isSecurityEnabled,
        topologyId,
        Some(topologyToDeploy),
        topicDefaults
      )

      changesWithApproval = createApprovedChangesToDeployTopology(allowedUnsafeChanges, changes)

      // in some cases we cannot perform any kafka cluster change, unless the user provides a code we generate
      // this is a basic protection against accidental unwanted prod deployments
      _ <- failIfAuthorizationCodeNeeded(
        kafkaCluster,
        topologyToDeploy.deployWithAuthorizationCode,
        command.topologiesStateForAuthorizationCode,
        command,
        changes.nonEmpty,
        command.options.authorizationCode
      ).left.map(f => command.failedResultWithNotPerformedChanges(changesWithApproval, f.toList))

      result <- {
        val changesWithApprovalAndActionResult = deployChangesWithApproval(changesWithApproval, command.dryRun)
        val allChangesWithApprovalAndActionResult = changesToRecreateTopicsWithApprovalAndActionResult ++ changesWithApprovalAndActionResult

        val hasAnyActionFailed = hasAnyActionFailedIn(allChangesWithApprovalAndActionResult)
        val kafkaClusterCommandResponseActions = kafkaClusterCommandResponseActionsIn(allChangesWithApprovalAndActionResult)
        // here not using allChangesWithApprovalAndActionResult, because failed topic deletions (for re-create) don't count as removesNotDone
        val removesNotDone = removesNotDoneIn(changesWithApprovalAndActionResult)
        val successfullyPerformedPlannedChanges = succeededToPerformPlannedChanges(changesWithApprovalAndActionResult)

        // creating the state changes as a consequence of this deployment (doesn't matter if anything failed or not, we still attempted this deployment)
        val newDeployedTopology = DeployedTopology(
          kafkaClusterId,
          topologyId,
          command.deploymentVersion,
          Some(TopologyToDeployWithVersion(topologyVersion, topologyToDeploy)),
          successfullyPerformedPlannedChanges,
          removesNotDone,
          kafkaClusterCommandResponseActions
        )
        val stateChanges = createStateChanges(
          command,
          stateStore,
          deployedTopologyId,
          newDeployedTopology,
          md => AllDeploymentEntitiesStateChanges(DeployedTopologyStateChange.NewVersion(md, newDeployedTopology))
        )

        // any action failed -> the command's result is also failure
        val commandResult =
          if (hasAnyActionFailed)
            command.failedResultWithDeployActions(kafkaClusterCommandResponseActions, deployedTopologyChanged = stateChanges.nonEmpty): KafkaClusterCommandResult
          else
            command.succeededDeployResult(kafkaClusterCommandResponseActions, deployedTopologyChanged = stateChanges.nonEmpty): KafkaClusterCommandResult

        // returning success always, but the command result may be failure (that failure is not really failure, we still
        // modify the deployed-topology states, we still may have had actions run, etc...)
        ProcessFuncResult.success(commandResult, stateChanges)
      }
    } yield result
  }

  /**
    * Executes the specified UndeployTopology command.
    *
    * @param kafkaCluster the kafka cluster details
    * @param command the undeploy topology command
    * @param stateStore the deployed-topology state-store
    * @return the success or failure result with all the details
    */
  def undeployTopology(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand.UndeployTopology,
    stateStore: ReadableStateStore[DeployedTopology]
  ): ProcessFuncResult = {

    val kafkaClusterId = command.kafkaClusterId
    val topologyId = command.topologyId
    val deployedTopologyId = KafkaClusterAndTopology.id(kafkaClusterId, topologyId)
    val currentDeployedTopologies = stateStore.getLatestLiveStates.map(dp => (dp.entity.topologyId, dp)).toMap
    val currentTopologies = currentDeployedTopologies
      .mapValues(_.entity.topologyWithVersion)
      .collect { case (p, Some(pv)) => (p, pv.topology) }

    command.deploymentVersion match {
      case None =>
        assert(
          currentDeployedTopologies.get(topologyId).exists(_.entity.topologyWithVersion.isEmpty),
          "expecting the deployed-topology to contain no topology and version when the deployment is being removed"
        )
        // no deployment version AND no topology and version inside this deployed-topology => we just remove this deployed topology without doing anything else
        // this happens when we got a DELETE /deployment message on a deployment that's in remove=true state already
        // it's important that we don't fail here, because the deployment has already been deleted - we can't have a deployed-topology for it
        val stateChanges = createDeleteStateChanges(command, stateStore, deployedTopologyId)
        val commandResult = command.succeededDeployResult(Seq.empty, deployedTopologyChanged = true)
        ProcessFuncResult.success(commandResult, stateChanges)

      case Some(dv) =>
        // otherwise, just try to un-deploy it normally
        val allowedUnsafeChanges = command.options.allowedUnsafeChanges

        for {
          // fail-fast validation
          _ <- TopologyToDeployValidator.validateTopology(currentTopologies, topologyId, None, kafkaClusterId, kafkaCluster, topicDefaults)
            .toEither
            .left.map(f => command.failedResult(f.toList))

          deployedKafkaClusterItems <- kafkaClusterItemsOfCluster(command, kafkaCluster.nonKewl)

          changes = createChangesToDeployTopology(
            kafkaCluster.resolveTopicConfig,
            kafkaCluster.isTopicConfigManaged,
            currentDeployedTopologies,
            currentTopologies,
            deployedKafkaClusterItems,
            kafkaClusterAdmin.isSecurityEnabled,
            topologyId,
            None,
            topicDefaults
          )

          changesWithApproval = createApprovedChangesToDeployTopology(allowedUnsafeChanges, changes)

          // in some cases we cannot perform any kafka cluster change, unless the user provides a code we generate
          // this is a basic protection against accidental unwanted prod deployments
          _ <- failIfAuthorizationCodeNeeded(
            kafkaCluster,
            currentTopologies.get(topologyId).flatMap(_.deployWithAuthorizationCode),
            command.topologiesStateForAuthorizationCode,
            command,
            changes.nonEmpty,
            command.options.authorizationCode
          ).left.map(f => command.failedResultWithNotPerformedChanges(changesWithApproval, f.toList))

          result <- {
            val changesWithApprovalAndActionResult = deployChangesWithApproval(changesWithApproval, command.dryRun)

            val kafkaClusterCommandResponseActions = kafkaClusterCommandResponseActionsIn(changesWithApprovalAndActionResult)
            val removesNotDone = removesNotDoneIn(changesWithApprovalAndActionResult)
            val hasAnyActionFailed = hasAnyActionFailedIn(changesWithApprovalAndActionResult)
            val successfullyPerformedPlannedChanges = succeededToPerformPlannedChanges(changesWithApprovalAndActionResult)

            // creating the state change as a consequence of this deployment (doesn't matter if anything failed or not, we still attempted this deployment)
            val newDeployedTopology = DeployedTopology(kafkaClusterId, topologyId, dv, None, successfullyPerformedPlannedChanges, removesNotDone, kafkaClusterCommandResponseActions)
            val stateChanges = {
              createStateChanges(
                command,
                stateStore,
                deployedTopologyId,
                newDeployedTopology,
                md => AllDeploymentEntitiesStateChanges(DeployedTopologyStateChange.NewVersion(md, newDeployedTopology))
              )
            }

            // any action failed -> the command's result is also failure
            val commandResult =
              if (hasAnyActionFailed)
                command.failedResultWithDeployActions(kafkaClusterCommandResponseActions, deployedTopologyChanged = stateChanges.nonEmpty): KafkaClusterCommandResult
              else
                command.succeededDeployResult(kafkaClusterCommandResponseActions, deployedTopologyChanged = stateChanges.nonEmpty): KafkaClusterCommandResult

            // returning success always, but the command result may be failure (that failure is not really failure, we still
            // modify the deployed-topology states, we still may have had actions run, etc...)
            ProcessFuncResult.success(commandResult, stateChanges)
          }
        } yield result
    }
  }
}
