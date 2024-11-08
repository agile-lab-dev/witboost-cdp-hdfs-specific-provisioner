package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerPolicyUtils.policyName;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerPolicyUtils.rangerPolicy;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerRoleUtils.*;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerSecurityZoneUtils.securityZone;
import static it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.utils.RangerSecurityZoneUtils.zoneName;

import io.vavr.Tuple2;
import io.vavr.Tuple3;
import io.vavr.Tuple6;
import io.vavr.control.Either;
import io.vavr.control.Option;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.RangerConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.*;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.HdfsService;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.PrincipalMappingService;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.service.RangerService;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class StorageAreaHandler extends BaseHandler {
    private final HdfsService hdfsService;
    private final Logger logger = LoggerFactory.getLogger(StorageAreaHandler.class);

    public StorageAreaHandler(
            PrincipalMappingService principalMappingService,
            RangerService rangerService,
            HdfsService hdfsService,
            RangerConfig rangerConfig) {
        super(rangerService, rangerConfig, principalMappingService);
        this.hdfsService = hdfsService;
    }

    public <T extends Specific> Either<FailedOperation, String> create(ProvisionRequest<T> provisionRequest) {
        var eitherOwners = mapOwners(provisionRequest);
        if (eitherOwners.isLeft()) return left(eitherOwners.getLeft());
        var ownerUsers = eitherOwners.get()._1();
        var ownerGroups = eitherOwners.get()._2();

        var eitherStorageSpecific = getStorageSpecific(provisionRequest);
        if (eitherStorageSpecific.isLeft()) return left(eitherStorageSpecific.getLeft());
        StorageSpecific ss = eitherStorageSpecific.get();

        return ss.getPath().flatMap(path -> {
            var rangerRes = upsertRangerEntities(provisionRequest, ss.getRootFolder(), path, ownerUsers, ownerGroups);
            if (rangerRes.isLeft()) return left(rangerRes.getLeft());

            return hdfsService.createFolder(path);
        });
    }

    public <T extends Specific> Either<FailedOperation, Void> destroy(ProvisionRequest<T> provisionRequest) {
        var rangerRes = deleteRangerEntities(provisionRequest.component().getId());
        if (rangerRes.isLeft()) return left(rangerRes.getLeft());

        if (Boolean.TRUE.equals(provisionRequest.removeData())) {
            var eitherStorageSpecific = getStorageSpecific(provisionRequest);
            if (eitherStorageSpecific.isLeft()) return left(eitherStorageSpecific.getLeft());
            return eitherStorageSpecific.get().getPath().flatMap(path -> hdfsService
                    .deleteFolder(path)
                    .map(f -> null));
        }
        return right(null);
    }

    private Either<FailedOperation, Void> deleteRangerEntities(String componentId) {
        var eitherPrefixes = buildPolicyPrefix(componentId)
                .flatMap(policyPrefix -> buildZoneName(componentId).flatMap(zoneName -> buildUserRolePrefix(componentId)
                        .map(userRolePrefix -> new Tuple3<>(policyPrefix, zoneName, userRolePrefix))));
        if (eitherPrefixes.isLeft()) {
            return left(eitherPrefixes.getLeft());
        }

        String policyPrefix = eitherPrefixes.get()._1();
        String zoneName = zoneName(eitherPrefixes.get()._2());
        String userRolePrefix = eitherPrefixes.get()._3();
        String userRoleName = generateUserRoleName(userRolePrefix);

        var eitherPolicy = rangerService
                .findPolicyByName(rangerConfig.hdfsServiceName(), policyName(policyPrefix), Optional.of(zoneName))
                .flatMap(optP -> Option.ofOptional(optP).fold(() -> right(null), rangerService::deletePolicy));
        if (eitherPolicy.isLeft()) return left(eitherPolicy.getLeft());

        var eitherUserRole = rangerService.findRoleByName(userRoleName).flatMap(optR -> Option.ofOptional(optR)
                .fold(() -> right(null), rangerService::deleteRole));
        if (eitherUserRole.isLeft()) return left(eitherUserRole.getLeft());

        return right(null);
    }

    private <T extends Specific> Either<FailedOperation, StorageSpecific> getStorageSpecific(
            ProvisionRequest<T> provisionRequest) {
        if (provisionRequest.component().getSpecific() instanceof StorageSpecific ss) {
            return right(ss);
        } else {
            String errorMessage = String.format(
                    "The specific section of the component %s is not of type StorageSpecific",
                    provisionRequest.component().getId());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    /*
        This method is synchronized to avoid an error on Ranger
        when deploying multiple storage areas for the same data product
        (Ranger doesn't manage updating the same entity at the same time: first update works,
        second update return an error because the DB is locked)
        TODO: we can be more granular, it should be enough to synchronize (find & (create/update)) entities
    */
    private synchronized <T extends Specific> Either<FailedOperation, Void> upsertRangerEntities(
            ProvisionRequest<T> provisionRequest,
            String rootFolder,
            String path,
            List<String> ownerUsers,
            List<String> ownerGroups) {
        String deployUser = rangerConfig.ownerTechnicalUser();

        var eitherPrefixes = buildZoneName(provisionRequest.component().getId())
                .flatMap(zoneName -> buildOwnerRolePrefix(
                                provisionRequest.component().getId())
                        .flatMap(ownerRolePrefix -> buildUserRolePrefix(
                                        provisionRequest.component().getId())
                                .flatMap(userRolePrefix -> buildPolicyPrefix(
                                                provisionRequest.component().getId())
                                        .map(policyPrefix -> new Tuple6<>(
                                                zoneName,
                                                ownerRolePrefix,
                                                userRolePrefix,
                                                policyPrefix,
                                                buildRangerPolicyFolderPath(path),
                                                rootFolder)))));
        if (eitherPrefixes.isLeft()) {
            return left(eitherPrefixes.getLeft());
        }

        String zoneName = zoneName(eitherPrefixes.get()._1());
        String ownerRolePrefix = eitherPrefixes.get()._2();
        String userRolePrefix = eitherPrefixes.get()._3();
        String policyPrefix = eitherPrefixes.get()._4();
        String rangerFolderPath = eitherPrefixes.get()._5();
        String securityZoneFolderPath = eitherPrefixes.get()._6();

        var rangerZoneRes = rangerService.findSecurityZoneByName(zoneName).flatMap(s -> Option.ofOptional(s)
                .fold(
                        () -> rangerService.createSecurityZone(securityZone(
                                zoneName, rangerConfig.hdfsServiceName(), deployUser, securityZoneFolderPath)),
                        sz -> rangerService.updateSecurityZone(
                                securityZone(sz, rangerConfig.hdfsServiceName(), securityZoneFolderPath))));
        if (rangerZoneRes.isLeft()) return left(rangerZoneRes.getLeft());

        String ownerRoleName = generateOwnerRoleName(ownerRolePrefix);
        var ownerRangerRoleRes = upsertRole(ownerRoleName, ownerUsers, ownerGroups, deployUser);
        if (ownerRangerRoleRes.isLeft()) return left(ownerRangerRoleRes.getLeft());

        String userRoleName = generateUserRoleName(userRolePrefix);
        var userRangerRoleRes = upsertRole(userRoleName, Collections.emptyList(), Collections.emptyList(), deployUser);
        if (userRangerRoleRes.isLeft()) return left(userRangerRoleRes.getLeft());

        var componentRangerPolicy = rangerService
                .findPolicyByName(rangerConfig.hdfsServiceName(), policyName(policyPrefix), Optional.of(zoneName))
                .flatMap(p -> Option.ofOptional(p)
                        .fold(
                                () -> rangerService.createPolicy(rangerPolicy(
                                        policyPrefix,
                                        zoneName,
                                        rangerFolderPath,
                                        ownerRoleName,
                                        userRoleName,
                                        rangerConfig.hdfsServiceName())),
                                pp -> rangerService.updatePolicy(rangerPolicy(
                                        pp,
                                        policyPrefix,
                                        zoneName,
                                        rangerFolderPath,
                                        ownerRoleName,
                                        userRoleName,
                                        rangerConfig.hdfsServiceName()))));
        if (componentRangerPolicy.isLeft()) return left(componentRangerPolicy.getLeft());
        return right(null);
    }

    private <T extends Specific> Either<FailedOperation, Tuple2<List<String>, List<String>>> mapOwners(
            ProvisionRequest<T> provisionRequest) {
        // FIXME workaround until related bug is fixed in witboost
        String devGroup = provisionRequest.dataProduct().getDevGroup().startsWith("group:")
                ? provisionRequest.dataProduct().getDevGroup()
                : "group:".concat(provisionRequest.dataProduct().getDevGroup());

        var owners = Set.of(provisionRequest.dataProduct().getDataProductOwner(), devGroup);
        var eitherPrincipals = principalMappingService.map(owners);
        var problems = eitherPrincipals.values().stream()
                .filter(Either::isLeft)
                .map(Either::getLeft)
                .map(FailedOperation::problems)
                .collect(ArrayList<Problem>::new, List::addAll, List::addAll);
        if (!problems.isEmpty()) return left(new FailedOperation(problems));
        var principals = eitherPrincipals.values().stream().map(Either::get).toList();
        return right(new Tuple2<>(
                principals.stream()
                        .filter(p -> p instanceof CDPUser)
                        .map(p -> (CDPUser) p)
                        .map(CDPUser::userId)
                        .toList(),
                principals.stream()
                        .filter(p -> p instanceof CDPGroup)
                        .map(p -> (CDPGroup) p)
                        .map(CDPGroup::name)
                        .toList()));
    }

    public FailedOperation updateAcl() {
        String errorMessage = "Updating Access Control Lists is not supported by the Storage Area Component";
        logger.error(errorMessage);
        return new FailedOperation(Collections.singletonList(new Problem(errorMessage)));
    }

    private String buildRangerPolicyFolderPath(String path) {
        return String.format("%s*", path);
    }

    private Either<FailedOperation, String> buildPolicyPrefix(String componentId) {
        var eitherIdentifiers = extractIdentifiers(componentId);
        return eitherIdentifiers.map(identifiers -> String.format(
                "%s_%s_%s_%s",
                identifiers.domain(),
                identifiers.dataProductId(),
                identifiers.dataProductMajorVersion(),
                identifiers.componentId()));
    }

    private Either<FailedOperation, String> buildOwnerRolePrefix(String componentId) {
        var eitherIdentifiers = extractIdentifiers(componentId);
        return eitherIdentifiers.map(identifiers -> String.format(
                "%s_%s_%s", identifiers.domain(), identifiers.dataProductId(), identifiers.dataProductMajorVersion()));
    }

    private Either<FailedOperation, String> buildZoneName(String componentId) {
        var eitherIdentifiers = extractIdentifiers(componentId);
        return eitherIdentifiers.map(identifiers -> String.format(
                "%s_%s_%s", identifiers.domain(), identifiers.dataProductId(), identifiers.dataProductMajorVersion()));
    }
}
