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
    private final PrincipalMappingService principalMappingService;
    private final RangerService rangerService;
    private final HdfsService hdfsService;
    private final RangerConfig rangerConfig;
    private final Logger logger = LoggerFactory.getLogger(StorageAreaHandler.class);

    public StorageAreaHandler(
            PrincipalMappingService principalMappingService,
            RangerService rangerService,
            HdfsService hdfsService,
            RangerConfig rangerConfig) {
        this.principalMappingService = principalMappingService;
        this.rangerService = rangerService;
        this.hdfsService = hdfsService;
        this.rangerConfig = rangerConfig;
    }

    public <T extends Specific> Either<FailedOperation, String> create(ProvisionRequest<T> provisionRequest) {
        var eitherOwners = mapOwners(provisionRequest);
        if (eitherOwners.isLeft()) return left(eitherOwners.getLeft());
        var ownerUsers = eitherOwners.get()._1();
        var ownerGroups = eitherOwners.get()._2();

        var eitherPrefixPath = getPrefixPath(provisionRequest);
        if (eitherPrefixPath.isLeft()) return left(eitherPrefixPath.getLeft());
        String prefixPath = eitherPrefixPath.get();

        var rangerRes = upsertRangerEntities(provisionRequest, prefixPath, ownerUsers, ownerGroups);
        if (rangerRes.isLeft()) return left(rangerRes.getLeft());

        var eitherHdfsFolderPath =
                buildHdfsFolderPath(provisionRequest.component().getId(), prefixPath);
        return eitherHdfsFolderPath.flatMap(hdfsService::createFolder);
    }

    public <T extends Specific> Either<FailedOperation, Void> destroy(ProvisionRequest<T> provisionRequest) {
        var rangerRes = deleteRangerEntities(provisionRequest.component().getId());
        if (rangerRes.isLeft()) return left(rangerRes.getLeft());

        if (Boolean.TRUE.equals(provisionRequest.removeData())) {
            var eitherPrefixPath = getPrefixPath(provisionRequest);
            if (eitherPrefixPath.isLeft()) return left(eitherPrefixPath.getLeft());
            String prefixPath = eitherPrefixPath.get();

            var eitherHdfsFolderPath =
                    buildHdfsFolderPath(provisionRequest.component().getId(), prefixPath);
            return eitherHdfsFolderPath.flatMap(hdfsService::deleteFolder).map(f -> null);
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

    private <T extends Specific> Either<FailedOperation, String> getPrefixPath(ProvisionRequest<T> provisionRequest) {
        String prefixPath;
        if (provisionRequest.component().getSpecific() instanceof StorageSpecific ss) {
            prefixPath = ss.getPrefixPath();
        } else {
            String errorMessage = String.format(
                    "The specific section of the component %s is not of type StorageSpecific",
                    provisionRequest.component().getId());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
        return right(prefixPath);
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
            String prefixPath,
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
                                        .flatMap(policyPrefix -> buildRangerPolicyFolderPath(
                                                        provisionRequest
                                                                .component()
                                                                .getId(),
                                                        prefixPath)
                                                .flatMap(rangerFolderPath -> buildSecurityZoneFolderPath(
                                                                provisionRequest
                                                                        .component()
                                                                        .getId(),
                                                                prefixPath)
                                                        .map(securityZoneFolderPath -> new Tuple6<>(
                                                                zoneName,
                                                                ownerRolePrefix,
                                                                userRolePrefix,
                                                                policyPrefix,
                                                                rangerFolderPath,
                                                                securityZoneFolderPath)))))));
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
        var ownerRangerRoleRes = rangerService.findRoleByName(ownerRoleName).flatMap(r -> Option.ofOptional(r)
                .fold(
                        () -> rangerService.createRole(rangerRole(ownerRoleName, ownerUsers, ownerGroups, deployUser)),
                        rr -> rangerService.updateRole(rangerRole(rr, ownerUsers, ownerGroups))));
        if (ownerRangerRoleRes.isLeft()) return left(ownerRangerRoleRes.getLeft());

        String userRoleName = generateUserRoleName(userRolePrefix);
        var userRangerRoleRes = rangerService.findRoleByName(userRoleName).flatMap(r -> Option.ofOptional(r)
                .fold(
                        () -> rangerService.createRole(
                                rangerRole(userRoleName, Collections.emptyList(), Collections.emptyList(), deployUser)),
                        rr -> rangerService.updateRole(
                                rangerRole(rr, Collections.emptyList(), Collections.emptyList()))));
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
        var owners = Set.of(
                provisionRequest.dataProduct().getDataProductOwner(),
                provisionRequest.dataProduct().getDevGroup());
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

    private Either<FailedOperation, String> buildSecurityZoneFolderPath(String componentId, String prefixPath) {
        var eitherIdentifiers = extractIdentifiers(componentId);
        return eitherIdentifiers.map(identifiers -> prefixPathWithTrailingSlash(prefixPath)
                .concat(String.format(
                        "%s/data-products/%s/%s",
                        identifiers.domain(), identifiers.dataProductId(), identifiers.dataProductMajorVersion())));
    }

    private Either<FailedOperation, String> buildRangerPolicyFolderPath(String componentId, String prefixPath) {
        return buildHdfsFolderPath(componentId, prefixPath).map(p -> String.format("%s*", p));
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
