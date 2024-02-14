package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class RangerServiceImpl implements RangerService {
    private final Logger logger = LoggerFactory.getLogger(RangerServiceImpl.class);

    private final RangerClient rangerClient;

    public RangerServiceImpl(RangerClient rangerClient) {
        this.rangerClient = rangerClient;
    }

    @Override
    public Either<FailedOperation, Optional<RangerSecurityZone>> findSecurityZoneByName(String zoneName) {
        try {
            return right(Optional.of(rangerClient.getSecurityZone(zoneName)));
        } catch (RangerServiceException e) {
            int statusCode = e.getStatus() != null ? e.getStatus().getStatusCode() : 0;
            if (statusCode == 400 || statusCode == 404) return right(Optional.empty());
            String errorMessage = String.format(
                    "An error occurred while searching for security zone '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    zoneName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, RangerSecurityZone> createSecurityZone(RangerSecurityZone zone) {
        try {
            return right(rangerClient.createSecurityZone(zone));
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while creating the security zone '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    zone.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, RangerSecurityZone> updateSecurityZone(RangerSecurityZone zone) {
        try {
            return right(rangerClient.updateSecurityZone(zone.getId(), zone));
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while updating the security zone '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    zone.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, Optional<RangerPolicy>> findPolicyByName(
            String serviceName, String policyName, Optional<String> zoneName) {
        try {
            Map<String, String> filter = new HashMap<>(Map.of("serviceName", serviceName, "policyName", policyName));
            zoneName.ifPresent(z -> filter.put("zoneName", z));
            return right(rangerClient.findPolicies(filter).stream().findFirst());
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while searching for policy '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    policyName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, RangerPolicy> createPolicy(RangerPolicy policy) {
        try {
            return right(rangerClient.createPolicy(policy));
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while creating the policy '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    policy.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, RangerPolicy> updatePolicy(RangerPolicy policy) {
        try {
            return right(rangerClient.updatePolicy(policy.getId(), policy));
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while updating the policy '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    policy.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, Void> deletePolicy(RangerPolicy policy) {
        try {
            rangerClient.deletePolicy(policy.getId());
            return right(null);
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while deleting the policy '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    policy.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, Optional<RangerRole>> findRoleByName(String roleName) {
        try {
            return right(rangerClient.findRoles(Collections.singletonMap("roleName", roleName)).stream()
                    .findFirst());
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while searching for role '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    roleName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, RangerRole> createRole(RangerRole role) {
        try {
            return right(rangerClient.createRole("", role));
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while creating the role '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    role.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, RangerRole> updateRole(RangerRole role) {
        try {
            return right(rangerClient.updateRole(role.getId(), role));
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while updating the role '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    role.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, Void> deleteRole(RangerRole role) {
        try {
            rangerClient.deleteRole(role.getId());
            return right(null);
        } catch (RangerServiceException e) {
            String errorMessage = String.format(
                    "An error occurred while deleting the role '%s' on Ranger. Please try again and if the error persists contact the platform team. Details: %s",
                    role.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
