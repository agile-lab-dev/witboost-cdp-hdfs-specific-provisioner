package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import java.util.Optional;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerSecurityZone;

/***
 * Ranger services
 */
public interface RangerService {
    /***
     * Find a security zone with the supplied name
     * @param zoneName name of the security zone
     * @return the optional security zone if existing or the error encountered
     */
    Either<FailedOperation, Optional<RangerSecurityZone>> findSecurityZoneByName(String zoneName);

    /***
     * Create a new security zone with the supplied data
     * @param zone the security zone to create
     * @return the created security zone or the error encountered
     */
    Either<FailedOperation, RangerSecurityZone> createSecurityZone(RangerSecurityZone zone);

    /***
     * Update an existing security zone with the supplied data
     * @param zone the security zone to update
     * @return the updated security zone or the error encountered
     */
    Either<FailedOperation, RangerSecurityZone> updateSecurityZone(RangerSecurityZone zone);

    /***
     * Find a policy with the supplied parameters
     * @param serviceName service name
     * @param policyName policy name
     * @param zoneName optional security zone name
     * @return the optional policy if existing or the error encountered
     */
    Either<FailedOperation, Optional<RangerPolicy>> findPolicyByName(
            String serviceName, String policyName, Optional<String> zoneName);

    /***
     * Create a new policy with the supplied data
     * @param policy policy to create
     * @return the created policy or the error encountered
     */
    Either<FailedOperation, RangerPolicy> createPolicy(RangerPolicy policy);

    /***
     * Update an existing policy with the supplied data
     * @param policy policy to update
     * @return the updated policy or the error encountered
     */
    Either<FailedOperation, RangerPolicy> updatePolicy(RangerPolicy policy);

    /***
     * Delete an existing policy with the supplied data
     * @param policy policy to delete
     * @return nothing or the error encountered
     */
    Either<FailedOperation, Void> deletePolicy(RangerPolicy policy);

    /***
     * Find a role with the supplied name
     * @param roleName role name
     * @return the optional role if existing or the error encountered
     */
    Either<FailedOperation, Optional<RangerRole>> findRoleByName(String roleName);

    /***
     * Create a new role with the supplied data
     * @param role role to create
     * @return the created role or the error encountered
     */
    Either<FailedOperation, RangerRole> createRole(RangerRole role);

    /***
     * Update an existing role with the supplied data
     * @param role role to update
     * @return the updated role or the error encountered
     */
    Either<FailedOperation, RangerRole> updateRole(RangerRole role);

    /***
     * Delete an existing role with the supplied data
     * @param role role to delete
     * @return nothing or the error encountered
     */
    Either<FailedOperation, Void> deleteRole(RangerRole role);
}
