package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import io.vavr.control.Either;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.CDPIdentity;
import java.util.Map;
import java.util.Set;

/***
 * Principal mapping services
 */
public interface PrincipalMappingService {
    /**
     * Map subjects to Cdp Identities
     *
     * @param subjects the set of subjects to map
     * @return return a FailedOperation or the mapped record for every subject to be mapped
     */
    Map<String, Either<FailedOperation, CDPIdentity>> map(Set<String> subjects);
}
