package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import io.vavr.control.Either;
import io.vavr.control.Option;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.CDPGroup;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.CDPUser;

public interface LdapService {
    Either<FailedOperation, Option<CDPUser>> findUserByMail(String mail);

    Either<FailedOperation, Option<CDPGroup>> findGroupByName(String name);
}
