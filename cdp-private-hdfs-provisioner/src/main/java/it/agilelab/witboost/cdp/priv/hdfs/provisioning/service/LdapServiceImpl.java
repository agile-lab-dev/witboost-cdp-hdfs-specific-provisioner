package it.agilelab.witboost.cdp.priv.hdfs.provisioning.service;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import io.vavr.control.Either;
import io.vavr.control.Option;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.FailedOperation;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.common.Problem;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.config.LdapConfig;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.CDPGroup;
import it.agilelab.witboost.cdp.priv.hdfs.provisioning.model.CDPUser;
import java.util.Collections;
import org.ldaptive.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class LdapServiceImpl implements LdapService {
    private final Logger logger = LoggerFactory.getLogger(LdapServiceImpl.class);
    private final SearchOperation searchOperation;
    private final LdapConfig ldapConfig;

    public LdapServiceImpl(SearchOperation searchOperation, LdapConfig ldapConfig) {
        this.searchOperation = searchOperation;
        this.ldapConfig = ldapConfig;
    }

    @Override
    public Either<FailedOperation, Option<CDPUser>> findUserByMail(String mail) {
        FilterTemplate filterTemplate =
                FilterTemplate.builder()
                        .filter(ldapConfig.userSearchFilter())
                        .parameter("mail", mail)
                        .build();
        SearchRequest searchRequest =
                SearchRequest.builder().dn(ldapConfig.searchBaseDN()).filter(filterTemplate).build();
        try {
            SearchResponse searchResponse = searchOperation.execute(searchRequest);
            LdapEntry entry = searchResponse.getEntry();
            if (entry == null) return right(Option.none());
            String userId = entry.getAttribute(ldapConfig.userAttributeName()).getStringValue();
            String ldapEmail = entry.getAttribute("mail").getStringValue();
            return right(Option.of(new CDPUser(userId, ldapEmail)));
        } catch (LdapException e) {
            String errorMessage =
                    String.format(
                            "An error occurred while searching for the user '%s' on LDAP. Please try again and if the error persists contact the platform team. Details: %s",
                            mail, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @Override
    public Either<FailedOperation, Option<CDPGroup>> findGroupByName(String name) {
        FilterTemplate filterTemplate =
                FilterTemplate.builder()
                        .filter(ldapConfig.groupSearchFilter())
                        .parameter("group", name)
                        .build();
        SearchRequest searchRequest =
                SearchRequest.builder().dn(ldapConfig.searchBaseDN()).filter(filterTemplate).build();
        try {
            SearchResponse searchResponse = searchOperation.execute(searchRequest);
            LdapEntry entry = searchResponse.getEntry();
            if (entry == null) return right(Option.none());
            String group = entry.getAttribute(ldapConfig.groupAttributeName()).getStringValue();
            return right(Option.of(new CDPGroup(group)));
        } catch (LdapException e) {
            String errorMessage =
                    String.format(
                            "An error occurred while searching for the group '%s' on LDAP. Please try again and if the error persists contact the platform team. Details: %s",
                            name, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
