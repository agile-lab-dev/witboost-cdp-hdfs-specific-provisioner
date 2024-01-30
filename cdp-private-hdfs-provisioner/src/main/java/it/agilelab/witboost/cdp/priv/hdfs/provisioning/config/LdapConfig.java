package it.agilelab.witboost.cdp.priv.hdfs.provisioning.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "mapping.ldap")
public record LdapConfig(
        String url,
        Boolean useTls,
        Integer timeout,
        String bindUsername,
        String bindPassword,
        String searchBaseDN,
        String userSearchFilter,
        String groupSearchFilter,
        String userAttributeName,
        String groupAttributeName) {}
