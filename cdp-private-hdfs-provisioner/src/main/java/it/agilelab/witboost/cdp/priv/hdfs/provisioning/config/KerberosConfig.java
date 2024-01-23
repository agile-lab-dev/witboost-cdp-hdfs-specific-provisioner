package it.agilelab.witboost.cdp.priv.hdfs.provisioning.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/***
 * Kerberos configuration
 * @param keytabLocation Location of the keytab to be used for authentication
 * @param principal Principal
 */
@ConfigurationProperties(prefix = "kerberos")
public record KerberosConfig(String keytabLocation, String principal) {}
