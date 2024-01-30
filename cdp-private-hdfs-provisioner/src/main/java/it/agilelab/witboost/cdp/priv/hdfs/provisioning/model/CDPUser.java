package it.agilelab.witboost.cdp.priv.hdfs.provisioning.model;

public record CDPUser(String userId, String email) implements CDPIdentity {}
