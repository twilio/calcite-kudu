package com.twilio.raas.sql.rel.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;

public class KuduRelMetadataProvider {

    public static final RelMetadataProvider INSTANCE = ChainedRelMetadataProvider.of(
            // add the kudu rel metadata providers before the default metadata provider
            ImmutableList.of(KuduRelMdSelectivity.SOURCE, DefaultRelMetadataProvider.INSTANCE));

}
