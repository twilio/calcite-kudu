package com.twilio.raas.sql;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public final class KuduSchema extends AbstractSchema {

    private static final Logger logger = LoggerFactory.getLogger(KuduSchema.class);

    private final AsyncKuduClient client;
    private final Map<String, KuduTableConfig> kuduTableConfigMap;
    private Optional<Map<String, Table>> cachedTableMap = Optional.empty();

    public KuduSchema(final String connectString) {
        this(connectString, Collections.EMPTY_MAP);
    }

    public KuduSchema(final String connectString, final Map<String, KuduTableConfig> kuduTableConfigMap) {
        this.client = new AsyncKuduClient.AsyncKuduClientBuilder(connectString).build();
        this.kuduTableConfigMap = kuduTableConfigMap;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (cachedTableMap.isPresent()) {
            return cachedTableMap.get();
        }

        HashMap<String, Table> tableMap = new HashMap<>();
        final List<String> tableNames;
        try {
            tableNames = this.client
                .getTablesList()
                .join()
                .getTablesList();
        }
        catch (Exception threadInterrupted) {
            return Collections.emptyMap();
        }
        for (String tableName: tableNames) {
            try {
                final KuduTable openedTable = client.openTable(tableName).join();
                final List<String> descendingSortedColumns = kuduTableConfigMap.containsKey(tableName) ? kuduTableConfigMap.get(tableName).getDescendingSortedFields() : Collections.<String>emptyList();
                final List<Integer> descendingSortedColumnIndices = descendingSortedColumns.stream().map(name -> openedTable.getSchema().getColumnIndex(name)).collect(Collectors.toList());
                tableMap.put(tableName,
                             new CalciteKuduTable(client.openTable(tableName).join(),
                                                  client,
                                                  descendingSortedColumnIndices));
            }
            catch (Exception failedToOpen) {
                // @TODO: hmm this seems wrong.
                // Ideally? CalciteKuduTable would take in
                // tableName or an openedTable? and if the
                // table wasn't opened it would attempt to
                // open it prior to sending queries?
                logger.error("Unable to open table " + tableName, failedToOpen);
            }      
        }
        if (!tableMap.isEmpty()) {
            cachedTableMap = Optional.of(tableMap);
        }
        return tableMap;
    }  
}
