package com.twilio.raas.sql;

import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.calcite.schema.Table;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.List;
import com.google.common.collect.Multimap;
import com.google.common.collect.ImmutableMultimap;
import java.sql.Timestamp;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import java.util.Collection;
import java.util.Set;
import java.util.Optional;


public final class KuduSchema extends AbstractSchema {

    private final AsyncKuduClient client;
    private Optional<Map<String, String>> descendingSortedTables = Optional.empty();
    private Optional<Map<String, Table>> cachedTableMap = Optional.empty();

    public KuduSchema(final String connectString) {
        this(connectString, Optional.empty());
    }

    public KuduSchema(final String connectString, final Optional<Map<String, String>> descendingSortedTables) {
        this.client = new AsyncKuduClient.AsyncKuduClientBuilder(connectString).build();
        this.descendingSortedTables = descendingSortedTables;
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
                tableMap.put(tableName,
                             new CalciteKuduTable(client.openTable(tableName).join(),
                                                  client,
                                                  (descendingSortedTables.isPresent() && descendingSortedTables.get().containsKey(tableName)) ?
                                                      Optional.of(descendingSortedTables.get().get(tableName)) : Optional.empty()));
            }
            catch (Exception failedToOpen) {
                // @TODO: hmm this seems wrong.
                // Ideally? CalciteKuduTable would take in
                // tableName or an openedTable? and if the
                // table wasn't opened it would attempt to
                // open it prior to sending queries?
            }      
        }
        if (!tableMap.isEmpty()) {
            cachedTableMap = Optional.of(tableMap);
        }
        return tableMap;
    }  
}
