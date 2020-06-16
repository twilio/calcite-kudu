package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.KuduCost;
import org.apache.calcite.prepare.CalcitePrepareImpl;

import java.util.Collections;
import java.util.List;

/**
 * Customized driver so that we can use our own cost factory
 */
public class KuduDriver extends Driver {

  public static final String CONNECT_STRING_PREFIX = "jdbc:kudu:";

  public static Function0<CalcitePrepare> CALCITE_PREPARE_FACTORY = () -> new CalcitePrepareImpl() {
    protected List<Function1<Context, RelOptPlanner>> createPlannerFactories() {
      return Collections.singletonList(
        context -> createPlanner(context, null, KuduCost.FACTORY));
    }
  };

  static {
    new KuduDriver().register();
  }

  protected Function0<CalcitePrepare> createPrepareFactory() {
    return CALCITE_PREPARE_FACTORY;
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    return new KuduMetaImpl((CalciteConnectionImpl) connection);
  }

  @Override
  protected Handler createHandler() {
    Handler handler = super.createHandler();
    return handler;
  }

}
