package com.twilio.raas.dataloader.generator;

import com.twilio.sids.SidUtil;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ActorSidGenerator extends SingleColumnValueGenerator<String> {

  private final Random rand = new Random();

  private final List<String> actorSids;

  public ActorSidGenerator() {
    actorSids = IntStream.range(0, 8)
      .mapToObj(index -> {
        final double choice = rand.nextDouble();
        final String prefix;
        // Signing Key
        if (choice < 0.25) {
          prefix = "SK";
        }
        // Account Sid
        else if (choice < 0.35) {
          prefix = "AC";
        }
        else if (choice < 0.5) {
          // TA
          prefix = "TA";
        }
        else {
          // User Sid
          prefix = "US";
        }
        return SidUtil.generateGUID(prefix).toString();
      })
      .collect(Collectors.toList());
  }

  @Override
  public String getColumnValue() {
    return actorSids.get(rand.nextInt(actorSids.size()));
  }

}
