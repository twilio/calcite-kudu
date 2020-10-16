/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.dataloader.generator;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import org.apache.commons.codec.digest.DigestUtils;

public class IdGenerator extends SingleColumnValueGenerator<String> {

  private static final String RANDOM_ALG = "SHA1PRNG";
  private static final String RANDOM_PROVIDER = "SUN";

  private static final String ADDR;

  public String sidPrefix;

  private static final SecureRandom srand;

  static {
    try {
      srand = SecureRandom.getInstance(RANDOM_ALG, RANDOM_PROVIDER);
      // force PRNG to seed itself
      final byte[] bytes = new byte[64];
      srand.nextBytes(bytes);
    } catch (NoSuchAlgorithmException | NoSuchProviderException hostBad) {
      throw new ExceptionInInitializerError(
          "Could not create secure random with " + RANDOM_ALG + " and " + RANDOM_PROVIDER);
    }
    try {
      ADDR = InetAddress.getLocalHost().toString();
    } catch (final UnknownHostException e) {
      throw new ExceptionInInitializerError("Could not resolve localhost, bailing out");
    }
  }

  private IdGenerator() {
  }

  public IdGenerator(final String sidPrefix) {
    this.sidPrefix = sidPrefix;
  }

  @Override
  public String getColumnValue() {
    return randomSid();
  }

  protected String randomSid() {
    final byte[] bytes = new byte[64];
    srand.nextBytes(bytes);
    final String random = new String(bytes);

    return DigestUtils.md5Hex(ADDR + random + System.currentTimeMillis());
  }
}