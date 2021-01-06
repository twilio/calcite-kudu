/* Copyright 2021 Twilio, Inc
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
package com.twilio.kudu.cli;

import java.io.IOException;

import com.twilio.kudu.sql.JDBCUtil;
import com.twilio.kudu.sql.schema.DefaultKuduSchemaFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import sqlline.SqlLine;

public final class KuduClient {
  public static void main(String[] args) throws IOException, ParseException {

    final Options options = new Options();
    options.addOption("c", "connection", true, "connection string to kudu leaders");
    final CommandLineParser parser = new DefaultParser();
    final CommandLine cmd = parser.parse(options, args);
    final String connectionString = cmd.getOptionValue("c");

    String[] updatedArgs = new String[args.length + 2];
    updatedArgs[0] = "-u";
    updatedArgs[1] = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
        DefaultKuduSchemaFactory.class.getName() + "#INSTANCE", connectionString);
    updatedArgs[2] = "--colorScheme=solarized";
    updatedArgs[3] = "--connectInteractionMode=notAskCredentials";

    SqlLine.main(updatedArgs);
  }
}
