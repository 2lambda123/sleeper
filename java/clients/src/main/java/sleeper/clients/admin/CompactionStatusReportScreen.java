/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sleeper.clients.admin;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.console.ConsoleHelper;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.MenuOption;
import sleeper.status.report.CompactionJobStatusReport;
import sleeper.status.report.CompactionTaskStatusReport;
import sleeper.status.report.compaction.job.StandardCompactionJobStatusReporter;
import sleeper.status.report.compaction.task.CompactionTaskQuery;
import sleeper.status.report.compaction.task.StandardCompactionTaskStatusReporter;
import sleeper.status.report.job.query.JobQuery;

import java.util.Optional;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;
import static sleeper.clients.admin.JobStatusScreenHelper.promptForJobId;
import static sleeper.clients.admin.JobStatusScreenHelper.promptForRange;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.COMPACTION_STATUS_STORE_ENABLED;

public class CompactionStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ConsoleHelper consoleHelper;
    private final AdminConfigStore store;
    private final TableSelectHelper tableSelectHelper;

    public CompactionStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.consoleHelper = new ConsoleHelper(out, in);
        this.store = store;
        this.tableSelectHelper = new TableSelectHelper(out, in, store);
    }

    public void chooseArgsAndPrint(String instanceId) throws InterruptedException {
        InstanceProperties properties = store.loadInstanceProperties(instanceId);
        if (!properties.getBoolean(COMPACTION_STATUS_STORE_ENABLED)) {
            out.println("");
            out.println("Compaction status store not enabled. Please enable in instance properties to access this screen");
            confirmReturnToMainScreen(out, in);
        } else {
            out.clearScreen("");
            consoleHelper.chooseOptionUntilValid("Which compaction report would you like to run",
                    new MenuOption("Compaction Job Status Report", () ->
                            chooseArgsForCompactionJobStatusReport(instanceId)),
                    new MenuOption("Compaction Task Status Report", () ->
                            chooseArgsForCompactionTaskStatusReport(instanceId))
            ).run();
        }
    }

    private void chooseArgsForCompactionJobStatusReport(String instanceId) throws InterruptedException {
        Optional<TableProperties> tableOpt = tableSelectHelper.chooseTableOrReturnToMain(instanceId);
        if (tableOpt.isPresent()) {
            String tableName = tableOpt.get().get(TableProperty.TABLE_NAME);
            consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                    new MenuOption("All", () ->
                            runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.ALL)),
                    new MenuOption("Unfinished", () ->
                            runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.UNFINISHED)),
                    new MenuOption("Detailed", () ->
                            runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.DETAILED, promptForJobId(in))),
                    new MenuOption("Range", () ->
                            runCompactionJobStatusReport(instanceId, tableName, JobQuery.Type.RANGE, promptForRange(in)))
            ).run();
        }
    }

    private void chooseArgsForCompactionTaskStatusReport(String instanceId) throws InterruptedException {
        consoleHelper.chooseOptionUntilValid("Which query type would you like to use?",
                new MenuOption("All", () ->
                        runCompactionTaskStatusReport(instanceId, CompactionTaskQuery.ALL)),
                new MenuOption("Unfinished", () ->
                        runCompactionTaskStatusReport(instanceId, CompactionTaskQuery.UNFINISHED))
        ).run();
    }

    private void runCompactionJobStatusReport(String instanceId, String tableName, JobQuery.Type queryType) {
        runCompactionJobStatusReport(instanceId, tableName, queryType, "");
    }

    private void runCompactionJobStatusReport(String instanceId, String tableName, JobQuery.Type queryType, String queryParameters) {
        new CompactionJobStatusReport(store.loadCompactionJobStatusStore(instanceId),
                new StandardCompactionJobStatusReporter(out.printStream()), tableName, queryType, queryParameters).run();
        confirmReturnToMainScreen(out, in);
    }

    private void runCompactionTaskStatusReport(String instanceId, CompactionTaskQuery queryType) {
        new CompactionTaskStatusReport(store.loadCompactionTaskStatusStore(instanceId),
                new StandardCompactionTaskStatusReporter(out.printStream()), queryType).run();
        confirmReturnToMainScreen(out, in);
    }
}