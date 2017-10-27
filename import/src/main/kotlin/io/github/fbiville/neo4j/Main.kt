/**
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.fbiville.neo4j


import com.beust.jcommander.JCommander
import io.github.fbiville.neo4j.companies.CompanyImportCommand
import io.github.fbiville.neo4j.drugs.DrugImportCommand
import io.github.fbiville.neo4j.packages.PackageImportCommand

fun main(args: Array<String>) {
    val main = parseCommand(args)
    main.performImport()
}

private fun parseCommand(args: Array<String>): ImportCommand {
    val companyImportCommand = CompanyImportCommand()
    val drugImportCommand = DrugImportCommand()
    val packageImportCommand = PackageImportCommand()
    val commandParser = JCommander.newBuilder()
            .addCommand("companies", companyImportCommand)
            .addCommand("packages", packageImportCommand)
            .addCommand("drugs", drugImportCommand)
            .build()

    commandParser.parse(*args)

    val parsedCommand = commandParser.parsedCommand
    return when (parsedCommand) {
        "companies" -> companyImportCommand
        "drugs" -> drugImportCommand
        "packages" -> packageImportCommand
        else -> throw IllegalArgumentException("Unsupported command: $parsedCommand")
    }
}

