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
package fr.graphlabs.neo4j

import com.beust.jcommander.JCommander
import fr.graphlabs.neo4j.benefits.BenefitImportCommand
import fr.graphlabs.neo4j.companies.CompanyImportCommand
import fr.graphlabs.neo4j.drugs.DrugImportCommand
import fr.graphlabs.neo4j.packages.PackageImportCommand

fun main(args: Array<String>) {
    val main = parseCommand(args)
    main.performImport()
}

private fun parseCommand(args: Array<String>): ImportCommand {

    val commands = mapOf<String, ImportCommand>(
            Pair("companies", CompanyImportCommand()),
            Pair("packages", PackageImportCommand()),
            Pair("benefits", BenefitImportCommand()),
            Pair("drugs", DrugImportCommand())
    )

    val parsedCommand = parseCommand(commandParser(commands), args)
    return when (parsedCommand) {
        in commands.keys -> commands[parsedCommand]!!
        else -> throw IllegalArgumentException("Unsupported command: $parsedCommand")
    }
}

private fun parseCommand(parser: JCommander, args: Array<String>): String? {
    parser.parse(*args)
    val parsedCommand = parser.parsedCommand
    return parsedCommand
}

private fun commandParser(commands: Map<String, ImportCommand>): JCommander {
    val builder = JCommander.newBuilder()
    commands.forEach { builder.addCommand(it.key, it.value) }
    return builder.build()
}

