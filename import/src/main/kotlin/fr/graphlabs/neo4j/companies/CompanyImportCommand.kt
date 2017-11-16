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
package fr.graphlabs.neo4j.companies

import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import fr.graphlabs.neo4j.ImportCommand
import java.io.BufferedReader
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

@Parameters(commandDescription = "Imports companies into Neo4j")
class CompanyImportCommand : ImportCommand {

    @Parameter(names = arrayOf("-f", "--defined-in"), required = true, description = "Path to CSV file")
    lateinit var csvFile: String

    @Parameter(names = arrayOf("-b", "--to-graph"), required = true, description = "Neo4j Bolt URI")
    lateinit var boltUri: String

    @Parameter(names = arrayOf("-u", "--username"), description = "Neo4j username")
    var username: String? = null

    @Parameter(names = arrayOf("-p", "--password"), password = true, description = "Neo4j password")
    var password: String? = null

    @Parameter(names = arrayOf("-s", "--batch-size"), description = "Advanced: batch size")
    var batchSize: Int = 500

    override fun performImport() {
        val importer = CompanyImporter(boltUri, username, password)
        importer.import(BufferedReader(fileReader(csvFile)), batchSize)
    }

    private fun fileReader(path: String) = InputStreamReader(FileInputStream(path), StandardCharsets.UTF_8)
}
