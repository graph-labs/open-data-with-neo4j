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
package io.github.fbiville.neo4j.companies

import io.github.fbiville.neo4j.Environment
import io.github.fbiville.neo4j.batch
import org.neo4j.driver.v1.AccessMode
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.StatementResult
import java.io.BufferedReader
import java.io.Reader
import java.util.Locale
import java.util.stream.Stream
import kotlin.streams.asSequence

class CompanyImporter(boltUri: String, username: String? = null, password: String? = null) {

    private val database =
            if (username == null) {
                GraphDatabase.driver(boltUri)
            } else {
                GraphDatabase.driver(boltUri, AuthTokens.basic(username, password.orEmpty()))
            }

    fun import(reader: Reader, commitPeriod: Int = 500) {
        BufferedReader(reader).use {
            streamRows(it.lines().skip(1))
                    .asSequence()
                    .batch(commitPeriod)
                    .forEach {
                        val rows = it
                        database.session(AccessMode.WRITE).use {
                            upsertCompanyGraph(it, rows)
                        }
                    }
        }
    }

    private fun streamRows(rows: Stream<String>): Stream<Map<String, String>> {
        return rows.map {
            val fields = it.split(",")
            mapOf(
                    Pair("company_id", fields[0].toUpperCase(Environment.locale)),
                    Pair("country_code", fields[1].toUpperCase(Environment.locale)),
                    Pair("country_name", fields[2].toUpperCase(Environment.locale)),
                    Pair("sector_code", fields[3].toUpperCase(Environment.locale)),
                    Pair("sector_label", fields[4].toUpperCase(Environment.locale)),
                    Pair("company_name", fields[5].toUpperCase(Environment.locale)),
                    Pair("address", mergeAddress(
                            fields[6],
                            fields[7],
                            fields[8],
                            fields[9])),
                    Pair("zipcode", fields[10].toUpperCase(Environment.locale)),
                    Pair("city_name", fields[11].toUpperCase(Environment.locale))
            )
        }
    }

    private fun upsertCompanyGraph(session: Session, rows: List<Map<String, String>>): StatementResult? {
        return session.run("""
            |UNWIND {rows} AS row
            |MERGE (country:Country {code: row.country_code, name: row.country_name})
            |MERGE (city:City {name: row.city_name})
            |MERGE (city)-[:LOCATED_IN_COUNTRY]->(country)
            |MERGE (address:Address {address: row.address})
            |MERGE (address)-[:LOCATED_IN_CITY {zipcode: row.zipcode}]->(city)
            |MERGE (segment: BusinessSegment {code: row.sector_code, label: row.sector_label})
            |MERGE (company: Company {identifier: row.company_id, name: row.company_name})
            |MERGE (company)-[:IN_BUSINESS_SEGMENT]->(segment)
            |MERGE (company)-[:LOCATED_AT_ADDRESS]->(address)
            |RETURN true""".trimMargin(), mapOf(Pair("rows", rows)))
    }

    private fun mergeAddress(addressFirstPart: String, addressSecondPart: String, addressThirdPart: String, addressFourthPart: String): String {
        var builder = StringBuilder()
        builder = builder.append(addressFirstPart.toUpperCase(Environment.locale))
        builder = appendNotBlank(addressSecondPart, builder)
        builder = appendNotBlank(addressThirdPart, builder)
        builder = appendNotBlank(addressFourthPart, builder)
        return builder.toString()
    }

    private fun appendNotBlank(part: String, builder: StringBuilder): StringBuilder {
        return if (part != "\"\"") {
            builder.append("\n").append(part.toUpperCase(Locale.FRENCH))
        } else {
            builder
        }
    }

}

