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

import fr.graphlabs.neo4j.*
import fr.graphlabs.neo4j.agnostic.collections.Streams
import fr.graphlabs.neo4j.agnostic.collections.batch
import fr.graphlabs.neo4j.agnostic.collections.setValuesUpperCase
import fr.graphlabs.neo4j.agnostic.csv.CsvMapIterator
import org.neo4j.driver.v1.*
import java.io.Reader
import java.util.*
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
        database.session(AccessMode.WRITE).use {
            createIndices(it)
        }
        reader.use {
            streamRows(it)
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

    private fun streamRows(reader: Reader): Stream<Row> {
        return Streams.fromIterator(
                CsvMapIterator(arrayOf("company_id",
                        "country_code", "country_name",
                        "segment_code", "segment_label",
                        "company_name",
                        "address_1", "address_2", "address_3", "address_4",
                        "zipcode",
                        "city_name"), reader
                )
        ).map {
            @Suppress("UNCHECKED_CAST")
            val row = it.toMutableMap() as MutableRow
            joinAddress(row, it)
            row.setValuesUpperCase(Locale.FRENCH)
            row
        }
    }

    private fun joinAddress(row: MutableRow, it: Row) {
        row["address"] = mergeAddress(it["address_1"], it["address_2"], it["address_3"], it["address_4"])
        row.remove("address_1")
        row.remove("address_2")
        row.remove("address_3")
        row.remove("address_4")
    }

    private fun upsertCompanyGraph(session: Session, rows: List<Row>): StatementResult? {
        return session.run("""
            |UNWIND {rows} AS row
            |MERGE (country:Country {code: row.country_code, name: row.country_name})
            |MERGE (city:City {name: row.city_name})
            |MERGE (city)-[:LOCATED_IN_COUNTRY]->(country)
            |MERGE (address:Address {address: row.address})
            |MERGE (address)-[:LOCATED_IN_CITY {zipcode: row.zipcode}]->(city)
            |MERGE (segment:BusinessSegment {code: row.segment_code, label: row.segment_label})
            |MERGE (company:Company {identifier: row.company_id, name: row.company_name})
            |MERGE (company)-[:IN_BUSINESS_SEGMENT]->(segment)
            |MERGE (company)-[:LOCATED_AT_ADDRESS]->(address)
            |RETURN true""".trimMargin(), mapOf(Pair("rows", rows)))
    }

    private fun createIndices(session: Session) {
        session.beginTransaction().use {
            it.run("CREATE INDEX ON :Country(name)")
            it.run("CREATE CONSTRAINT ON (c:Country) ASSERT c.code IS UNIQUE")
            it.run("CREATE INDEX ON :City(name)")
            it.run("CREATE INDEX ON :BusinessSegment(name)")
            it.run("CREATE CONSTRAINT ON (b:BusinessSegment) ASSERT b.code IS UNIQUE")
            it.run("CREATE INDEX ON :Company(name)")
            it.run("CREATE CONSTRAINT ON (c:Company) ASSERT c.identifier IS UNIQUE")
            it.success()
        }
    }

    private fun mergeAddress(addressFirstPart: Any?, addressSecondPart: Any?, addressThirdPart: Any?, addressFourthPart: Any?): String {
        var builder = StringBuilder()
        builder = builder.append(addressFirstPart?.toString() ?: "".toUpperCase(Environment.locale))
        builder = appendNotBlank(addressSecondPart?.toString() ?: "", builder)
        builder = appendNotBlank(addressThirdPart?.toString() ?: "", builder)
        builder = appendNotBlank(addressFourthPart?.toString() ?: "", builder)
        return builder.toString()
    }

    private fun appendNotBlank(part: String, builder: StringBuilder): StringBuilder {
        return if (part.isBlank() || part == "\"\"") {
            builder
        } else {
            builder.append("\n").append(part.toUpperCase(Locale.FRENCH))
        }
    }

}

