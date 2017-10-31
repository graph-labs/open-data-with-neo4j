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
package fr.graphlabs.neo4j.labs

import org.neo4j.driver.v1.AccessMode
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.Value
import org.springframework.stereotype.Repository

@Repository
class LabsRepository(private val driver: Driver) {

    fun findAllByMarketedDrugPackage(drugPackage: String): List<Lab> {
        driver.session(AccessMode.READ).use {
            val result = it.run("""
                MATCH (lab:Company)<-[:DRUG_HELD_BY]-(:Drug)-[:DRUG_PACKAGED_AS]->(:Package {name: {name}})
                OPTIONAL MATCH (lab)-[:IN_BUSINESS_SEGMENT]->(segment:BusinessSegment),
                               (lab)-[:LOCATED_AT_ADDRESS]->(address:Address)-[cityLoc:LOCATED_IN_CITY]->(city:City),
                               (city)-[:LOCATED_IN_COUNTRY]->(country:Country)
                RETURN lab {.identifier, .name},
                       segment {.code, .label},
                       address {.address},
                       cityLoc {.zipcode},
                       city {.name},
                       country {.code, .name}
                ORDER BY lab.identifier ASC""".trimIndent(), mapOf(Pair("name", drugPackage)))

            return result.list().map {
                val labData = it["lab"].asMap()
                val segmentData = asNullableMap(it["segment"])
                val addressData = asNullableMap(it["address"])
                val cityLocData = asNullableMap(it["cityLoc"])
                val cityData = asNullableMap(it["city"])
                val countryData = asNullableMap(it["country"])
                if (segmentData == null
                        || addressData == null
                        || cityLocData == null
                        || cityData == null
                        || countryData == null) {
                    Lab(labData["identifier"].toString(), labData["name"].toString())
                } else {
                    val country = country(countryData)
                    val city = city(cityData, country)
                    Lab(labData["identifier"].toString(),
                        labData["name"].toString(),
                        businessSegment(segmentData),
                        address(addressData, cityLocData, city)
                    )
                }
            }
        }
    }

    private fun businessSegment(segmentData: Map<String, Any>) =
            BusinessSegment(segmentData["code"].toString(), segmentData["label"].toString())

    private fun address(addressData: Map<String, Any>,
                        cityLocData: Map<String, Any>,
                        city: City): Address {

        return Address(
                addressData["address"].toString(),
                cityLocData["zipcode"].toString(),
                city
        )
    }

    private fun city(cityData: Map<String, Any>, country: Country): City {
        return City(
                cityData["name"].toString(),
                country
        )
    }

    private fun country(countryData: Map<String, Any>): Country {
        return Country(
                countryData["code"].toString(),
                countryData["name"].toString()
        )
    }

    private fun asNullableMap(value: Value): Map<String, Any>? {
        return if (value.isNull) null else value.asMap()
    }
}

