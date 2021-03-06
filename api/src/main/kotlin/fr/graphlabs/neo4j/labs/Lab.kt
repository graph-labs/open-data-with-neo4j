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

data class Lab(val identifier: String, val name: String, val segment: BusinessSegment? = null, val address: Address? = null)
data class BusinessSegment(val code: String, val label: String)

data class Address(val address: String, val zipCode: String, val city: City)
data class City(val name: String, val country: Country)
data class Country(val code: String, val name: String)

