package com.lowes.auditor.client.infrastructure.frameworks.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.lowes.auditor.client.entities.domain.AuditorEventConfig
import com.lowes.auditor.client.entities.interfaces.infrastructure.frameworks.ObjectDiffChecker
import com.lowes.auditor.client.entities.util.ONE_THOUSAND
import com.lowes.auditor.client.entities.util.TWO
import com.lowes.auditor.client.entities.util.orDefault
import com.lowes.auditor.client.infrastructure.frameworks.mapper.JsonNodeMapper
import com.lowes.auditor.core.entities.domain.Element
import com.lowes.auditor.core.entities.domain.EventType
import com.lowes.auditor.core.entities.domain.EventType.CREATED
import com.lowes.auditor.core.entities.domain.EventType.DELETED
import reactor.core.publisher.Flux

/**
 * Provides implementation definitions for [ObjectDiffChecker]
 * @property objectMapper instance of [ObjectMapper]
 * @property auditorEventConfig instance of [AuditorEventConfig]
 * @see ObjectDiffChecker
 */
class ObjectDiffCheckerService(
    private val objectMapper: ObjectMapper,
    private val auditorEventConfig: AuditorEventConfig
) : ObjectDiffChecker {

    private val prefetch = auditorEventConfig.maxElements?.times(TWO).orDefault(ONE_THOUSAND)
    private val ignoreCollectionOrder = auditorEventConfig.ignoreCollectionOrder?.enabled.orDefault(false)
    private val altIdentifierFields = auditorEventConfig.ignoreCollectionOrder?.fields.orDefault(listOf("id"))

    /**
     * provides diff between two objects
     * @see ObjectDiffChecker.diff
     */
    override fun diff(objectOne: Any?, objectTwo: Any?): Flux<Element> {
        return when {
            objectOne == null && objectTwo != null -> getElementsWhenSingleObjectExists(objectTwo, CREATED)
            objectOne != null && objectTwo == null -> getElementsWhenSingleObjectExists(objectOne, DELETED)
            objectOne != null && objectTwo != null -> getElementsWhenBothObjectExists(objectOne, objectTwo)
            else -> Flux.empty()
        }
    }

    /**
     * Converts the difference between two object's properties into flux of [Element]
     */
    private fun getElementsWhenBothObjectExists(objectOne: Any, objectTwo: Any): Flux<Element> {
        val objectOneElements = getElementsWhenSingleObjectExists(objectOne, DELETED)
        val objectTwoElements = getElementsWhenSingleObjectExists(objectTwo, CREATED)
        return Flux.merge(objectOneElements, objectTwoElements)
            .groupBy({ it.metadata?.fqdn ?: "missingMetaData" }, prefetch)
            .flatMap { it.collectList().flatMapIterable { list -> getChanges(list) } }
            .filter { it.name != null }
    }

    /**
     * Converts a single object properties into flux of [Element]
     */
    private fun getElementsWhenSingleObjectExists(singleObject: Any, eventType: EventType, fqcn: String? = null): Flux<Element> {
        val node = objectMapper.valueToTree<JsonNode>(singleObject)
        return JsonNodeMapper.toElement(node, eventType, fqcn ?: singleObject.javaClass.canonicalName, ignoreCollectionOrder, altIdentifierFields)
    }

    /**
     * Checks list of elements with same fqdn for changes
     */
    private fun getChanges(elements: List<Element>): List<Element> {
        return when (elements.size) {
            // typical create/delete
            1 -> elements
            // typical update, not sure if order will be consistent so check both ways
            2 -> {
                if (elements[0].previousValue != null && elements[0].previousValue != elements[1].updatedValue)
                    listOf(elements[0].copy(updatedValue = elements[1].updatedValue))
                else if (elements[1].previousValue != null && elements[1].previousValue != elements[0].updatedValue)
                    listOf(elements[1].copy(updatedValue = elements[0].updatedValue))
                else listOf()
            }
            // will only be reached if ignoring order is enabled and a collection contains duplicates
            else -> diffDuplicates(elements)
        }
    }

    private fun diffDuplicates(elements: List<Element>): List<Element> {
        val changes = mutableListOf<Element>()
        // sort
        val previous = mutableListOf<Element>()
        val updated = mutableListOf<Element>()
        elements.forEach { element ->
            if (element.previousValue != null) previous.add(element)
            else if (element.updatedValue != null) updated.add(element)
        }
        // group by value
        val prevGrouped = previous.groupBy { it.previousValue }
        val updGrouped = updated.groupBy { it.updatedValue }
        // for each value, diff by number of prev/upd occurrences
        val prevDiff = mutableListOf<Element>()
        val updDiff = mutableListOf<Element>()
        (prevGrouped.keys + updGrouped.keys).distinct().forEach { value ->
            val prevSize = prevGrouped[value]?.size ?: 0
            val updSize = updGrouped[value]?.size ?: 0
            when {
                prevSize > updSize -> prevGrouped[value]?.let { prevDiff.addAll(it.drop(updSize)) }
                updSize > prevSize -> updGrouped[value]?.let { updDiff.addAll(it.drop(prevSize)) }
            }
        }
        // consolidate elements where possible
        if (prevDiff.size >= updDiff.size) {
            for (n in 0 until updDiff.size) changes.add(prevDiff[n].copy(updatedValue = updDiff[n].updatedValue))
            changes.addAll(prevDiff.drop(updDiff.size))
        } else {
            for (n in 0 until prevDiff.size) changes.add(updDiff[n].copy(previousValue = prevDiff[n].previousValue))
            changes.addAll(updDiff.drop(prevDiff.size))
        }
        return changes
    }
}
