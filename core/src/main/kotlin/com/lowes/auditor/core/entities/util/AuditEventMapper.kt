package com.lowes.auditor.core.entities.util

import com.lowes.auditor.core.entities.domain.AuditEvent
import com.lowes.auditor.core.entities.domain.Element
import com.lowes.auditor.core.entities.domain.ElementMetadata
import com.lowes.auditor.core.entities.domain.EventSource
import com.lowes.auditor.core.entities.domain.EventSourceMetadata
import com.lowes.auditor.core.entities.domain.EventSourceType
import com.lowes.auditor.core.entities.domain.EventType
import com.lowes.auditor.core.infrastructure.event.model.AuditEventDTO
import com.lowes.auditor.core.infrastructure.event.model.ElementDTO
import com.lowes.auditor.core.infrastructure.event.model.ElementMetadataDTO
import com.lowes.auditor.core.infrastructure.event.model.EventSourceMetadataDTO

object AuditEventMapper {

    fun toAuditEvent(auditEventDTO: AuditEventDTO): AuditEvent {
        return AuditEvent(
            id = auditEventDTO.id,
            applicationName = auditEventDTO.applicationName,
            timestamp = auditEventDTO.timestamp,
            type = EventType.valueOf(auditEventDTO.type.value),
            source = EventSource(
                type = auditEventDTO.source.type.value.let { EventSourceType.valueOf(it) },
                metadata = toSourceMetadata(auditEventDTO.source.metadata)
            ),
            elements = toElement(auditEventDTO.elements),
            subType = auditEventDTO.subType,
            metadata = auditEventDTO.metadata
        )
    }

    private fun toElement(elementsDTO: List<ElementDTO>): List<Element> {
        return elementsDTO.map {
            Element(
                name = it.name,
                previousValue = it.previousValue,
                updatedValue = it.updatedValue,
                metadata = toElementMetadata(it.metadata)
            )
        }
    }

    private fun toElementMetadata(elementMetadataDTO: ElementMetadataDTO?): ElementMetadata {
        return ElementMetadata(
            fqdn = elementMetadataDTO?.fqdn,
            identifiers = elementMetadataDTO?.identifiers
        )
    }

    private fun toSourceMetadata(sourceMetadataDTO: EventSourceMetadataDTO?): EventSourceMetadata {
        return EventSourceMetadata(
            id = sourceMetadataDTO?.id,
            email = sourceMetadataDTO?.email,
            name = sourceMetadataDTO?.name
        )
    }
}
