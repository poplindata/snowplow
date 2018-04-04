/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics
package snowplow
package enrich
package common
package adapters
package registry

// Java
import com.fasterxml.jackson.core.JsonParseException

// Scalaz
import scalaz.Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// Iglu
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}

// Joda Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

// This project
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.utils.{JsonUtils => JU}
import scala.util.{Failure, Success, Try}

/**
 * Transforms a collector payload which conforms to
 * a known version of the Twilio REST API payload
 * into raw events.
 */
object TwilioAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Twilio"

  // Tracker version for an Twilio API
  private val TrackerVersion = "com.twilio-v1"

    // Expected content type for a request body
  private val ContentType = "application/json"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map (
    "delivered"   -> SchemaKey("com.twilio", "delivered", "jsonschema", "1-0-0").toSchemaUri,
    "queued"      -> SchemaKey("com.twilio", "queued", "jsonschema", "1-0-0").toSchemaUri,
    "sent"        -> SchemaKey("com.twilio", "sent", "jsonschema", "1-0-0").toSchemaUri,
    "undelivered" -> SchemaKey("com.twilio", "undelivered", "jsonschema", "1-0-0").toSchemaUri,
    "failed"      -> SchemaKey("com.twilio", "failed", "jsonschema", "1-0-0").toSchemaUri
  )

  // Datetime format used by Twilio (RFC2822 format)
  private val TwilioDateTimeFormat = DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss Z").withZone(DateTimeZone.UTC)

  /**
   * Converts payload into a single validated event
   * Expects a valid json, returns failure if one is not present
   *
   * @param json json payload as a string
   * @param payload other payload details
   * @return a validated event - a success will contain the corresponding RawEvent, failures will
   *         contain a reason for failure
   */
  private def payloadBodyToEvent(json: String, payload: CollectorPayload): Validated[RawEvent] = {

    val parsed = Try(parse(json))
      parsed match {
        case Success(parsed) => parsed
        case Failure(e)      => s"$VendorName event failed to parse into JSON: [${e.getMessage}]".failureNel
      }
    val eventType = parsed.toOption.flatMap(p => (p \ "MessageStatus").extractOpt[String]).getOrElse("failed")
    val cleaned = parsed.map(p => cleanJson(p))

    lookupSchema(Some(eventType), VendorName, EventSchemaMap) map {
      schema => RawEvent(
        api = payload.api,
        parameters = toUnstructEventParams(
          TrackerVersion,
          toMap(payload.querystring),
          schema,
          cleaned,
          "srv"
        ),
        contentType = payload.contentType,
        source = payload.source,
        context = payload.context 
      )
    }

  }

  /**
   * Converts a CollectorPayload instance into raw events.
   * A Twilio REST API payload only contains a single event.
   * We expect the name parameter to match the supported events, else
   * we have an unsupported event type.
   *
   * @param payload The CollectorPayload containing one or more
   *        raw events as collected by a Snowplow collector
   * @param resolver (implicit) The Iglu resolver used for
   *        schema lookup and validation. Not used
   * @return a Validation boxing either a NEL of RawEvents on
   *         Success, or a NEL of Failure Strings
   */
  def toRawEvents(payload: CollectorPayload)(implicit resolver: Resolver): ValidatedRawEvents =
    (payload.body, payload.contentType) match {
      case (None, _) => s"Request body is empty: no ${VendorName} event to process".failNel
      case (Some(body), _) => {
        val event = payloadBodyToEvent(body, payload)
        rawEventsListProcessor(List(event))
      }
    }

  /**
   * Function that removes selected fields
   * and converts the RFC2822 date format to a valid date-time format
   *
   * @param json The JSON payload that'll be parsed
   * @return a JValue of the formatted payload
   */
  def cleanJson(json: JValue): JValue = 
    json.removeField {
      case JField("SmsStatus", _)     => true 
      case JField("MessageStatus", _) => true 
      case JField("Status", _)        => true 
      case _                          => false 
    }.transformField {
      case ("DateCreated", JString(value)) => ("DateCreated", JString(JU.toJsonSchemaDateTime(value, TwilioDateTimeFormat)))
      case ("DateUpdated", JString(value)) => ("DateUpdated", JString(JU.toJsonSchemaDateTime(value, TwilioDateTimeFormat)))
      case ("DateSent", JString(value))    => ("DateSent", JString(JU.toJsonSchemaDateTime(value, TwilioDateTimeFormat)))
    }
}
