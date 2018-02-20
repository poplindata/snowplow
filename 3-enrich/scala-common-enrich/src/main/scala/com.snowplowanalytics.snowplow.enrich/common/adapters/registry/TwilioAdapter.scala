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

/**
 * Transforms a collector payload which conforms to
 * a known version of the UrbanAirship Connect API
 * into raw events.
 */
object TwilioAdapter extends Adapter {

  // Vendor name for Failure Message
  private val VendorName = "Twilio"

  // Tracker version for an UrbanAirship Connect API
  private val TrackerVersion = "com.twilio-v1"

    // Expected content type for a request body
  private val ContentType = "application/json"

  // Schemas for reverse-engineering a Snowplow unstructured event
  private val EventSchemaMap = Map (
    "delivered" -> SchemaKey("com.twilio", "sms_delivered", "jsonschema", "1-0-0").toSchemaUri,
    "queued" -> SchemaKey("com.twilio", "sms_queued", "jsonschema", "1-0-0").toSchemaUri,
    "sent" -> SchemaKey("com.twilio", "sms_sent", "jsonschema", "1-0-0").toSchemaUri
  )

  /**
   * Converts payload into a single validated event
   * Expects a valid json, returns failure if one is not present
   *
   * @param body_json json payload as a string
   * @param payload other payload details
   * @return a validated event - a success will contain the corresponding RawEvent, failures will
   *         contain a reason for failure
   */
  private def payloadBodyToEvent(body_json: String, payload: CollectorPayload): Validated[RawEvent] = {

    try {

      val parsed = parse(body_json)
      val eventType = (parsed \ "MessageStatus").extractOpt[String]
      val clean = cleanJson(cleanJson(parsed, "MessageStatus"), "SmsStatus") 
      println("eventType is " + eventType) 
      lookupSchema(eventType, VendorName, EventSchemaMap) map {
        schema => RawEvent(
          api = payload.api,
          parameters = toUnstructEventParams(
            TrackerVersion,
            toMap(payload.querystring),
            schema,
            clean,
            "srv"
          ),
          contentType = payload.contentType,
          source = payload.source,
          //context = payload.context.copy(timestamp = Some(new DateTime(collectorTimestamp.get, DateTimeZone.UTC)))
          context = payload.context 
        )
      }

    } catch {
      case e: JsonParseException => {
        val exception = JU.stripInstanceEtc(e.toString).orNull
        s"$VendorName event failed to parse into JSON: [$exception]".failNel
      }
    }

  }

  /**
   * Converts a CollectorPayload instance into raw events.
   * A UrbanAirship connect API payload only contains a single event.
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

  def cleanJson(json: JValue, jsonfield: String): JValue =
    json removeField {
      case JField(`jsonfield`, _) => true 
      case _ => false 
    }


}
