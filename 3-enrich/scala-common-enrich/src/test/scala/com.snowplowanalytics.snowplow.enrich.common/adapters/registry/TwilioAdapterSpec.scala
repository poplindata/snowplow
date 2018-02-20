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
package com.snowplowanalytics.snowplow.enrich.common
package adapters
package registry

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.json4s.scalaz.JsonScalaz._

// Snowplow
import loaders.{
  CollectorApi,
  CollectorSource,
  CollectorContext,
  CollectorPayload
}
import utils.ConversionUtils
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers


class TwilioAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck {  def is = s2"""
  This is a specification to test the VeroAdapter functionality
  toRawEvents must return a success for a valid for all event types payload body being passed                 $e1
  test event types are mapped to the correct schema names                                                     $e2
  toRawEvents must return a Failure Nel if a body is not specified in the payload                             $e3
  """

  implicit val resolver = SpecHelpers.IgluResolver
  //implicit val formats = DefaultFormats

  object Shared {
    val api = CollectorApi("com.twilio", "v1")
    val cljSource = CollectorSource("clj-tomcat", "UTF-8", None)
    val context = CollectorContext(DateTime.parse("2018-01-01T00:00:00.000+00:00").some, "37.157.33.123".some, None, None, Nil, None)  // NB the collector timestamp is set to None!
  }

  val ContentType = "application/json"

  def e1 = {

    val inputJson =     
       """|{
            |"To":"to_field",
            |"ApiVersion":"apiv",
            |"MessageSid":"messagesid_field",
            |"AccountSid":"accountsid_field",
            |"SmsSid":"smsid_field",
            |"SmsStatus":"delivered",
            |"MessageStatus":"delivered",
            |"From":"from_field",
            |"MessagingServiceSid":"msserviceid_field"
           |}""".stripMargin.replaceAll("[\n\r]", "")

    val outputJson =     
       """|{
            |"To":"to_field",
            |"ApiVersion":"apiv",
            |"MessageSid":"messagesid_field",
            |"AccountSid":"accountsid_field",
            |"SmsSid":"smsid_field",
            |"From":"from_field",
            |"MessagingServiceSid":"msserviceid_field"
           |}""".stripMargin.replaceAll("[\n\r]", "")

    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, inputJson.some,  Shared.cljSource, Shared.context) 

    val expected = NonEmptyList(
        RawEvent(
                 Shared.api,
                 Map(
                    "tv" -> "com.twilio-v1",
                    "e" -> "ue",
                    "p" -> "srv",
                    "ue_pr" -> """|{
                                    |"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
                                    |"data":
                                      |{
                                        |"schema":"iglu:com.twilio/sms_delivered/jsonschema/1-0-0",
                                        |"data":%s
                                      |}
                                  |}""".format(outputJson)
                                       .stripMargin
                                       .replaceAll("[\n\r]", "")),
                 ContentType.some, 
                 Shared.cljSource,
                 Shared.context
                 )
    )
    TwilioAdapter.toRawEvents(payload) must beSuccessful(expected) 
  }

  def e2 =
    "SPEC NAME"                 || "SCHEMA TYPE"    | "EXPECTED SCHEMA"                                      |
    "Valid, type sms_delivered" !! "delivered"      ! "iglu:com.twilio/sms_delivered/jsonschema/1-0-0"       |
    "Valid, type sms_sent"      !! "sent"           ! "iglu:com.twilio/sms_sent/jsonschema/1-0-0"            |
    "Valid, type sms_queued"    !! "queued"         ! "iglu:com.twilio/sms_queued/jsonschema/1-0-0"       |> {
      (_, schema, expected) =>
        println("TEST: " + schema + " " + expected)
        val body = "{\"MessageStatus\":\"" + schema + "\"}"
        val payload = CollectorPayload(Shared.api, Nil, ContentType.some, body.some, Shared.cljSource, Shared.context)
        val expectedJson = "{\"schema\":\"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0\",\"data\":{\"schema\":\"" + expected + "\",\"data\":{}}}"
        val actual = TwilioAdapter.toRawEvents(payload)
        actual must beSuccessful(NonEmptyList(RawEvent(Shared.api, Map("tv" -> "com.twilio-v1", "e" -> "ue", "p" -> "srv", "ue_pr" -> expectedJson), ContentType.some, Shared.cljSource, Shared.context)))
  }

  def e3 = {  
    val payload = CollectorPayload(Shared.api, Nil, ContentType.some, None, Shared.cljSource, Shared.context)
    TwilioAdapter.toRawEvents(payload) must beFailing(NonEmptyList("Request body is empty: no Twilio event to process"))
  }

}
