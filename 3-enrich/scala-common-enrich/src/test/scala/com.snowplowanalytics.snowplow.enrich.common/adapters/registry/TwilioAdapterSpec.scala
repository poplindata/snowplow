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
            |"Sid":"xxxxxxxxxxxx",
            |"DateCreated":"Mon, 07 Aug 2017 01:38:24 +0000",
            |"DateUpdated":"Mon, 07 Aug 2017 01:38:24 +0000",
            |"DateSent":"Mon, 07 Aug 2017 01:38:24 +0000",
            |"AccountSid":"ACa192d23e5e9662f4e229a11885fd907b",
            |"To":"+xxxxxxxxx",
            |"From":"+xxxxxxxx",
            |"MessagingServiceSid":"",
            |"Body":"hello",
            |"Status":"failed",
            |"NumSegments":"1",
            |"NumMedia":"1",
            |"Direction":"outbound-api",
            |"ApiVersion":"2010-04-01",
            |"Price":42.33,
            |"PriceUnit":"USD",
            |"ErrorCode":21621,
            |"ErrorMessage":null,
            |"Uri":"/2010-04-01/Accounts/ACa192d23e5e9662f4e229a11885fd907b/Messages/MMxxxxxxxx.json",
            |"SubresourceUris":{
                |"Media":"/2010-04-01/Accounts/ACa192d23e5e9662f4e229a11885fd907b/Messages/MMxxxxxxxx/Media.json"
            |}
          |}""".stripMargin.replaceAll("[\n\r]", "")

    val outputJson =     
       """|{
            |"Sid":"xxxxxxxxxxxx",
            |"DateCreated":"2017-08-07T01:38:24.000Z",
            |"DateUpdated":"2017-08-07T01:38:24.000Z",
            |"DateSent":"2017-08-07T01:38:24.000Z",
            |"AccountSid":"ACa192d23e5e9662f4e229a11885fd907b",
            |"To":"+xxxxxxxxx",
            |"From":"+xxxxxxxx",
            |"MessagingServiceSid":"",
            |"Body":"hello",
            |"NumSegments":"1",
            |"NumMedia":"1",
            |"Direction":"outbound-api",
            |"ApiVersion":"2010-04-01",
            |"Price":42.33,
            |"PriceUnit":"USD",
            |"ErrorCode":21621,
            |"ErrorMessage":null,
            |"Uri":"/2010-04-01/Accounts/ACa192d23e5e9662f4e229a11885fd907b/Messages/MMxxxxxxxx.json",
            |"SubresourceUris":{
                |"Media":"/2010-04-01/Accounts/ACa192d23e5e9662f4e229a11885fd907b/Messages/MMxxxxxxxx/Media.json"
            |}
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
                                        |"schema":"iglu:com.twilio/failed/jsonschema/1-0-0",
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
    "SPEC NAME"                 || "SCHEMA TYPE"    | "EXPECTED SCHEMA"                                  |
    "Valid, type delivered"     !! "delivered"      ! "iglu:com.twilio/delivered/jsonschema/1-0-0"       |
    "Valid, type sent"          !! "sent"           ! "iglu:com.twilio/sent/jsonschema/1-0-0"            |
    "Valid, type queued"        !! "queued"         ! "iglu:com.twilio/queued/jsonschema/1-0-0"          |
    "Valid, type undelivered"   !! "undelivered"    ! "iglu:com.twilio/undelivered/jsonschema/1-0-0"     |
    "Valid, type failed"        !! "failed"         ! "iglu:com.twilio/failed/jsonschema/1-0-0"          |> {
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
