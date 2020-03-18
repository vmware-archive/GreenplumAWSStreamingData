/**********************************************************************************************
 Copyright 2020 VMWare Inc
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 *********************************************************************************************/

package io.greenplum.demo.streaming.model;
/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Location
  implements Serializable
{
  private static final long serialVersionUID = -6530323492867456655L;
  @JsonProperty("merchant_long")
  private Double merchantLongitude;
  @JsonProperty("merchant_lat")
  private Double merchantLatitude;
  @JsonProperty("merchant_city")
  private String merchantCity;
  @JsonProperty("merchant_city_alias")
  private String merchantCityAlias;
  @JsonProperty("merchant_name")
  private String merchantName;
  @JsonProperty("merchant_state")
  private String merchantState;
  @JsonProperty("merchant_trxn_std")
  private Double merchantTrxnStd;
  @JsonProperty("merchant_trxn_mean")
  private Double merchantTrxnMean;
  @JsonProperty("rlb_location_key")
  private int rlbLocationKey;
  @JsonProperty("location_id")
  private int locationId;
  @JsonProperty("transaction_id")
  private int transactionId;

  @JsonProperty("mon_close")
  private int mondayClose;
  @JsonProperty("tue_close")
  private int tuesdayClose;
  @JsonProperty("wed_close")
  private int wednesdayClose;
  @JsonProperty( "thu_close")
  private int thursdayClose;
  @JsonProperty("fri_close")
  private int fridayClose;
  @JsonProperty("sat_close")
  private int saturdayClose;
  @JsonProperty("sun_close")
  private int sundayClose;

  @JsonProperty("mon_open")
  private int mondayOpen;
  @JsonProperty("tue_open")
  private int tuesdayOpen;
  @JsonProperty("wed_open")
  private int wednesdayOpen;
  @JsonProperty("thu_open")
  private int thursdayOpen;
  @JsonProperty("fri_open")
  private int fridayOpen;
  @JsonProperty("sat_open")
  private int SaturdayOpen;
  @JsonProperty("sun_open")
  private int sundayOpen;

  //This gets calculated runtime
  @JsonIgnore
  private Double merchantDistance;
 
}

