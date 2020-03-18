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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;
/**
 * @author Sridhar Paladugu
 * @version 1.0
 */
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Account implements Serializable {
	private static final long serialVersionUID = 2741400974480282819L;

	@JsonProperty("city")
	private String city;
	@JsonProperty("long")
	private Double longitude;
	@JsonProperty("lat")
	private Double latitude;
	@JsonProperty("account_id")
	private Integer accountId;
	@JsonProperty("cvv")
	private String cvv;
	@JsonProperty("expiration_date")
	private String expirationDate;
	@JsonProperty("trxn_std")
	private Double trxStd;
	@JsonProperty("trxn_mean")
	private Double trxnMean;
	@JsonProperty("city_alias")
	private String cityAlias;
	@JsonProperty("card_type")
	private String cardType;
	@JsonProperty("state")
	private String state;
	@JsonProperty("transaction_radius")
	private int transactionRadius;
	@JsonProperty("account_number")
	private String accountNumber;



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Account other = (Account) obj;
		if (accountNumber == null) {
			if (other.accountNumber != null)
				return false;
		} else if (!accountNumber.equals(other.accountNumber))
			return false;
		return true;
	}


}

