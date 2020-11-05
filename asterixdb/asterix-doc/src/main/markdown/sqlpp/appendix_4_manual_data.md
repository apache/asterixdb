<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

This appendix lists the data definitions and the datasets used for the examples provided throughout this manual. 

### <a id="definition_statements">Data Definitions</a>

	CREATE DATAVERSE Commerce IF NOT EXISTS;
	
	USE Commerce;
	
	CREATE TYPE addressType AS {
	    street:			string,
	    city:			string,
	    zipcode:			string?
	};
	
	CREATE TYPE customerType AS {
        custid:			string,
        name:			string,
	    address:			addressType?
    };
	
	CREATE DATASET customers(customerType)
	    PRIMARY KEY custid;
    
	CREATE TYPE itemType AS {
	    itemno:			int,
	    qty:			int,
	    price:			int
	};

	CREATE TYPE orderType AS {
	    orderno:			int,
	    custid:			string,
	    order_date:			string,
	    ship_date:			string?,
	    items:			[ itemType ]
	};

	CREATE DATASET orders(orderType)
	    PRIMARY KEY orderno;
### <a id="customers_data">Customers Data</a>

	[
	    {
	        "custid": "C13",
	        "name": "T. Cody",
	        "address": {
	            "street": "201 Main St.",
	            "city": "St. Louis, MO",
	            "zipcode": "63101"
	        },
	        "rating": 750
	    },
	    {
	        "custid": "C25",
	        "name": "M. Sinclair",
	        "address": {
	            "street": "690 River St.",
	            "city": "Hanover, MA",
	            "zipcode": "02340"
	        },
	        "rating": 690
	    },
	    {
	        "custid": "C31",
	        "name": "B. Pruitt",
	        "address": {
	            "street": "360 Mountain Ave.",
	            "city": "St. Louis, MO",
	            "zipcode": "63101"
	        }
	    },
	    {
	        "custid": "C35",
	        "name": "J. Roberts",
	        "address": {
	            "street": "420 Green St.",
	            "city": "Boston, MA",
	            "zipcode": "02115"
	        },
	        "rating": 565
	    },
	    {
	        "custid": "C37",
	        "name": "T. Henry",
	        "address": {
	            "street": "120 Harbor Blvd.",
	            "city": "Boston, MA",
	            "zipcode": "02115"
	        },
	        "rating": 750
	    },
	    {
	        "custid": "C41",
	        "name": "R. Dodge",
	        "address": {
	            "street": "150 Market St.",
	            "city": "St. Louis, MO",
	            "zipcode": "63101"
	        },
	        "rating": 640
	    },
	    {
	        "custid": "C47",
	        "name": "S. Logan",
	        "address": {
	            "street": "Via del Corso",
	            "city": "Rome, Italy"
	        },
	        "rating": 625
	    }
	]


### <a id="orders_data">Orders Data</a>

	[
	    {
	        "orderno": 1001,
	        "custid": "C41",
	        "order_date": "2020-04-29",
	        "ship_date": "2020-05-03",
	        "items": [
	            {
	                "itemno": 347,
	                "qty": 5,
	                "price": 19.99
	            },
	            {
	                "itemno": 193,
	                "qty": 2,
	                "price": 28.89
	            }
	        ]
	    },
	    {
	        "orderno": 1002,
	        "custid": "C13",
	        "order_date": "2020-05-01",
	        "ship_date": "2020-05-03",
	        "items": [
	            {
	                "itemno": 460,
	                "qty": 95,
	                "price": 100.99
	            },
	            {
	                "itemno": 680,
	                "qty": 150,
	                "price": 8.75
	            }
	        ]
	    },
	    {
	        "orderno": 1003,
	        "custid": "C31",
	        "order_date": "2020-06-15",
	        "ship_date": "2020-06-16",
	        "items": [
	            {
	                "itemno": 120,
	                "qty": 2,
	                "price": 88.99
	            },
	            {
	                "itemno": 460,
	                "qty": 3,
	                "price": 99.99
	            }
	        ]
	    },
	    {
	        "orderno": 1004,
	        "custid": "C35",
	        "order_date": "2020-07-10",
	        "ship_date": "2020-07-15",
	        "items": [
	            {
	                "itemno": 680,
	                "qty": 6,
	                "price": 9.99
	            },
	            {
	                "itemno": 195,
	                "qty": 4,
	                "price": 35
	            }
	        ]
	    },
	    {
	        "orderno": 1005,
	        "custid": "C37",
	        "order_date": "2020-08-30",
	        "items": [
	            {
	                "itemno": 460,
	                "qty": 2,
	                "price": 99.98
	            },
	            {
	                "itemno": 347,
	                "qty": 120,
	                "price": 22
	            },
	            {
	                "itemno": 780,
	                "qty": 1,
	                "price": 1500
	            },
	            {
	                "itemno": 375,
	                "qty": 2,
	                "price": 149.98
	            }
	        ]
	    },
	    {
	        "orderno": 1006,
	        "custid": "C41",
	        "order_date": "2020-09-02",
	        "ship_date": "2020-09-04",
	        "items": [
	            {
	                "itemno": 680,
	                "qty": 51,
	                "price": 25.98
	            },
	            {
	                "itemno": 120,
	                "qty": 65,
	                "price": 85
	            },
	            {
	                "itemno": 460,
	                "qty": 120,
	                "price": 99.98
	            }
	        ]
	    },
	    {
	        "orderno": 1007,
	        "custid": "C13",
	        "order_date": "2020-09-13",
	        "ship_date": "2020-09-20",
	        "items": [
	            {
	                "itemno": 185,
	                "qty": 5,
	                "price": 21.99
	            },
	            {
	                "itemno": 680,
	                "qty": 1,
	                "price": 20.5
	            }
	        ]
	    },
	    {
	        "orderno": 1008,
	        "custid": "C13",
	        "order_date": "2020-10-13",
	        "items": [
	            {
	                "itemno": 460,
	                "qty": 20,
	                "price": 99.99
	            }
	        ]
	    },
	    {
	        "orderno": 1009,
	        "custid": "C13",
	        "order_date": "2020-10-13",
	        "items": []
	    }
	]
