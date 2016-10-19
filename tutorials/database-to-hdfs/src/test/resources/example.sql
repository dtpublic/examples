/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

DROP DATABASE IF EXISTS testDev;

CREATE DATABASE testDev;

USE testDev;

CREATE TABLE IF NOT EXISTS `test_event_table` (
  `ACCOUNT_NO` int(11) NOT NULL,
  `NAME` varchar(255) DEFAULT NULL,
  `AMOUNT` int(11) DEFAULT NULL,
  primary key(`ACCOUNT_NO`)
) ENGINE=MyISAM  DEFAULT CHARSET=latin1;

INSERT INTO `test_event_table` (`ACCOUNT_NO`, `NAME`, `AMOUNT`) VALUES
(1, 'User1', 1000),
(2, 'User2', 2000),
(3, 'User3', 3000),
(4, 'User4', 4000),
(5, 'User5', 5000),
(6, 'User6', 6000),
(7, 'User7', 7000),
(8, 'User8', 8000),
(9, 'User9', 9000),
(10, 'User10', 1000);
