/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
)

func init() {
	sql.Register("monetdb", &Driver{})
}

type Driver struct {
}

type config struct {
	Username string
	Password string
	Hostname string
	Database string
	Port     int
}

func (*Driver) Open(name string) (driver.Conn, error) {
	c, err := parseDSN(name)
	if err != nil {
		return nil, err
	}

	return newConn(c)
}

func parseDSN(name string) (config, error) {
	re := regexp.MustCompile(`^((?P<username>[^:]+?)(:(?P<password>[^@]+?))?@)?(?P<hostname>[a-zA-Z0-9.]+?)(:(?P<port>\d+?))?/(?P<database>.+?)$`)
	ipv6_re := regexp.MustCompile(`^((?P<username>[^:]+?)(:(?P<password>[^@]+?))?@)?\[(?P<hostname>(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))+?)\](:(?P<port>\d+?))?\/(?P<database>.+?)$`)
	m := make([]string, 0)
	n := make([]string, 0)

	if re.MatchString(name) {
		m = re.FindAllStringSubmatch(name, -1)[0]
		n = re.SubexpNames()
		return getConfig(m, n, false), nil
	} else if ipv6_re.MatchString(name) {
		m = ipv6_re.FindAllStringSubmatch(name, -1)[0]
		n = ipv6_re.SubexpNames()
		return getConfig(m, n, true), nil
	}

	return config{}, fmt.Errorf("Invalid DSN")
}

func getConfig(m []string, n []string, ipv6 bool) config {
	c := config{
		Hostname: "localhost",
		Port:     50000,
	}
	for i, v := range m {
		if n[i] == "username" {
			c.Username = v
		} else if n[i] == "password" {
			c.Password = v
		} else if n[i] == "hostname" {
			if ipv6 {
				c.Hostname = fmt.Sprintf("[%s]", v)
				continue
			}

			c.Hostname = v
		} else if n[i] == "port" && v != "" {
			c.Port, _ = strconv.Atoi(v)
		} else if n[i] == "database" {
			c.Database = v
		}
	}

	return c
}
