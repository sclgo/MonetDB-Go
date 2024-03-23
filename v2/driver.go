/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"database/sql/driver"
)

const DriverVersion = "2.0.0"

func init() {
	sql.Register("monetdbscl", &Driver{})
}

type Driver struct {
}

func (*Driver) Open(name string) (driver.Conn, error) {
	return newConn(name)
}

