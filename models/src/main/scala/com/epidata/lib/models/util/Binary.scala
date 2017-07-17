/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models.util

import javax.xml.bind.DatatypeConverter

/*
 * Class representing binary data for storage as a cassandra blob. The data
 * is in epidata format comprising a single byte representing datatype followed
 * by the binary payload.
 *
 * TODO Improve backing for better array slicing support
 * TODO Avoid unnecessary copying during base64 conversion.
 */
class Binary(val backing: Array[Byte])

object Binary {

  def fromBase64(datatype: Datatype.Value, base64String: String): Binary =
    new Binary(Array(datatype.id.toByte) ++ DatatypeConverter.parseBase64Binary(base64String))

  def toBase64(binary: Binary): (Datatype.Value, String) =
    (
      Datatype.byId(binary.backing(0)),
      DatatypeConverter.printBase64Binary(binary.backing.slice(1, binary.backing.length))
    )
}
