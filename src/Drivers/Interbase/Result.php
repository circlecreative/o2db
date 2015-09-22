<?php
/**
 * O2DB
 *
 * An open source PHP database engine driver for PHP 5.4 or newer
 *
 * This content is released under the MIT License (MIT)
 *
 * Copyright (c) 2014, PT. Lingkar Kreasi (Circle Creative).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @package        O2System
 * @author         Steeven Andrian Salim
 * @copyright      Copyright (c) 2005 - 2014, PT. Lingkar Kreasi (Circle Creative).
 * @license        http://circle-creative.com/products/o2db/license.html
 * @license        http://opensource.org/licenses/MIT   MIT License
 * @link           http://circle-creative.com/products/o2db.html
 * @filesource
 */
// ------------------------------------------------------------------------

namespace O2System\O2DB\Drivers\Interbase;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Result as ResultInterface;

/**
 * Firebird\Interbase Database Result
 *
 * @author      Circle Creative Developer Team
 */
class Result extends ResultInterface
{
    /**
     * Fetch Field Names
     *
     * Generates an array of column names
     *
     * @access  public
     * @return  array
     */
    public function list_fields()
    {
        $field_names = array();
        for( $i = 0, $num_fields = $this->num_fields(); $i < $num_fields; $i++ )
        {
            $info = ibase_field_info( $this->id_result, $i );
            $field_names[ ] = $info[ 'name' ];
        }

        return $field_names;
    }

    // --------------------------------------------------------------------

    /**
     * Number of fields in the result set
     *
     * @access  public
     * @return  int
     */
    public function num_fields()
    {
        return ibase_num_fields( $this->id_result );
    }

    // --------------------------------------------------------------------

    /**
     * Field data
     *
     * Generates an array of objects containing field meta-data
     *
     * @access  public
     * @return  array
     */
    public function field_data()
    {
        $data = array();
        for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
        {
            $info = ibase_field_info( $this->id_result, $i );

            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = $info[ 'name' ];
            $data[ $i ]->type = $info[ 'type' ];
            $data[ $i ]->max_length = $info[ 'length' ];
        }

        return $data;
    }

    // --------------------------------------------------------------------

    /**
     * Free the result
     *
     * @access  public
     * @return  void
     */
    public function free_result()
    {
        ibase_free_result( $this->id_result );
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array
     *
     * @access  protected
     * @return  array
     */
    protected function _fetch_assoc()
    {
        return ibase_fetch_assoc( $this->id_result, IBASE_FETCH_BLOBS );
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object
     *
     * @param   string $class_name
     *
     * @access  protected
     * @return  object
     */
    protected function _fetch_object( $class_name = '\stdClass' )
    {
        $row = ibase_fetch_object( $this->id_result, IBASE_FETCH_BLOBS );

        if( $class_name === '\stdClass' OR ! $row )
        {
            return $row;
        }

        $class_name = new $class_name();
        foreach( $row as $key => $value )
        {
            $class_name->$key = $value;
        }

        return $class_name;
    }

}