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

namespace O2System\O2DB\Drivers\Odbc;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Result as ResultInterface;

/**
 * ODBC (Unified) Database Result
 *
 * @author      Circle Creative Developer Team
 */
class Result extends ResultInterface
{
    /**
     * Number of rows in the result set
     *
     * @access  public
     * @return  int
     */
    public function num_rows()
    {
        if( is_int( $this->num_rows ) )
        {
            return $this->num_rows;
        }
        elseif( ( $this->num_rows = odbc_num_rows( $this->id_result ) ) !== -1 )
        {
            return $this->num_rows;
        }

        // Work-around for ODBC subdrivers that don't support num_rows()
        if( count( $this->result_array ) > 0 )
        {
            return $this->num_rows = count( $this->result_array );
        }
        elseif( count( $this->result_object ) > 0 )
        {
            return $this->num_rows = count( $this->result_object );
        }

        return $this->num_rows = count( $this->result_array() );
    }

    // --------------------------------------------------------------------

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
        $num_fields = $this->num_fields();

        if( $num_fields > 0 )
        {
            for( $i = 1; $i <= $num_fields; $i++ )
            {
                $field_names[ ] = odbc_field_name( $this->id_result, $i );
            }
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
        return odbc_num_fields( $this->id_result );
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
        for( $i = 0, $odbc_index = 1, $c = $this->num_fields(); $i < $c; $i++, $odbc_index++ )
        {
            $data[ $i ] = new \stdClass();
            $data[ $i ]->name = odbc_field_name( $this->id_result, $odbc_index );
            $data[ $i ]->type = odbc_field_type( $this->id_result, $odbc_index );
            $data[ $i ]->max_length = odbc_field_len( $this->id_result, $odbc_index );
            $data[ $i ]->primary_key = 0;
            $data[ $i ]->default = '';
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
        if( is_resource( $this->id_result ) )
        {
            odbc_free_result( $this->id_result );
            $this->id_result = FALSE;
        }
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
        return odbc_fetch_array( $this->id_result );
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
        $row = odbc_fetch_object( $this->id_result );

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

// --------------------------------------------------------------------

if( ! function_exists( 'odbc_fetch_array' ) )
{
    /**
     * ODBC Fetch array
     *
     * Emulates the native odbc_fetch_array() function when
     * it is not available (odbc_fetch_array() requires unixODBC)
     *
     * @param    resource &$result
     * @param    int      $row_number
     *
     * @return    array
     */
    function odbc_fetch_array( &$result, $row_number = 1 )
    {
        $rs = array();
        if( ! odbc_fetch_into( $result, $rs, $row_number ) )
        {
            return FALSE;
        }

        $rs_assoc = array();
        foreach( $rs as $k => $v )
        {
            $field_name = odbc_field_name( $result, $k + 1 );
            $rs_assoc[ $field_name ] = $v;
        }

        return $rs_assoc;
    }
}

// --------------------------------------------------------------------

if( ! function_exists( 'odbc_fetch_object' ) )
{
    /**
     * ODBC Fetch object
     *
     * Emulates the native odbc_fetch_object() function when
     * it is not available.
     *
     * @param    resource &$result
     * @param    int      $row_number
     *
     * @return    object
     */
    function odbc_fetch_object( &$result, $row_number = 1 )
    {
        $rs = array();
        if( ! odbc_fetch_into( $result, $rs, $row_number ) )
        {
            return FALSE;
        }

        $rs_object = new \stdClass();
        foreach( $rs as $k => $v )
        {
            $field_name = odbc_field_name( $result, $k + 1 );
            $rs_object->$field_name = $v;
        }

        return $rs_object;
    }
}