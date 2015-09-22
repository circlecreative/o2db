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

namespace O2System\O2DB\Drivers\Oracle;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Result as ResultInterface;

/**
 * Oracle Database Result
 *
 * @author      Circle Creative Developer Team
 */
class Result extends ResultInterface
{

    /**
     * Statement ID
     *
     * @type    resource
     */
    public $id_statement;

    /**
     * Cursor ID
     *
     * @type    resource
     */
    public $id_cursor;

    /**
     * Limit used flag
     *
     * @type    bool
     */
    public $limit_used;

    /**
     * Commit mode flag
     *
     * @type    int
     */
    public $commit_mode;

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param   object  &$driver_object
     *
     * @access  public
     */
    public function __construct( &$driver_object )
    {
        parent::__construct( $driver_object );

        $this->id_statement = $driver_object->id_statement;
        $this->id_cursor = $driver_object->id_cursor;
        $this->limit_used = $driver_object->limit_used;
        $this->commit_mode =& $driver_object->commit_mode;
        $driver_object->id_statement = FALSE;
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
        for( $c = 1, $fieldCount = $this->num_fields(); $c <= $fieldCount; $c++ )
        {
            $field_names[ ] = oci_field_name( $this->id_statement, $c );
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
        $count = oci_num_fields( $this->id_statement );

        // if we used a limit we subtract it
        return ( $this->limit_used ) ? $count - 1 : $count;
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
        for( $c = 1, $num_fields = $this->num_fields(); $c <= $num_fields; $c++ )
        {
            $field = new \stdClass();
            $field->name = oci_field_name( $this->id_statement, $c );
            $field->type = oci_field_type( $this->id_statement, $c );
            $field->max_length = oci_field_size( $this->id_statement, $c );

            $data[ ] = $field;
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
            oci_free_statement( $this->id_result );
            $this->id_result = FALSE;
        }

        if( is_resource( $this->id_statement ) )
        {
            oci_free_statement( $this->id_statement );
        }

        if( is_resource( $this->id_cursor ) )
        {
            oci_cancel( $this->id_cursor );
            $this->id_cursor = NULL;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Result - associative array
     *
     * Returns the result set as an array
     *
     * @access  public
     * @return  array
     */
    protected function _fetch_assoc()
    {
        $id = ( $this->id_cursor ) ? $this->id_cursor : $this->id_statement;

        return oci_fetch_assoc( $id );
    }

    // --------------------------------------------------------------------

    /**
     * Result - object
     *
     * Returns the result set as an object
     *
     * @param   string  $class_name
     *
     * @access  protected
     * @return  object
     */
    protected function _fetch_object( $class_name = '\stdClass' )
    {
        $row = ( $this->id_cursor )
            ? oci_fetch_object( $this->id_cursor )
            : oci_fetch_object( $this->id_statement );

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