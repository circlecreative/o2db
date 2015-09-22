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

namespace O2System\O2DB\Drivers\Sqlsrv;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Result as ResultInterface;

/**
 * Microsoft SQL Server Database Result
 *
 * @author      Circle Creative Developer Team
 */
class Result extends ResultInterface
{
    /**
     * Scrollable flag
     *
     * @access  public
     * @type    mixed
     */
    public $scrollable;

    // --------------------------------------------------------------------

    /**
     * Class constructor
     *
     * @param   object $driver_object
     *
     * @access  public
     */
    public function __construct( &$driver_object )
    {
        parent::__construct( $driver_object );

        $this->scrollable = $driver_object->scrollable;
    }

    // --------------------------------------------------------------------

    /**
     * Number of rows in the result set
     *
     * @access  public
     * @return  int
     */
    public function num_rows()
    {
        // sqlsrv_num_rows() doesn't work with the FORWARD and DYNAMIC cursors (FALSE is the same as FORWARD)
        if( ! in_array( $this->scrollable, array( FALSE, SQLSRV_CURSOR_FORWARD, SQLSRV_CURSOR_DYNAMIC ), TRUE ) )
        {
            return parent::num_rows();
        }

        return is_int( $this->num_rows )
            ? $this->num_rows
            : $this->num_rows = sqlsrv_num_rows( $this->id_result );
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
        return @sqlsrv_num_fields( $this->id_result );
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
        foreach( sqlsrv_field_metadata( $this->id_result ) as $offset => $field )
        {
            $field_names[ ] = $field[ 'Name' ];
        }

        return $field_names;
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
        $retval = array();
        foreach( sqlsrv_field_metadata( $this->id_result ) as $i => $field )
        {
            $retval[ $i ] = new \stdClass();
            $retval[ $i ]->name = $field[ 'Name' ];
            $retval[ $i ]->type = $field[ 'Type' ];
            $retval[ $i ]->max_length = $field[ 'Size' ];
        }

        return $retval;
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
            sqlsrv_free_stmt( $this->id_result );
            $this->id_result = FALSE;
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
        return sqlsrv_fetch_array( $this->id_result, SQLSRV_FETCH_ASSOC );
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
        return sqlsrv_fetch_object( $this->id_result, $class_name );
    }

}