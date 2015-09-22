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

namespace O2System\O2DB\Drivers\Mysql;

// ------------------------------------------------------------------------

use O2System\O2DB\Interfaces\Result as ResultInterface;

/**
 * MySQL Database Result
 *
 * @author      Circle Creative Developer Team
 */
class Result extends ResultInterface
{

    /**
     * Class constructor
     *
     * @param   object &$driver_object
     *
     * @access  public
     */
    public function __construct( &$driver_object )
    {
        parent::__construct( $driver_object );

        // Required, due to mysql_data_seek() causing nightmares
        // with empty result sets
        $this->num_rows = mysql_num_rows( $this->id_result );
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
        return $this->num_rows;
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
        mysql_field_seek( $this->id_result, 0 );
        while( $field = mysql_fetch_field( $this->id_result ) )
        {
            $field_names[ ] = $field->name;
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
        for( $i = 0, $c = $this->num_fields(); $i < $c; $i++ )
        {
            $retval[ $i ] = new \stdClass();
            $retval[ $i ]->name = mysql_field_name( $this->id_result, $i );
            $retval[ $i ]->type = mysql_field_type( $this->id_result, $i );
            $retval[ $i ]->max_length = mysql_field_len( $this->id_result, $i );
            $retval[ $i ]->primary_key = (int)( strpos( mysql_field_flags( $this->id_result, $i ), 'primary_key' ) !== FALSE );
        }

        return $retval;
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
        return mysql_num_fields( $this->id_result );
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
            mysql_free_result( $this->id_result );
            $this->id_result = FALSE;
        }
    }

    // --------------------------------------------------------------------

    /**
     * Data Seek
     *
     * Moves the internal pointer to the desired offset. We call
     * this internally before fetching results to make sure the
     * result set starts at zero.
     *
     * @param   int $n
     *
     * @access  public
     * @return  bool
     */
    public function data_seek( $n = 0 )
    {
        return $this->num_rows
            ? mysql_data_seek( $this->id_result, $n )
            : FALSE;
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
        return mysql_fetch_assoc( $this->id_result );
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
        return mysql_fetch_object( $this->id_result, $class_name );
    }

}
